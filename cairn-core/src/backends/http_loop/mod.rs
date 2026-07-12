//! Provider-agnostic in-process HTTP turn/tool loop.
//!
//! Cairn owns the turn/tool loop for HTTP chat-completions backends so models
//! receive Cairn's direct read/write/run tools instead of a CLI's host tools.
//! This module is the neutral driver: it spawns the owned turn loop, asks a
//! [`WireAdapter`] to build the conversation and POST each generation, executes
//! the returned tool calls, persists every transcript event, pushes a
//! context-token snapshot to the frontend gauge, and finalizes the run (success,
//! cancel, suspend, budget, or fatal failure).
//!
//! The adapter owns every wire concern — the request/response DTOs, the streaming
//! reader, transcript↔message mapping, context trimming, the api key, and the
//! backend identity — and converts its wire types to the neutral
//! [`Generation`]/[`TurnToolCall`]/[`TurnUsage`] at its boundary, so no wire DTO
//! ever reaches this loop. OpenRouter is the first and today the ONLY adapter
//! (`backends/openrouter/adapter.rs`); the seam is structural readiness, not a
//! second implementation.

mod persist;
pub(in crate::backends) mod repair;
mod tools;

#[cfg(test)]
mod tests;

pub(in crate::backends) use persist::AssistantStreamState;
pub(in crate::backends) use tools::render_tool_result;

use persist::{
    store_assistant_message, store_assistant_tool_call, store_success_result, store_tool_result,
    tool_call_usage,
};
use tools::execute_tool_call;

use crate::agent_process::process::{BackendStdin, RunHandle, SuspendKind};
use crate::agent_process::stream::TokenCounts;
use crate::backends::run_state::{
    resolve_run_db, run_backend_db, run_job_id, set_session_backend_id, transition_run_to_live,
};
use crate::backends::{SessionConfig, SessionStart};
use crate::dispatch::DispatchOutput;
use crate::models::{ContextTokenState, RunStatus};
use crate::orchestrator::session::{
    assemble_prompt_segments, flatten_prompt_segments, insert_error_event,
    persist_system_prompt_event,
};
use crate::orchestrator::Orchestrator;
use crate::storage::LocalDb;
use crate::transcripts::stream_store::get_next_sequence;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::any::Any;
use std::borrow::Cow;
use std::io::{Result as IoResult, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

// Runaway guard, not a turn-length limit: a real coding turn can legitimately
// exceed dozens of read/write/run round-trips, so the cap is set high and the
// turn finalizes gracefully on reaching it rather than crashing the run.
const MAX_TOOL_ITERATIONS: usize = 200;

/// A model-emitted tool call, neutralized from the adapter's wire type: the
/// dispatch id, the (already tool-name-normalized) verb, and the raw JSON
/// argument string the tool dispatch / repair path parses.
pub(in crate::backends) struct TurnToolCall {
    pub(in crate::backends) id: String,
    pub(in crate::backends) name: String,
    pub(in crate::backends) arguments: String,
}

/// OpenAI-style usage payload — the neutral HTTP-loop usage shape. Its field
/// names and serde attributes MUST stay byte-identical to what the stored
/// transcript's `raw.usage` has always embedded (the frontend's two-event token
/// rollup and the radial context dial read these keys), so this is deliberately
/// the OpenAI/OpenRouter usage shape rather than a reinvented one. Every field
/// is `#[serde(default)]` with NO `skip_serializing_if`, so serialization always
/// emits all eight keys (nulls included), exactly as before.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(in crate::backends) struct TurnUsage {
    #[serde(default)]
    pub(in crate::backends) prompt_tokens: Option<i32>,
    #[serde(default)]
    pub(in crate::backends) completion_tokens: Option<i32>,
    #[serde(default)]
    pub(in crate::backends) total_tokens: Option<i32>,
    #[serde(default)]
    pub(in crate::backends) reasoning_tokens: Option<i32>,
    #[serde(default)]
    pub(in crate::backends) prompt_tokens_details: Option<Value>,
    #[serde(default)]
    pub(in crate::backends) completion_tokens_details: Option<Value>,
    #[serde(default)]
    pub(in crate::backends) cost: Option<f64>,
    #[serde(default)]
    pub(in crate::backends) cost_details: Option<Value>,
}

impl TurnUsage {
    pub(in crate::backends) fn token_counts(&self) -> TokenCounts {
        let cache_read = self.prompt_tokens_details.as_ref().and_then(|v| {
            v.get("cached_tokens")
                .and_then(Value::as_i64)
                .map(|n| n as i32)
        });
        TokenCounts {
            input: self
                .prompt_tokens
                .map(|input| input.saturating_sub(cache_read.unwrap_or(0))),
            output: self.completion_tokens,
            cache_read,
            cache_create: None,
            thinking: self.reasoning_tokens.or_else(|| {
                self.completion_tokens_details.as_ref().and_then(|v| {
                    v.get("reasoning_tokens")
                        .and_then(Value::as_i64)
                        .map(|n| n as i32)
                })
            }),
        }
    }
}

/// One assistant generation, neutralized from the adapter's wire response. The
/// loop owns this and never sees a wire DTO: `assistant_message` (the adapter's
/// own message type) is pushed back onto the running conversation verbatim,
/// while the neutral fields drive execution, persistence, and the context
/// snapshot.
pub(in crate::backends) struct Generation<M> {
    pub(in crate::backends) assistant_message: M,
    pub(in crate::backends) assistant_text: String,
    pub(in crate::backends) tool_calls: Vec<TurnToolCall>,
    pub(in crate::backends) reasoning_details: Option<Value>,
    pub(in crate::backends) usage: Option<TurnUsage>,
    pub(in crate::backends) finish_reason: Option<String>,
    pub(in crate::backends) generation_id: Option<String>,
    pub(in crate::backends) response_model: Option<String>,
    pub(in crate::backends) streamed_text: bool,
}

/// The wire seam behind the neutral HTTP turn loop. An adapter owns a single
/// provider's chat-completions format: how to build the outgoing conversation,
/// look up and trim to the model's context window, POST one streaming
/// generation, and render a tool result back into a message. The loop is
/// monomorphized over the adapter (no `dyn`), and the adapter converts its wire
/// types to the neutral [`Generation`]/[`TurnToolCall`]/[`TurnUsage`] at this
/// boundary so no wire DTO ever reaches the loop.
pub(in crate::backends) trait WireAdapter {
    /// The adapter's own conversation-message type (OpenRouter: `ChatMessage`).
    type Message: Clone + Send;

    /// Stable backend key stored in transcript `raw` payloads and used to route
    /// the run to its owning DB (OpenRouter: `"openrouter"`).
    fn backend_key(&self) -> &'static str;

    /// Human-readable backend name for logs and transcript error text.
    fn backend_name(&self) -> &'static str;

    /// Fallback model id when the session carries none.
    fn default_model(&self) -> &'static str;

    /// The configured API key, or `None` when the provider is unauthenticated.
    fn api_key(&self, orch: &Orchestrator) -> Option<String>;

    /// Build the outgoing message array (system + prior transcript + new user).
    fn build_conversation(
        &self,
        orch: &Orchestrator,
        config: &SessionConfig,
        session_id: &str,
        system_prompt: &str,
    ) -> Result<Vec<Self::Message>, String>;

    /// The model's real context window, or `None` to disable outgoing trimming.
    fn context_window(&self, orch: &Orchestrator, model: &str) -> Option<i64>;

    /// Trim the OUTGOING request to fit the window; borrows when it already fits.
    fn fit_conversation<'a>(
        &self,
        messages: &'a [Self::Message],
        context_window: Option<i64>,
    ) -> Cow<'a, [Self::Message]>;

    /// POST one streaming generation and return it neutralized. Normalizing tool
    /// names on the returned `assistant_message` (so the stored/replayed history
    /// references the dispatched verb) is the adapter's responsibility.
    #[allow(clippy::too_many_arguments)]
    fn post_generation(
        &self,
        orch: &Orchestrator,
        run_db: &Arc<LocalDb>,
        api_key: &str,
        model: &str,
        session_id: &str,
        outgoing: &[Self::Message],
        config: &SessionConfig,
        run_id: &str,
        turn_id: Option<&str>,
        sequence: i32,
        cancel: &Arc<AtomicBool>,
    ) -> Result<Generation<Self::Message>, String>;

    /// Render a dispatch result into a tool-result message for the conversation.
    fn render_tool_result_message(
        &self,
        tool_call_id: &str,
        output: DispatchOutput,
    ) -> Self::Message;
}

/// The stdin registered for an owned-loop run. It writes nowhere; its only job
/// is to carry the cancel flag the streaming turn polls at SSE line boundaries.
/// A backend's `send_interrupt` downcasts to this and flips `cancel`, which
/// breaks the SSE reader and drops the connection (stopping billing).
pub(in crate::backends) struct HttpTurnStdin {
    pub(in crate::backends) cancel: Arc<AtomicBool>,
}

impl Write for HttpTurnStdin {
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        Ok(buf.len())
    }

    fn flush(&mut self) -> IoResult<()> {
        Ok(())
    }
}

impl BackendStdin for HttpTurnStdin {
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
    A::Message: Send + 'static,
{
    let session_id = config.session_start.session_id().to_string();
    let backend_key = adapter.backend_key();
    let backend_name = adapter.backend_name();
    let api_key = match adapter.api_key(orch) {
        Some(key) => key,
        None => {
            insert_error_event(
                orch,
                &config.run_id,
                Some(&session_id),
                &format!(
                    "{backend_name} API key not configured. Add an {backend_name} API key in Settings → Providers."
                ),
            );
            return Err(format!("Missing {backend_name} API key"));
        }
    };

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
    let run_db = match resolve_run_db(backend_name, orch, &config.run_id) {
        Ok(db) => db,
        Err(error) => {
            insert_error_event(orch, &config.run_id, Some(&session_id), &error);
            return Err(error);
        }
    };

    let initial_sequence = if matches!(
        config.session_start,
        SessionStart::New { .. } | SessionStart::Fork { .. }
    ) {
        persist_system_prompt_event(
            orch,
            &config.run_id,
            Some(&session_id),
            backend_key,
            &prompt_segments,
        )
    } else {
        get_next_sequence(run_db.clone(), &config.run_id).unwrap_or(0)
    };
    // Flatten the same segments that were (or would be) persisted. Each HTTP turn
    // re-sends the whole message array with no server-side system-prompt
    // retention, so the full harness contract (Cairn prompt + workspace + agent +
    // orientation tail) must lead the system message on New and Resume alike.
    let system_prompt = flatten_prompt_segments(&prompt_segments);

    if let Err(error) = set_session_backend_id(backend_name, &run_db, &session_id, &session_id) {
        log::warn!("Failed to set {backend_name} backend id: {error}");
    }
    if let Err(error) = transition_run_to_live(backend_name, orch, &run_db, &config.run_id) {
        log::warn!("Failed to transition {backend_name} run to live: {error}");
    }

    let process_job_id = run_job_id(backend_name, &run_db, &config.run_id);
    let cancel = Arc::new(AtomicBool::new(false));
    {
        let mut handle = RunHandle::new(
            Arc::new(Mutex::new(None)),
            Arc::new(Mutex::new(Some(Box::new(HttpTurnStdin {
                cancel: cancel.clone(),
            })))),
            Some(session_id.clone()),
            process_job_id,
        );
        handle.backend = Some(backend_key.to_string());
        handle.model = config.model.as_ref().map(|model| model.to_string());
        // An HTTP-loop backend owns its turn/tool loop in-process and has no warm
        // process, so foreground questions and inline delegated tasks suspend the
        // turn rather than inline-waiting. The blocking handlers key off this flag.
        handle.owns_turn_loop = true;
        if let Ok(mut processes) = orch.process_state.processes.lock() {
            processes.register(config.run_id.clone(), handle);
        }
    }

    let orch_clone = orch.clone();
    let run_id = config.run_id.clone();
    let error_session_id = session_id.clone();
    std::thread::spawn(move || {
        // Catch a panic in the owned loop. Without this, a panic silently kills
        // the thread: `finish_run` is never called, the `run_completions`
        // broadcast never fires, and a delegated child's packet stays `running`
        // forever. Both the `Err` and panic arms route through
        // `finish_run_failed`, which fails a delegated task terminally so its
        // suspended parent resumes instead of hanging.
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            run_http_turn(
                &orch_clone,
                run_db,
                config,
                api_key,
                session_id,
                initial_sequence,
                None,
                cancel,
                system_prompt,
                adapter,
            )
        }));
        match outcome {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                log::error!("{backend_name} turn failed: {error}");
                insert_error_event(
                    &orch_clone,
                    &run_id,
                    Some(&error_session_id),
                    &format!("{backend_name} turn failed: {error}"),
                );
                finish_run_failed(&orch_clone, &run_id, "turn_failed");
            }
            Err(panic) => {
                let detail = panic_message(panic.as_ref());
                log::error!("{backend_name} turn panicked: {detail}");
                insert_error_event(
                    &orch_clone,
                    &run_id,
                    Some(&error_session_id),
                    &format!("{backend_name} turn panicked: {detail}"),
                );
                finish_run_failed(&orch_clone, &run_id, "turn_panicked");
            }
        }
    });

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run_http_turn<A: WireAdapter>(
    orch: &Orchestrator,
    run_db: Arc<LocalDb>,
    config: SessionConfig,
    api_key: String,
    session_id: String,
    mut sequence: i32,
    turn_id: Option<String>,
    cancel: Arc<AtomicBool>,
    system_prompt: String,
    adapter: A,
) -> Result<(), String> {
    let backend_key = adapter.backend_key();
    let backend_name = adapter.backend_name();
    let model = config
        .model
        .as_ref()
        .map(|model| model.to_string())
        .unwrap_or_else(|| adapter.default_model().to_string());
    let mut conversation =
        adapter.build_conversation(orch, &config, &session_id, &system_prompt)?;

    // The selected model's real context window, sourced from the adapter's
    // discovered catalog (not a hardcoded assumption). Used to trim the outgoing
    // request to fit; `None` (catalog miss / unknown window) disables trimming
    // and the read timeout remains the backstop against an over-limit hang.
    let context_window = adapter.context_window(orch, &model);

    let mut exact_cost = None;

    for _ in 0..MAX_TOOL_ITERATIONS {
        if cancel.load(Ordering::SeqCst) {
            return finalize_cancelled(orch, &config.run_id);
        }
        // Trim the OUTGOING request only (never the stored transcript or the
        // running `conversation`): the adapter collapses the oldest aged tool
        // outputs so the request fits under the model's real window. `outgoing`
        // borrows `conversation` when no trim is needed; it is dropped before the
        // post-response mutations below.
        let outgoing = adapter.fit_conversation(&conversation, context_window);
        let generation = adapter.post_generation(
            orch,
            &run_db,
            &api_key,
            &model,
            &session_id,
            &outgoing,
            &config,
            &config.run_id,
            turn_id.as_deref(),
            sequence,
            &cancel,
        )?;
        drop(outgoing);
        // A cancel observed mid-stream breaks the SSE reader and returns a
        // partial response; land the run idle here before treating it as a
        // result (an incomplete partial would otherwise look like success).
        if cancel.load(Ordering::SeqCst) {
            return finalize_cancelled(orch, &config.run_id);
        }

        let Generation {
            assistant_message,
            assistant_text,
            tool_calls,
            reasoning_details,
            usage,
            finish_reason,
            generation_id,
            response_model,
            streamed_text,
        } = generation;

        // A generation that ends with finish_reason "length" was cut off at the
        // output-token cap, so its last tool call may be truncated even when the
        // JSON happens to parse. Used below to refuse partial side-effecting calls.
        let generation_truncated = finish_reason.as_deref() == Some("length");

        // Cost ships inline in every generation's usage. Accumulate across the
        // turn so a multi-tool turn captures cost on each POST, not only the
        // terminal generation.
        if let Some(generation_cost) = usage.as_ref().and_then(|usage| usage.cost) {
            exact_cost = Some(exact_cost.unwrap_or(0.0) + generation_cost);
        }

        // Push live context occupancy to the frontend gauge (mirrors Claude/Codex).
        // Without this the radial context dial never renders: the hook's one-shot
        // query is never invalidated, so the real per-model window sourced here
        // only reaches the UI via this event.
        if let Some(usage) = usage.as_ref() {
            emit_context_snapshot(
                orch,
                &config.run_id,
                &session_id,
                backend_key,
                &model,
                context_window,
                usage,
            );
        }

        conversation.push(assistant_message);

        // Observability: surface output-token truncation per model so its
        // frequency is visible. The last tool call is the one that can be cut
        // off; name it when present.
        if generation_truncated {
            let truncated_tool = tool_calls
                .last()
                .map(|call| call.name.as_str())
                .unwrap_or("<none>");
            log::warn!(
                "{backend_name} generation truncated (finish_reason=length) model={} tool={}",
                model,
                truncated_tool
            );
        }

        if tool_calls.is_empty() {
            if !assistant_text.is_empty() {
                if streamed_text {
                    sequence += 1;
                } else {
                    store_assistant_message(
                        orch,
                        &run_db,
                        &config.run_id,
                        &session_id,
                        turn_id.as_deref(),
                        sequence,
                        &assistant_text,
                        usage.as_ref(),
                        generation_id.as_deref(),
                        response_model.as_deref(),
                        exact_cost,
                        backend_key,
                    )?;
                    sequence += 1;
                }
            }
            store_success_result(
                orch,
                &run_db,
                &config.run_id,
                &session_id,
                turn_id.as_deref(),
                sequence,
                usage.as_ref(),
                generation_id.as_deref(),
                response_model.as_deref(),
                exact_cost,
                backend_key,
            )?;
            // Schema-constrained call: capture the model's final structured
            // content as the return artifact server-side (CAIRN-2505), validated
            // against the contract, BEFORE finishing the run. A parse/validation
            // failure logs and leaves the call artifactless (it resolves to null)
            // rather than storing non-conforming data.
            if config.output_schema.is_some() && !assistant_text.is_empty() {
                match serde_json::from_str::<serde_json::Value>(&assistant_text) {
                    Ok(value) => {
                        let orch_capture = orch.clone();
                        let run_id_capture = config.run_id.clone();
                        if let Err(e) = run_backend_db(backend_key, async move {
                            crate::mcp::handlers::comments_artifacts::capture_call_structured_output(
                                &orch_capture,
                                &run_id_capture,
                                value,
                            )
                            .await
                        }) {
                            log::warn!(
                                "Failed to capture {backend_name} structured output for run {}: {}",
                                config.run_id,
                                e
                            );
                        }
                    }
                    Err(e) => log::warn!(
                        "{backend_name} schema-constrained call produced non-JSON content for run {}: {}",
                        config.run_id,
                        e
                    ),
                }
            }
            finish_run(orch, &config.run_id, RunStatus::Exited);
            return Ok(());
        }

        let tool_call_text = if streamed_text {
            sequence += 1;
            ""
        } else {
            assistant_text.as_str()
        };
        store_assistant_tool_call(
            orch,
            &run_db,
            &config.run_id,
            &session_id,
            turn_id.as_deref(),
            sequence,
            tool_call_text,
            &tool_calls,
            tool_call_usage(streamed_text, usage.as_ref()),
            generation_id.as_deref(),
            response_model.as_deref(),
            reasoning_details.as_ref(),
            backend_key,
        )?;
        sequence += 1;

        let mut suspend: Option<(usize, SuspendKind)> = None;
        for (index, tool_call) in tool_calls.iter().enumerate() {
            // Only the last tool call in a length-capped generation can be the
            // cut-off one; earlier calls closed before the next began.
            let suspected_truncated = generation_truncated && index + 1 == tool_calls.len();
            let result = execute_tool_call(orch, &config, tool_call, suspected_truncated)?;
            // An owned-loop blocking handler (foreground question / inline task)
            // requests a suspend synchronously inside the dispatch above. Read it
            // back before storing this call's tool_result: the real result is
            // supplied on resume keyed to this same tool_call id, so storing the
            // suspend marker here would double up with the synthetic result.
            if let Some(kind) = orch.process_state.take_suspend(&config.run_id) {
                suspend = Some((index, kind));
                break;
            }
            store_tool_result(
                orch,
                &run_db,
                &config.run_id,
                &session_id,
                turn_id.as_deref(),
                sequence,
                &tool_call.id,
                &result,
                backend_key,
            )?;
            sequence += 1;
            conversation.push(adapter.render_tool_result_message(&tool_call.id, result));
        }

        // A suspend takes precedence over the cancel path: it is a resumable
        // wait, not a user stop. Pair every still-unexecuted tool_call in this
        // assistant message with a placeholder result so the resumed
        // OpenAI-style conversation stays valid (each tool_call needs a matching
        // tool_result), then finalize the run into a resumable waiting state.
        if let Some((suspended_at, kind)) = suspend {
            let placeholder = DispatchOutput {
                content: "Skipped: turn suspended pending user response / task result.".to_string(),
                reminders: Vec::new(),
            };
            for tool_call in tool_calls.iter().skip(suspended_at + 1) {
                store_tool_result(
                    orch,
                    &run_db,
                    &config.run_id,
                    &session_id,
                    turn_id.as_deref(),
                    sequence,
                    &tool_call.id,
                    &placeholder,
                    backend_key,
                )?;
                sequence += 1;
            }
            return finalize_suspended(orch, &config.run_id, kind);
        }

        // The agent wrote its output artifact this iteration (`create-pr`, a
        // delegated task's `return`, or any node output schema), which arms the
        // terminal-tool boundary. Like the warm-process backends' boundary
        // interrupt, end the turn here instead of POSTing again: a top-level node
        // hands off for review, and a delegated child finalizes so its parent's
        // resume gate fires (without this the child loops forever and never
        // returns). The tool results for this assistant message are all stored,
        // so this is a safe boundary.
        if orch.process_state.terminal_tool_armed(&config.run_id) {
            store_success_result(
                orch,
                &run_db,
                &config.run_id,
                &session_id,
                turn_id.as_deref(),
                sequence,
                usage.as_ref(),
                generation_id.as_deref(),
                response_model.as_deref(),
                exact_cost,
                backend_key,
            )?;
            finish_run(orch, &config.run_id, RunStatus::Exited);
            return Ok(());
        }

        if cancel.load(Ordering::SeqCst) {
            return finalize_cancelled(orch, &config.run_id);
        }
    }

    // Reached the per-turn tool-iteration budget. Finalize gracefully instead of
    // crashing: note it to the transcript, record success, and exit idle so the
    // run stays resumable.
    store_assistant_message(
        orch,
        &run_db,
        &config.run_id,
        &session_id,
        turn_id.as_deref(),
        sequence,
        &format!(
            "Reached the per-turn tool-iteration budget ({MAX_TOOL_ITERATIONS}); finalizing this turn."
        ),
        None,
        None,
        None,
        exact_cost,
        backend_key,
    )?;
    sequence += 1;
    store_success_result(
        orch,
        &run_db,
        &config.run_id,
        &session_id,
        turn_id.as_deref(),
        sequence,
        None,
        None,
        None,
        exact_cost,
        backend_key,
    )?;
    finish_run(orch, &config.run_id, RunStatus::Exited);
    Ok(())
}

fn finalize_cancelled(orch: &Orchestrator, run_id: &str) -> Result<(), String> {
    // Non-warm backend: a user stop lands the run idle (clean Exited), not
    // crashed. A follow-up message starts a fresh resumed turn.
    let _ = crate::orchestrator::lifecycle::set_exit_reason(orch, run_id, "user_stop");
    finish_run(orch, run_id, RunStatus::Exited);
    Ok(())
}

/// Finalize an owned-loop turn that suspended on a foreground question or inline
/// delegated task. Unlike a user stop, this is a resumable wait: the run exits
/// idle with a suspend-specific exit reason (never `user_stop`), and the open
/// prompt or pending delegated successor keeps the issue waiting until the
/// answer/child result drives a fresh resumed turn via `continue_job_impl`.
fn finalize_suspended(orch: &Orchestrator, run_id: &str, kind: SuspendKind) -> Result<(), String> {
    let reason = match kind {
        SuspendKind::Prompt => "prompt_wait_suspended",
        SuspendKind::DelegatedTask => "delegated_wait_suspended",
    };
    let _ = crate::orchestrator::lifecycle::set_exit_reason(orch, run_id, reason);
    finish_run(orch, run_id, RunStatus::Exited);
    Ok(())
}

fn finish_run(orch: &Orchestrator, run_id: &str, status: RunStatus) {
    crate::orchestrator::lifecycle::finalize_run(orch, run_id, status);
    let _ = orch.process_state.stop_and_remove(run_id);
}

/// Finalize an owned-loop turn that hit a genuinely fatal failure (an `Err`
/// return or a caught panic). Mirrors `finish_run` but routes through the
/// task-aware `fail_run`: a delegated task is finalized terminally `Failed` (so
/// its suspended parent resumes with the error), while a top-level job stays a
/// resumable interruption.
fn finish_run_failed(orch: &Orchestrator, run_id: &str, reason: &str) {
    crate::orchestrator::lifecycle::fail_run(orch, run_id, reason);
    let _ = orch.process_state.stop_and_remove(run_id);
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
