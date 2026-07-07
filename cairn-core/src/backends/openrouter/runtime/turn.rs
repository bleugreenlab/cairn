//! Session entry and the tool-iteration driver: spawn the owned turn loop, POST
//! each generation, execute tool calls, persist every event, and finalize the run
//! (success, cancel, suspend, budget, or fatal failure). Pushes a context-token
//! snapshot to the frontend gauge each generation.

use super::context::fit_conversation;
use super::conversation::{build_conversation_messages, render_tool_result};
use super::http::post_chat_completion;
use super::persist::{
    store_assistant_message, store_assistant_tool_call, store_success_result, store_tool_result,
    tool_call_usage,
};
use super::tools::{build_provider_object, execute_tool_call};
use super::wire::{ChatMessage, OpenRouterUsage};
use crate::agent_process::process::{RunHandle, SuspendKind};
use crate::backends::openrouter::{
    openrouter_api_key, repair, NoopOpenRouterStdin, OPENROUTER_BACKEND_KEY,
    OPENROUTER_BACKEND_NAME,
};
use crate::backends::run_state::{
    resolve_run_db, run_job_id, set_session_backend_id, transition_run_to_live,
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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

// Runaway guard, not a turn-length limit: a real coding turn can legitimately
// exceed dozens of read/write/run round-trips, so the cap is set high and the
// turn finalizes gracefully on reaching it rather than crashing the run.
const MAX_TOOL_ITERATIONS: usize = 200;

pub(crate) fn start_session(config: SessionConfig, orch: &Orchestrator) -> Result<(), String> {
    let session_id = config.session_start.session_id().to_string();
    let api_key = match openrouter_api_key(orch) {
        Some(key) => key,
        None => {
            insert_error_event(
                orch,
                &config.run_id,
                Some(&session_id),
                "OpenRouter API key not configured. Add an OpenRouter API key in Settings → Providers.",
            );
            return Err("Missing OpenRouter API key".to_string());
        }
    };

    let workspace_instructions = crate::workspace::instructions::read_workspace_instructions();
    let project_instructions = crate::workspace::instructions::read_project_instructions(
        std::path::Path::new(&config.working_dir),
    );
    let prompt_segments = assemble_prompt_segments(
        &crate::system_prompt::cairn_system_prompt(),
        workspace_instructions.as_deref(),
        project_instructions.as_deref(),
        config.system_prompt_content.as_deref(),
        config.system_prompt_dynamic_tail.as_deref(),
    );
    // Resolve the run's owning DB ONCE (CAIRN-2208) and thread it through the
    // run-state and streaming-transcript writes below. A team run lives wholly in
    // its synced replica; targeting the private DB would fail the
    // message_streams→runs foreign key. Fail-closed.
    let run_db = match resolve_run_db(OPENROUTER_BACKEND_NAME, orch, &config.run_id) {
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
            OPENROUTER_BACKEND_KEY,
            &prompt_segments,
        )
    } else {
        get_next_sequence(run_db.clone(), &config.run_id).unwrap_or(0)
    };
    // Flatten the same segments that were (or would be) persisted. Each OpenRouter
    // HTTP turn re-sends the whole message array with no server-side system-prompt
    // retention, so the full harness contract (Cairn prompt + workspace + agent +
    // orientation tail) must lead the system message on New and Resume alike.
    let system_prompt = flatten_prompt_segments(&prompt_segments);

    if let Err(error) =
        set_session_backend_id(OPENROUTER_BACKEND_NAME, &run_db, &session_id, &session_id)
    {
        log::warn!("Failed to set OpenRouter backend id: {error}");
    }
    if let Err(error) =
        transition_run_to_live(OPENROUTER_BACKEND_NAME, orch, &run_db, &config.run_id)
    {
        log::warn!("Failed to transition OpenRouter run to live: {error}");
    }

    let process_job_id = run_job_id(OPENROUTER_BACKEND_NAME, &run_db, &config.run_id);
    let cancel = Arc::new(AtomicBool::new(false));
    {
        let mut handle = RunHandle::new(
            Arc::new(Mutex::new(None)),
            Arc::new(Mutex::new(Some(Box::new(NoopOpenRouterStdin {
                cancel: cancel.clone(),
            })))),
            Some(session_id.clone()),
            process_job_id,
        );
        handle.backend = Some(OPENROUTER_BACKEND_KEY.to_string());
        handle.model = config.model.as_ref().map(|model| model.to_string());
        // OpenRouter owns its turn/tool loop in-process and has no warm process,
        // so foreground questions and inline delegated tasks suspend the turn
        // rather than inline-waiting. The blocking handlers key off this flag.
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
            )
        }));
        match outcome {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                log::error!("OpenRouter turn failed: {error}");
                insert_error_event(
                    &orch_clone,
                    &run_id,
                    Some(&error_session_id),
                    &format!("OpenRouter turn failed: {error}"),
                );
                finish_run_failed(&orch_clone, &run_id, "turn_failed");
            }
            Err(panic) => {
                let detail = panic_message(panic.as_ref());
                log::error!("OpenRouter turn panicked: {detail}");
                insert_error_event(
                    &orch_clone,
                    &run_id,
                    Some(&error_session_id),
                    &format!("OpenRouter turn panicked: {detail}"),
                );
                finish_run_failed(&orch_clone, &run_id, "turn_panicked");
            }
        }
    });

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run_http_turn(
    orch: &Orchestrator,
    run_db: Arc<LocalDb>,
    config: SessionConfig,
    api_key: String,
    session_id: String,
    mut sequence: i32,
    turn_id: Option<String>,
    cancel: Arc<AtomicBool>,
    system_prompt: String,
) -> Result<(), String> {
    let model = config
        .model
        .as_ref()
        .map(|model| model.to_string())
        .unwrap_or_else(|| "openrouter/auto".to_string());
    let mut conversation = build_conversation_messages(orch, &config, &session_id, &system_prompt)?;

    // The selected model's real context window, sourced from the discovered
    // OpenRouter catalog (not a hardcoded assumption). Used to trim the outgoing
    // request to fit; `None` (catalog miss / unknown window) disables trimming
    // and the read timeout remains the backstop against an over-limit hang.
    let context_window =
        orch.context_window_for_context_tokens(OPENROUTER_BACKEND_KEY, Some(&model));

    // Resolve provider-routing once per turn (it reads settings.yaml); the same
    // object rides every tool-iteration POST.
    let provider = build_provider_object(&orch.get_settings().openrouter_routing);

    let mut exact_cost = None;

    for _ in 0..MAX_TOOL_ITERATIONS {
        if cancel.load(Ordering::SeqCst) {
            return finalize_cancelled(orch, &config.run_id);
        }
        // Trim the OUTGOING request only (never the stored transcript or the
        // running `conversation`): collapse the oldest aged tool outputs so the
        // request fits under the model's real window. `outgoing` borrows
        // `conversation` when no trim is needed; it is dropped before the
        // post-response mutations below.
        let outgoing = fit_conversation(&conversation, context_window);
        let response = post_chat_completion(
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
            &provider,
            &cancel,
        )?;
        drop(outgoing);
        // A cancel observed mid-stream breaks the SSE reader and returns a
        // partial response; land the run idle here before treating it as a
        // result (an incomplete partial would otherwise look like success).
        if cancel.load(Ordering::SeqCst) {
            return finalize_cancelled(orch, &config.run_id);
        }
        let response_usage = response.usage.clone();
        let response_id = response.id.clone();
        let response_model = response.model.clone();
        // A generation that ends with finish_reason "length" was cut off at the
        // output-token cap, so its last tool call may be truncated even when the
        // JSON happens to parse. Used below to refuse partial side-effecting calls.
        let generation_truncated = response.finish_reason.as_deref() == Some("length");

        // Cost ships inline in every generation's usage. Accumulate across the
        // turn so a multi-tool turn captures cost on each POST, not only the
        // terminal generation (the old /generation lookup under-counted by
        // capturing just the final one).
        if let Some(generation_cost) = response_usage.as_ref().and_then(|usage| usage.cost) {
            exact_cost = Some(exact_cost.unwrap_or(0.0) + generation_cost);
        }

        // Push live context occupancy to the frontend gauge (mirrors Claude/Codex).
        // Without this the radial context dial never renders for OpenRouter: the
        // hook's one-shot query is never invalidated, so the real per-model window
        // sourced here only reaches the UI via this event.
        if let Some(usage) = response_usage.as_ref() {
            emit_openrouter_context_snapshot(
                orch,
                &config.run_id,
                &session_id,
                &model,
                context_window,
                usage,
            );
        }

        let Some(choice) = response.choices.into_iter().next() else {
            return Err("OpenRouter response did not include choices".to_string());
        };
        let mut assistant_message = choice.message;
        // Canonicalize tool names in place BEFORE the call is stored, pushed into
        // the conversation, and executed, so execution, the stored transcript,
        // and any replay/resume all reference the dispatched verb. Otherwise a
        // successful call replays under an invalid name like `Write_File` or
        // `mcp__cairn__run` and reinforces it in the model-facing history.
        if let Some(calls) = assistant_message.tool_calls.as_mut() {
            for call in calls.iter_mut() {
                if let Some(verb) = repair::normalize_tool_name(&call.function.name) {
                    if verb != call.function.name {
                        log::warn!(
                            "OpenRouter normalized tool name {:?} -> {:?}",
                            call.function.name,
                            verb
                        );
                        call.function.name = verb.to_string();
                    }
                }
            }
        }
        let assistant_text = assistant_message.content.clone().unwrap_or_default();
        let tool_calls = assistant_message.tool_calls.clone().unwrap_or_default();
        let reasoning_details = assistant_message.reasoning_details.clone();
        conversation.push(assistant_message);

        // Observability: surface output-token truncation per model so its
        // frequency is visible. The last tool call is the one that can be cut
        // off; name it when present.
        if generation_truncated {
            let truncated_tool = tool_calls
                .last()
                .map(|call| call.function.name.as_str())
                .unwrap_or("<none>");
            log::warn!(
                "OpenRouter generation truncated (finish_reason=length) model={} tool={}",
                model,
                truncated_tool
            );
        }

        if tool_calls.is_empty() {
            if !assistant_text.is_empty() {
                if response.streamed_text {
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
                        response_usage.as_ref(),
                        response_id.as_deref(),
                        response_model.as_deref(),
                        exact_cost,
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
                response_usage.as_ref(),
                response_id.as_deref(),
                response_model.as_deref(),
                exact_cost,
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
                        if let Err(e) = crate::backends::run_state::run_backend_db(
                            OPENROUTER_BACKEND_KEY,
                            async move {
                                crate::mcp::handlers::comments_artifacts::capture_call_structured_output(
                                    &orch_capture,
                                    &run_id_capture,
                                    value,
                                )
                                .await
                            },
                        ) {
                            log::warn!(
                                "Failed to capture OpenRouter structured output for run {}: {}",
                                config.run_id,
                                e
                            );
                        }
                    }
                    Err(e) => log::warn!(
                        "OpenRouter schema-constrained call produced non-JSON content for run {}: {}",
                        config.run_id,
                        e
                    ),
                }
            }
            finish_run(orch, &config.run_id, RunStatus::Exited);
            return Ok(());
        }

        let tool_call_text = if response.streamed_text {
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
            tool_call_usage(response.streamed_text, response_usage.as_ref()),
            response_id.as_deref(),
            response_model.as_deref(),
            reasoning_details.as_ref(),
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
            )?;
            sequence += 1;
            conversation.push(ChatMessage::tool(
                tool_call.id.clone(),
                render_tool_result(result),
            ));
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
                response_usage.as_ref(),
                response_id.as_deref(),
                response_model.as_deref(),
                exact_cost,
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
        &format!("Reached the per-turn tool-iteration budget ({MAX_TOOL_ITERATIONS}); finalizing this turn."),
        None,
        None,
        None,
        exact_cost,
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
fn emit_openrouter_context_snapshot(
    orch: &Orchestrator,
    run_id: &str,
    session_id: &str,
    model: &str,
    context_window: Option<i64>,
    usage: &OpenRouterUsage,
) {
    let counts = usage.token_counts();
    let input = counts.input.unwrap_or(0) as i64;
    let output = counts.output.unwrap_or(0) as i64;
    orch.store_context_token_snapshot(ContextTokenState {
        run_id: run_id.to_string(),
        session_id: Some(session_id.to_string()),
        backend: OPENROUTER_BACKEND_KEY.to_string(),
        model: Some(model.to_string()),
        // OpenAI-style usage: prompt_tokens already includes any cached input, so
        // full input plus output is the occupancy (adding cache_read double counts).
        used_tokens: input + output,
        context_window,
        auto_compact_limit: None,
        reasoning_tokens: counts.thinking.map(|thinking| thinking as i64),
        last_output_tokens: counts.output.map(|output| output as i64),
        captured_at: chrono::Utc::now().timestamp(),
    });
}
