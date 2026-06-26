use super::repair;
use super::{
    openrouter_api_key, NoopOpenRouterStdin, OPENROUTER_BACKEND_KEY, OPENROUTER_BACKEND_NAME,
};
use crate::agent_process::process::{RunHandle, SuspendKind};
use crate::agent_process::stream::{TokenCounts, ToolUseInfo, TranscriptEvent};
use crate::backends::run_state::{run_job_id, set_session_backend_id, transition_run_to_live};
use crate::backends::{SessionConfig, SessionStart};
use crate::dispatch::DispatchOutput;
use crate::models::{ContextTokenState, OpenRouterRouting, OpenRouterSort, RunStatus};
use crate::orchestrator::session::{
    assemble_prompt_segments, flatten_prompt_segments, insert_error_event,
    persist_system_prompt_event,
};
use crate::orchestrator::Orchestrator;
use crate::storage::{run_db_blocking, RowExt};
use crate::transcripts::stream_store::{
    append_chunks, finalize_stream_emit, get_next_sequence, insert_event_emit, open_stream,
    process_post_commit_outbox, EmitDelta, EventInsert, StreamAccumulator, StreamChunkInput,
};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::borrow::Cow;
use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use uuid::Uuid;

const CHAT_URL: &str = "https://openrouter.ai/api/v1/chat/completions";
// Runaway guard, not a turn-length limit: a real coding turn can legitimately
// exceed dozens of read/write/run round-trips, so the cap is set high and the
// turn finalizes gracefully on reaching it rather than crashing the run.
const MAX_TOOL_ITERATIONS: usize = 200;
// Bound on a single streaming generation request. Each tool-loop iteration is
// one POST for one assistant response (the loop re-POSTs per tool round-trip),
// so this caps one generation, not a whole multi-iteration turn. A stalled
// upstream — including an over-limit request the provider hangs on — hits this
// and surfaces a clear error instead of hanging forever (the old `timeout(None)`
// waited indefinitely). Sized generously so a high-effort reasoning generation
// is never mistaken for a hang.
const STREAM_REQUEST_TIMEOUT: Duration = Duration::from_secs(300);

pub(super) fn start_session(config: SessionConfig, orch: &Orchestrator) -> Result<(), String> {
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
        get_next_sequence(orch.db.local.clone(), &config.run_id).unwrap_or(0)
    };
    // Flatten the same segments that were (or would be) persisted. Each OpenRouter
    // HTTP turn re-sends the whole message array with no server-side system-prompt
    // retention, so the full harness contract (Cairn prompt + workspace + agent +
    // orientation tail) must lead the system message on New and Resume alike.
    let system_prompt = flatten_prompt_segments(&prompt_segments);

    if let Err(error) =
        set_session_backend_id(OPENROUTER_BACKEND_NAME, orch, &session_id, &session_id)
    {
        log::warn!("Failed to set OpenRouter backend id: {error}");
    }
    if let Err(error) = transition_run_to_live(OPENROUTER_BACKEND_NAME, orch, &config.run_id) {
        log::warn!("Failed to transition OpenRouter run to live: {error}");
    }

    let process_job_id = run_job_id(OPENROUTER_BACKEND_NAME, orch, &config.run_id);
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

        let tool_call_text = if response.streamed_text {
            sequence += 1;
            ""
        } else {
            assistant_text.as_str()
        };
        store_assistant_tool_call(
            orch,
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

/// Concatenate assembled prompt segments into the full system prompt. This is
/// byte-identical to what `persist_system_prompt_event` records, so the wire
/// system message equals the persisted/displayed prompt with no drift.
fn build_conversation_messages(
    orch: &Orchestrator,
    config: &SessionConfig,
    session_id: &str,
    system_prompt: &str,
) -> Result<Vec<ChatMessage>, String> {
    let mut messages = vec![ChatMessage::system(system_prompt.to_string())];
    if !matches!(config.session_start, SessionStart::New { .. }) {
        messages.extend(load_prior_chat_messages(orch, session_id, &config.run_id)?);
    }
    messages.push(ChatMessage::user(config.prompt.clone()));
    Ok(messages)
}

fn load_prior_chat_messages(
    orch: &Orchestrator,
    session_id: &str,
    current_run_id: &str,
) -> Result<Vec<ChatMessage>, String> {
    let session_id = session_id.to_string();
    let current_run_id = current_run_id.to_string();
    let messages = run_db_blocking(|| async move {
        orch.db
            .local
            .read(|conn| {
                let session_id = session_id.clone();
                let current_run_id = current_run_id.clone();
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT event_type, data FROM events
                             WHERE session_id = ?1
                               AND run_id != ?2
                               AND event_type IN ('user', 'assistant', 'tool_result')
                             ORDER BY created_at ASC, rowid ASC
                             LIMIT 200",
                            (session_id.as_str(), current_run_id.as_str()),
                        )
                        .await?;
                    let mut out = Vec::new();
                    while let Some(row) = rows.next().await? {
                        let event_type = row.text(0)?;
                        let data = row.text(1)?;
                        let Ok(event) = serde_json::from_str::<TranscriptEvent>(&data) else {
                            continue;
                        };
                        if let Some(message) = transcript_event_to_chat_message(&event_type, event)
                        {
                            out.push(message);
                        }
                    }
                    Ok(out)
                })
            })
            .await
            .map_err(|error| error.to_string())
    })?;
    Ok(reorder_tool_results_by_call_order(messages))
}

/// Reorder each assistant message's following `tool` results to match the order
/// of that assistant message's `tool_calls`, regardless of the order the events
/// were persisted in. The OpenRouter rebuild reads transcript events by
/// `(created_at, rowid)`, but a suspended turn persists the un-executed calls'
/// placeholder results at suspend time and the suspended call's real result only
/// later on resume (a higher sequence). Without this, a suspended non-last tool
/// call replays its later sibling's result before its own, which is not the
/// OpenAI/OpenRouter tool-result ordering for the assistant message. The sort is
/// stable and keyed by `tool_call_id` position, so an already-ordered run is a
/// no-op and any result whose id is not in the call list is left at the end.
fn reorder_tool_results_by_call_order(messages: Vec<ChatMessage>) -> Vec<ChatMessage> {
    let mut result: Vec<ChatMessage> = Vec::with_capacity(messages.len());
    let mut iter = messages.into_iter().peekable();
    while let Some(message) = iter.next() {
        let call_order: Option<Vec<String>> = if message.role == "assistant" {
            message
                .tool_calls
                .as_ref()
                .map(|calls| calls.iter().map(|call| call.id.clone()).collect())
        } else {
            None
        };
        result.push(message);
        let Some(order) = call_order else {
            continue;
        };
        // Gather the contiguous run of tool results that answer this assistant
        // message, then order them by their position in its `tool_calls`.
        let mut tools: Vec<ChatMessage> = Vec::new();
        while iter.peek().map(|m| m.role == "tool").unwrap_or(false) {
            tools.push(iter.next().expect("peeked tool message"));
        }
        tools.sort_by_key(|tool| {
            tool.tool_call_id
                .as_ref()
                .and_then(|id| order.iter().position(|call_id| call_id == id))
                .unwrap_or(usize::MAX)
        });
        result.extend(tools);
    }
    result
}

fn transcript_event_to_chat_message(
    event_type: &str,
    event: TranscriptEvent,
) -> Option<ChatMessage> {
    match event_type {
        "user" => event.content.map(ChatMessage::user),
        "assistant" => {
            let tool_calls = event.tool_uses.as_ref().map(|uses| {
                uses.iter()
                    .map(|tool| ToolCall {
                        id: tool.id.clone(),
                        r#type: default_function_type(),
                        function: ToolFunction {
                            name: tool.name.clone(),
                            arguments: serde_json::to_string(&tool.input)
                                .unwrap_or_else(|_| "{}".to_string()),
                        },
                    })
                    .collect::<Vec<_>>()
            });
            // Replay structured reasoning verbatim and in original order; stored
            // under either casing depending on which writer persisted the event.
            let reasoning_details = event
                .raw
                .as_ref()
                .and_then(|raw| {
                    raw.get("reasoning_details")
                        .or_else(|| raw.get("reasoningDetails"))
                })
                // Writers store `null` (no reasoning) or `[]`; treat both as absent
                // so a non-reasoning tool-call turn does not replay `reasoning_details: null`.
                .filter(|value| {
                    !value.is_null() && !matches!(value, Value::Array(items) if items.is_empty())
                })
                .cloned();
            if event.content.is_none() && tool_calls.as_ref().map(Vec::is_empty).unwrap_or(true) {
                None
            } else {
                Some(ChatMessage {
                    role: "assistant".to_string(),
                    content: event.content,
                    tool_call_id: None,
                    tool_calls: tool_calls.filter(|calls| !calls.is_empty()),
                    reasoning_details,
                })
            }
        }
        "tool_result" => event
            .tool_use_id
            .zip(event.tool_result)
            .map(|(tool_call_id, content)| ChatMessage::tool(tool_call_id, content)),
        _ => None,
    }
}

fn render_tool_result(output: DispatchOutput) -> String {
    if output.reminders.is_empty() {
        return output.content;
    }
    let mut rendered = output.content;
    for reminder in output.reminders {
        rendered.push_str("\n\n<system-reminder>\n");
        rendered.push_str(&reminder);
        rendered.push_str("\n</system-reminder>");
    }
    rendered
}

/// The outcome of classifying a model-emitted tool call: either a verb and
/// payload ready to dispatch, or a tool-result error to hand back to the model.
enum PreparedCall {
    Dispatch {
        verb: &'static str,
        payload: Value,
        repaired_args: bool,
    },
    Reject(DispatchOutput),
}

/// Retry message for a tool call whose arguments were cut off before the JSON
/// closed. Used both when the model's generation ended with `finish_reason:
/// length` and when balancing had to synthesize closers (the truncation case).
fn truncated_args_message(verb: &str) -> DispatchOutput {
    DispatchOutput {
        content: format!(
            "OpenRouter tool arguments for {verb} appear truncated (the model's output was cut off \
             before the JSON closed), so the call was NOT run to avoid applying a partial {verb}. \
             Re-emit the call with complete, well-formed JSON. If the payload is large (for example \
             a big write), split it into several smaller calls so each fits in the output budget."
        ),
        reminders: Vec::new(),
    }
}

/// Resolve a tool call's name and arguments through the deterministic repair
/// pipeline. Pure (no orchestrator/HTTP state) so the non-fatal behavior is
/// unit-testable: an unknown name or unrepairable arguments yield a `Reject`
/// carrying a retry message instead of an `Err` that would crash the run.
///
/// `suspected_truncated` is true when the generation ended with `finish_reason:
/// length` and this is the last tool call (the only one that can be cut off). A
/// side-effecting verb (`write`/`run`) is rejected whenever truncation is
/// suspected — by the finish signal or by balancing having to close the JSON —
/// so a partial mutation never reaches disk or the shell. `read` is
/// non-destructive, so a recovered partial is allowed through.
fn prepare_tool_call(tool_call: &ToolCall, suspected_truncated: bool) -> PreparedCall {
    let raw_name = tool_call.function.name.as_str();
    let Some(verb) = repair::normalize_tool_name(raw_name) else {
        return PreparedCall::Reject(DispatchOutput {
            content: format!(
                "Unsupported OpenRouter tool: {raw_name:?}. Valid tools are: read, write, run."
            ),
            reminders: Vec::new(),
        });
    };
    let side_effecting = matches!(verb, "write" | "run");

    let (payload, repaired_args) = match repair::parse_tool_arguments(&tool_call.function.arguments)
    {
        repair::ParsedArguments::Ready { value, repaired } => {
            // Even cleanly-parsed JSON can be a cut-off payload when the
            // generation hit the output-token cap right after a complete
            // element. Don't risk a partial side effect on that signal.
            if side_effecting && suspected_truncated {
                return PreparedCall::Reject(truncated_args_message(verb));
            }
            (value, repaired)
        }
        repair::ParsedArguments::Truncated { value } => {
            // Balancing synthesized the closers: the model never finished this
            // payload. Refuse to dispatch a side-effecting partial.
            if side_effecting {
                return PreparedCall::Reject(truncated_args_message(verb));
            }
            (value, true)
        }
        repair::ParsedArguments::Unrecoverable => {
            // A severely cut-off large call (the generation hit the output-token
            // cap) is the common cause of unrepairable JSON. Give the
            // truncation-specific guidance so the model splits it up rather than
            // the generic "not valid JSON" message.
            if suspected_truncated {
                return PreparedCall::Reject(truncated_args_message(verb));
            }
            return PreparedCall::Reject(DispatchOutput {
                content: format!(
                    "OpenRouter tool arguments for {verb} were not valid JSON, even after repair. \
                     Re-emit the call with a single well-formed JSON object as the arguments. \
                     If the payload is very large (for example a big write), split it into smaller calls."
                ),
                reminders: Vec::new(),
            });
        }
    };

    PreparedCall::Dispatch {
        verb,
        payload,
        repaired_args,
    }
}

fn execute_tool_call(
    orch: &Orchestrator,
    config: &SessionConfig,
    tool_call: &ToolCall,
    suspected_truncated: bool,
) -> Result<DispatchOutput, String> {
    let (tool_name, mut payload) = match prepare_tool_call(tool_call, suspected_truncated) {
        PreparedCall::Dispatch {
            verb,
            payload,
            repaired_args,
        } => {
            if repaired_args {
                log::warn!(
                    "OpenRouter repaired malformed JSON arguments for tool {verb:?} ({} bytes)",
                    tool_call.function.arguments.len()
                );
            }
            (verb, payload)
        }
        // Never fatal: a bad name, unrepairable arguments, or a truncated
        // side-effecting call become a tool-result error the model retries
        // against, the same graceful channel structural validation errors
        // already use. A malformed tool call must not reach RunStatus::Crashed.
        PreparedCall::Reject(output) => return Ok(output),
    };
    normalize_tool_payload(tool_name, &mut payload, &config.home_uri);
    let request = crate::mcp::types::McpCallbackRequest {
        cwd: config.working_dir.clone(),
        run_id: Some(config.run_id.clone()),
        tool: if tool_name == "read" && payload.get("paths").is_some() {
            "read_batch".to_string()
        } else {
            tool_name.to_string()
        },
        payload,
        tool_use_id: Some(tool_call.id.clone()),
    };
    // read_batch and run return a JSON batch envelope (composed text + image
    // content blocks). The OpenAI tool-message format is text-only, so collapse
    // the envelope to its text on this edge; image blocks cannot ride a
    // chat-completions tool result.
    let collapse_envelope = request.tool == "read_batch" || request.tool == "run";
    let read_cursors = Mutex::new(HashMap::new());
    let mut output = run_db_blocking(|| async move {
        Ok(crate::dispatch::dispatch_tool(orch, &request, &read_cursors).await)
    })?;
    if collapse_envelope {
        output.content = collapse_envelope_text(output.content);
    }
    Ok(output)
}

/// Collapse a read/run batch envelope to its composed text for the text-only
/// OpenAI tool-message format. `read_batch` and `run` return a JSON envelope
/// carrying text plus image content blocks; a chat-completions tool result
/// cannot carry images, so this edge renders the text and drops the images.
/// Non-envelope content (or a parse failure) passes through unchanged.
fn collapse_envelope_text(content: String) -> String {
    if let Ok(envelope) = serde_json::from_str::<cairn_common::read::RunBatchEnvelope>(&content) {
        return envelope.text;
    }
    if let Ok(envelope) = serde_json::from_str::<cairn_common::read::ReadBatchEnvelope>(&content) {
        return envelope.text;
    }
    content
}

fn normalize_tool_payload(tool_name: &str, payload: &mut Value, home_uri: &str) {
    match tool_name {
        "read" => normalize_read_payload(payload, home_uri),
        "write" => normalize_write_payload(payload, home_uri),
        _ => {}
    }
}

fn normalize_read_payload(payload: &mut Value, home_uri: &str) {
    if let Some(paths) = payload.get_mut("paths").and_then(Value::as_array_mut) {
        for path in paths {
            if let Some(target) = path
                .as_str()
                .map(|target| resolve_home_target(target, home_uri))
            {
                *path = Value::String(target);
            }
        }
    }
    if let Some(target) = payload
        .get("path")
        .and_then(Value::as_str)
        .map(|target| resolve_home_target(target, home_uri))
    {
        payload["path"] = Value::String(target);
    }
}

fn normalize_write_payload(payload: &mut Value, home_uri: &str) {
    let Some(changes) = payload.get_mut("changes").and_then(Value::as_array_mut) else {
        return;
    };
    for change in changes {
        let Some(target) = change
            .get("target")
            .and_then(Value::as_str)
            .map(|target| resolve_home_target(target, home_uri))
        else {
            continue;
        };
        change["target"] = Value::String(target);
    }
}

fn resolve_home_target(target: &str, home_uri: &str) -> String {
    let Some(suffix) = target.strip_prefix("cairn:~/") else {
        return target.to_string();
    };
    let (suffix_identity, query) = match suffix.split_once('?') {
        Some((identity, query)) => (identity, Some(query)),
        None => (suffix, None),
    };
    let mut resolved = home_uri.trim_end_matches('/').to_string();
    if !suffix_identity.is_empty() {
        resolved.push('/');
        resolved.push_str(suffix_identity.trim_start_matches('/'));
    }
    if let Some(query) = query {
        resolved.push('?');
        resolved.push_str(query);
    }
    resolved
}

/// Build the OpenRouter `provider` object from routing settings.
///
/// `require_parameters` is always sent: tool schemas are unconditional, so this
/// only formalizes the existing effective behavior (route only to tool-capable
/// providers). `zdr`/`sort` are added only when the user opts in.
fn build_provider_object(routing: &OpenRouterRouting) -> Value {
    let mut provider = json!({ "require_parameters": true });
    if routing.zero_data_retention {
        provider["zdr"] = json!(true);
    }
    if let Some(sort) = routing.sort {
        provider["sort"] = json!(match sort {
            OpenRouterSort::Price => "price",
            OpenRouterSort::Throughput => "throughput",
            OpenRouterSort::Latency => "latency",
        });
    }
    provider
}

/// Rough chars-per-token ratio for the trim heuristic. English and code average
/// ~3.5–4 characters per token; 4 deliberately under-counts token volume, with
/// the slack absorbed by trimming to a fraction of the window
/// (`CONTEXT_FIT_FRACTION`) rather than to its exact edge. Phase 1 only needs to
/// stay under the limit, not account perfectly.
const CHARS_PER_TOKEN: i64 = 4;

/// Trim the outgoing request down to this fraction of the model's real context
/// window. The remaining headroom covers the response (including reasoning
/// tokens) plus slack for the approximate char-per-token estimate.
const CONTEXT_FIT_FRACTION: f64 = 0.75;

/// The most recent messages whose tool outputs are never collapsed, so the model
/// keeps full fidelity on the exchange it is actively reasoning about. Older tool
/// outputs are collapsed oldest-first when the request would exceed budget.
const PROTECT_RECENT_MESSAGES: usize = 8;

/// Rough token estimate for a single message: content, any tool-call name and
/// arguments, and serialized reasoning, divided by the char heuristic plus a
/// small per-message framing overhead.
fn estimate_message_tokens(message: &ChatMessage) -> i64 {
    let mut chars = message.content.as_ref().map(String::len).unwrap_or(0);
    if let Some(calls) = &message.tool_calls {
        for call in calls {
            chars += call.function.name.len() + call.function.arguments.len();
        }
    }
    if let Some(details) = &message.reasoning_details {
        chars += details.to_string().len();
    }
    (chars as i64) / CHARS_PER_TOKEN + 4
}

fn estimate_conversation_tokens(messages: &[ChatMessage]) -> i64 {
    messages.iter().map(estimate_message_tokens).sum()
}

/// The token budget the outgoing request is trimmed to fit under.
fn context_fit_budget(context_window: i64) -> i64 {
    ((context_window as f64) * CONTEXT_FIT_FRACTION) as i64
}

/// Return a view of `messages` that fits under the model's real context window,
/// trimming aged tool outputs only when necessary. Borrows the input untouched
/// when the window is unknown or the request already fits; otherwise returns an
/// owned, trimmed copy. The stored transcript is never affected — only this
/// reconstructed outgoing array.
fn fit_conversation<'a>(
    messages: &'a [ChatMessage],
    context_window: Option<i64>,
) -> Cow<'a, [ChatMessage]> {
    let Some(window) = context_window.filter(|window| *window > 0) else {
        return Cow::Borrowed(messages);
    };
    let budget = context_fit_budget(window);
    if estimate_conversation_tokens(messages) <= budget {
        return Cow::Borrowed(messages);
    }
    Cow::Owned(trim_conversation_to_budget(messages, budget))
}

/// Collapse the oldest tool-result messages to short markers until the estimated
/// request fits under `budget`. The system prompt, user turns, and assistant
/// reasoning / tool-call decisions are never touched (only `tool` messages are
/// eligible), and the most recent exchanges' tool outputs are protected. When
/// even collapsing every eligible tool output cannot reach budget, the request
/// is left as small as possible and the read timeout backstops the over-limit
/// send.
fn trim_conversation_to_budget(messages: &[ChatMessage], budget: i64) -> Vec<ChatMessage> {
    let mut trimmed = messages.to_vec();
    // Map each tool_call id to its tool name so a collapsed marker can name the
    // source (`read`/`write`/`run`). Borrows `messages`, not `trimmed`.
    let mut tool_names: HashMap<&str, &str> = HashMap::new();
    for message in messages {
        if let Some(calls) = &message.tool_calls {
            for call in calls {
                tool_names.insert(call.id.as_str(), call.function.name.as_str());
            }
        }
    }

    let protect_from = trimmed.len().saturating_sub(PROTECT_RECENT_MESSAGES);
    let mut estimate = estimate_conversation_tokens(messages);
    for message in trimmed.iter_mut().take(protect_from) {
        if estimate <= budget {
            break;
        }
        if message.role != "tool" {
            continue;
        }
        let marker = {
            let Some(content) = message.content.as_ref() else {
                continue;
            };
            let lines = content.lines().count().max(1);
            let name = message
                .tool_call_id
                .as_deref()
                .and_then(|id| tool_names.get(id).copied())
                .unwrap_or("tool");
            format!("[{name} output elided — {lines} lines]")
        };
        let before = estimate_message_tokens(message);
        message.content = Some(marker);
        let after = estimate_message_tokens(message);
        estimate -= before - after;
    }
    trimmed
}

#[allow(clippy::too_many_arguments)]
fn post_chat_completion(
    orch: &Orchestrator,
    api_key: &str,
    model: &str,
    session_id: &str,
    messages: &[ChatMessage],
    config: &SessionConfig,
    run_id: &str,
    turn_id: Option<&str>,
    sequence: i32,
    provider: &Value,
    cancel: &Arc<AtomicBool>,
) -> Result<ChatResponse, String> {
    let mut body = json!({
        "model": model,
        "messages": messages,
        "tools": tool_schemas(),
        "tool_choice": "auto",
        "stream": true,
    });
    if let Some(effort) = config.reasoning_effort.as_deref() {
        body["reasoning"] = json!({ "effort": effort });
    }
    body["provider"] = provider.clone();
    post_chat_completion_streaming(
        orch, api_key, body, run_id, session_id, turn_id, sequence, cancel,
    )
}

#[allow(clippy::too_many_arguments)]
fn post_chat_completion_streaming(
    orch: &Orchestrator,
    api_key: &str,
    body: Value,
    run_id: &str,
    session_id: &str,
    turn_id: Option<&str>,
    sequence: i32,
    cancel: &Arc<AtomicBool>,
) -> Result<ChatResponse, String> {
    let client = reqwest::blocking::Client::builder()
        .timeout(STREAM_REQUEST_TIMEOUT)
        .build()
        .map_err(|error| format!("Failed to build OpenRouter streaming client: {error}"))?;
    let response = client
        .post(CHAT_URL)
        .headers(openrouter_headers(api_key)?)
        .json(&body)
        .send()
        .map_err(|error| format!("OpenRouter streaming request failed: {error}"))?;
    let status = response.status();
    if !status.is_success() {
        let text = response
            .text()
            .unwrap_or_else(|error| format!("<failed to read error body: {error}>"));
        return Err(format!(
            "OpenRouter chat completion returned HTTP {}: {}",
            status.as_u16(),
            text
        ));
    }

    let mut aggregate = StreamingAggregate::default();
    let mut stream_state: Option<AssistantStreamState> = None;
    let reader = BufReader::new(response);
    for line in reader.lines() {
        // The cancel flag is observed at SSE line boundaries. During a streaming
        // turn tokens flow near-continuously, so this fires promptly in practice;
        // dropping the response (on break) closes the TCP connection, which is
        // what makes OpenRouter stop billing.
        if cancel.load(Ordering::SeqCst) {
            break;
        }
        let line = line.map_err(|error: std::io::Error| {
            if error.kind() == std::io::ErrorKind::TimedOut {
                format!(
                    "OpenRouter generation exceeded {}s (an over-limit or hung upstream request); finalizing the turn instead of waiting indefinitely.",
                    STREAM_REQUEST_TIMEOUT.as_secs()
                )
            } else {
                format!("OpenRouter stream read failed: {error}")
            }
        })?;
        let Some(data) = line.strip_prefix("data:") else {
            continue;
        };
        let data = data.trim();
        if data.is_empty() {
            continue;
        }
        if data == "[DONE]" {
            break;
        }
        let chunk: ChatStreamChunk = serde_json::from_str(data)
            .map_err(|error| format!("Failed to parse OpenRouter stream chunk: {error}: {data}"))?;
        if let Some(message) = chunk.error_message() {
            // OpenRouter delivers post-stream-start errors in-band (HTTP stays
            // 200). Finalize any open live stream so it is not left dangling,
            // then surface the error; the turn handler records it and crashes
            // the run.
            if let Some(state) = stream_state.take() {
                state.finalize(
                    orch,
                    run_id,
                    session_id,
                    aggregate.text.clone(),
                    aggregate.reasoning.clone(),
                    aggregate.reasoning_detail_values(),
                    aggregate.usage.as_ref(),
                    aggregate.id.as_deref(),
                    aggregate.model.as_deref(),
                )?;
            }
            return Err(message);
        }
        aggregate.apply_chunk(&chunk);
        let reasoning_delta = chunk
            .choices
            .iter()
            .filter_map(|choice| choice.delta.reasoning.as_deref())
            .collect::<String>();
        let content_delta = chunk
            .choices
            .iter()
            .filter_map(|choice| choice.delta.content.as_deref())
            .collect::<String>();
        // Reasoning typically streams before content, so either delta opens the
        // live stream.
        if !reasoning_delta.is_empty() {
            let state = open_stream_state(
                &mut stream_state,
                orch,
                run_id,
                session_id,
                turn_id,
                sequence,
            )?;
            state.append_thinking(orch, run_id, &reasoning_delta)?;
        }
        if !content_delta.is_empty() {
            let state = open_stream_state(
                &mut stream_state,
                orch,
                run_id,
                session_id,
                turn_id,
                sequence,
            )?;
            state.append(orch, run_id, &content_delta)?;
        }
    }

    let streamed_text = stream_state.is_some();
    if let Some(state) = stream_state {
        state.finalize(
            orch,
            run_id,
            session_id,
            aggregate.text.clone(),
            aggregate.reasoning.clone(),
            aggregate.reasoning_detail_values(),
            aggregate.usage.as_ref(),
            aggregate.id.as_deref(),
            aggregate.model.as_deref(),
        )?;
    }

    Ok(aggregate.into_response(streamed_text))
}

fn open_stream_state<'a>(
    state: &'a mut Option<AssistantStreamState>,
    orch: &Orchestrator,
    run_id: &str,
    session_id: &str,
    turn_id: Option<&str>,
    sequence: i32,
) -> Result<&'a mut AssistantStreamState, String> {
    if state.is_none() {
        *state = Some(AssistantStreamState::open(
            orch, run_id, session_id, turn_id, sequence,
        )?);
    }
    Ok(state.as_mut().expect("stream state just initialized"))
}

fn openrouter_headers(api_key: &str) -> Result<HeaderMap, String> {
    let mut headers = HeaderMap::new();
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {api_key}"))
            .map_err(|error| format!("invalid OpenRouter API key header: {error}"))?,
    );
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(
        "HTTP-Referer",
        HeaderValue::from_static("https://cairn.computer"),
    );
    headers.insert("X-OpenRouter-Title", HeaderValue::from_static("Cairn"));
    Ok(headers)
}

fn tool_schemas() -> Vec<Value> {
    vec![
        json!({
            "type": "function",
            "function": {
                "name": "read",
                "description": "Read one or more file, Cairn resource, web, or PDF targets. Prefer paths[] for batch reads.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "paths": { "type": "array", "items": { "type": "string" }, "minItems": 1 },
                        "path": { "type": "string" },
                        "offset": { "type": "integer" },
                        "limit": { "type": "integer" }
                    }
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "write",
                "description": "Apply ordered file/resource mutations. Include commit_msg when touching files.",
                "parameters": {
                    "type": "object",
                    "required": ["changes"],
                    "properties": {
                        "changes": { "type": "array", "items": { "type": "object" }, "minItems": 1 },
                        "commit_msg": { "type": "string" },
                        "preview": { "type": "boolean" },
                        "atomic": { "type": "boolean" }
                    }
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "run",
                "description": "Execute shell commands or skill scripts in the worktree.",
                "parameters": {
                    "type": "object",
                    "required": ["commands"],
                    "properties": {
                        "commands": { "type": "array", "items": { "type": "object" }, "minItems": 1 },
                        "commit_msg": { "type": "string" },
                        "sequential": { "type": "boolean" },
                        "stop_on_error": { "type": "boolean" }
                    }
                }
            }
        }),
    ]
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ChatMessage {
    role: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_call_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_calls: Option<Vec<ToolCall>>,
    // Original structured reasoning, replayed verbatim and in order on the
    // assistant message that requested a tool. Anthropic providers error if the
    // thinking block is missing before a tool_use.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reasoning_details: Option<Value>,
}

impl ChatMessage {
    fn system(content: String) -> Self {
        Self {
            role: "system".to_string(),
            content: Some(content),
            tool_call_id: None,
            tool_calls: None,
            reasoning_details: None,
        }
    }
    fn user(content: String) -> Self {
        Self {
            role: "user".to_string(),
            content: Some(content),
            tool_call_id: None,
            tool_calls: None,
            reasoning_details: None,
        }
    }
    fn tool(tool_call_id: String, content: String) -> Self {
        Self {
            role: "tool".to_string(),
            content: Some(content),
            tool_call_id: Some(tool_call_id),
            tool_calls: None,
            reasoning_details: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct ChatResponse {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    model: Option<String>,
    choices: Vec<ChatChoice>,
    #[serde(default)]
    usage: Option<OpenRouterUsage>,
    #[serde(default)]
    streamed_text: bool,
    // The generation's terminal finish_reason (e.g. "tool_calls", "stop",
    // "length"). "length" flags an output-token cutoff that may truncate the
    // last tool call. Constructed from the stream; never deserialized from wire.
    #[serde(default)]
    finish_reason: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct ChatChoice {
    message: ChatMessage,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ToolCall {
    id: String,
    #[serde(default = "default_function_type")]
    r#type: String,
    function: ToolFunction,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ToolFunction {
    name: String,
    arguments: String,
}

fn default_function_type() -> String {
    "function".to_string()
}

#[derive(Debug, Clone, Deserialize)]
struct ChatStreamChunk {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    choices: Vec<ChatStreamChoice>,
    #[serde(default)]
    usage: Option<OpenRouterUsage>,
    #[serde(default)]
    error: Option<StreamError>,
}

impl ChatStreamChunk {
    /// Detect an in-band error chunk: OpenRouter delivers post-stream-start
    /// errors as a top-level `error` object (HTTP stays 200) and/or a choice
    /// whose `finish_reason` is `"error"`. Returns the provider message.
    fn error_message(&self) -> Option<String> {
        if let Some(error) = &self.error {
            return Some(error.message.clone());
        }
        if self
            .choices
            .iter()
            .any(|choice| choice.finish_reason.as_deref() == Some("error"))
        {
            return Some("OpenRouter stream reported finish_reason=error".to_string());
        }
        None
    }
}

#[derive(Debug, Clone, Deserialize)]
struct StreamError {
    #[serde(default)]
    #[allow(dead_code)]
    code: Option<Value>,
    message: String,
}

#[derive(Debug, Clone, Deserialize)]
struct ChatStreamChoice {
    #[serde(default)]
    delta: ChatStreamDelta,
    #[serde(default)]
    finish_reason: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct ChatStreamDelta {
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    tool_calls: Option<Vec<StreamingToolCallDelta>>,
    #[serde(default)]
    reasoning: Option<String>,
    // Kept verbatim as raw JSON (not reshaped into typed structs) so order and
    // round-trip fidelity for signature/encrypted/format fields are preserved.
    #[serde(default)]
    reasoning_details: Option<Vec<Value>>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct StreamingToolCallDelta {
    index: usize,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    r#type: Option<String>,
    #[serde(default)]
    function: Option<StreamingToolFunctionDelta>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct StreamingToolFunctionDelta {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    arguments: Option<String>,
}

#[derive(Debug, Default)]
struct StreamingAggregate {
    id: Option<String>,
    model: Option<String>,
    text: String,
    reasoning: String,
    reasoning_details: Vec<ReasoningDetailBuilder>,
    tool_calls: Vec<StreamingToolCallBuilder>,
    usage: Option<OpenRouterUsage>,
    finish_reason: Option<String>,
}

#[derive(Debug, Default)]
struct StreamingToolCallBuilder {
    id: Option<String>,
    r#type: Option<String>,
    name: String,
    arguments: String,
}

/// Accumulates one streaming `reasoning_details` block. OpenRouter streams these
/// incrementally and keyed by `index`: text/summary/signature/data arrive as
/// string deltas across chunks, while type/id/format are sent once. Appending
/// each delta as its own array element instead of merging by index splits a
/// thinking block's text from its signature, which Anthropic rejects on replay
/// with "Invalid `signature` in `thinking` block".
#[derive(Debug, Default)]
struct ReasoningDetailBuilder {
    /// Set-once metadata (type, id, format, index, and any unrecognized key).
    meta: serde_json::Map<String, Value>,
    text: Option<String>,
    summary: Option<String>,
    signature: Option<String>,
    data: Option<String>,
}

impl ReasoningDetailBuilder {
    fn apply(&mut self, delta: &Value) {
        let Some(obj) = delta.as_object() else {
            return;
        };
        for (key, value) in obj {
            match key.as_str() {
                "text" => append_reasoning_field(&mut self.text, value),
                "summary" => append_reasoning_field(&mut self.summary, value),
                "signature" => append_reasoning_field(&mut self.signature, value),
                "data" => append_reasoning_field(&mut self.data, value),
                _ => {
                    self.meta.insert(key.clone(), value.clone());
                }
            }
        }
    }

    fn to_value(&self) -> Value {
        let mut obj = self.meta.clone();
        if let Some(text) = &self.text {
            obj.insert("text".to_string(), Value::String(text.clone()));
        }
        if let Some(summary) = &self.summary {
            obj.insert("summary".to_string(), Value::String(summary.clone()));
        }
        if let Some(signature) = &self.signature {
            obj.insert("signature".to_string(), Value::String(signature.clone()));
        }
        if let Some(data) = &self.data {
            obj.insert("data".to_string(), Value::String(data.clone()));
        }
        Value::Object(obj)
    }
}

fn append_reasoning_field(slot: &mut Option<String>, value: &Value) {
    if let Some(text) = value.as_str() {
        slot.get_or_insert_with(String::new).push_str(text);
    }
}

impl StreamingAggregate {
    fn apply_chunk(&mut self, chunk: &ChatStreamChunk) {
        if self.id.is_none() {
            self.id = chunk.id.clone();
        }
        if self.model.is_none() {
            self.model = chunk.model.clone();
        }
        if chunk.usage.is_some() {
            self.usage = chunk.usage.clone();
        }
        for choice in &chunk.choices {
            if let Some(reason) = &choice.finish_reason {
                self.finish_reason = Some(reason.clone());
            }
            if let Some(content) = choice.delta.content.as_deref() {
                self.text.push_str(content);
            }
            if let Some(reasoning) = choice.delta.reasoning.as_deref() {
                self.reasoning.push_str(reasoning);
            }
            if let Some(details) = &choice.delta.reasoning_details {
                for detail in details {
                    // OpenRouter keys each block by `index`; merge deltas into the
                    // matching builder so text and its signature stay one block.
                    let index = detail.get("index").and_then(Value::as_u64).unwrap_or(0) as usize;
                    while self.reasoning_details.len() <= index {
                        self.reasoning_details
                            .push(ReasoningDetailBuilder::default());
                    }
                    self.reasoning_details[index].apply(detail);
                }
            }
            if let Some(tool_calls) = &choice.delta.tool_calls {
                for delta in tool_calls {
                    while self.tool_calls.len() <= delta.index {
                        self.tool_calls.push(StreamingToolCallBuilder::default());
                    }
                    let builder = &mut self.tool_calls[delta.index];
                    if let Some(id) = delta.id.as_deref() {
                        builder.id = Some(id.to_string());
                    }
                    if let Some(kind) = delta.r#type.as_deref() {
                        builder.r#type = Some(kind.to_string());
                    }
                    if let Some(function) = &delta.function {
                        if let Some(name) = function.name.as_deref() {
                            builder.name.push_str(name);
                        }
                        if let Some(arguments) = function.arguments.as_deref() {
                            builder.arguments.push_str(arguments);
                        }
                    }
                }
            }
        }
    }

    fn tool_calls(&self) -> Vec<ToolCall> {
        self.tool_calls
            .iter()
            .enumerate()
            .filter_map(|(index, builder)| {
                if builder.name.is_empty() {
                    log::warn!(
                        "OpenRouter dropping streamed tool call #{index} with empty function name (id={:?}, {} arg bytes)",
                        builder.id,
                        builder.arguments.len()
                    );
                    return None;
                }
                Some(ToolCall {
                    id: builder
                        .id
                        .clone()
                        .unwrap_or_else(|| format!("openrouter-tool-{index}")),
                    r#type: builder.r#type.clone().unwrap_or_else(default_function_type),
                    function: ToolFunction {
                        name: builder.name.clone(),
                        arguments: builder.arguments.clone(),
                    },
                })
            })
            .collect()
    }

    fn reasoning_detail_values(&self) -> Vec<Value> {
        self.reasoning_details
            .iter()
            .map(ReasoningDetailBuilder::to_value)
            .collect()
    }

    fn into_response(self, streamed_text: bool) -> ChatResponse {
        let tool_calls = self.tool_calls();
        let reasoning_details = if self.reasoning_details.is_empty() {
            None
        } else {
            Some(Value::Array(self.reasoning_detail_values()))
        };
        ChatResponse {
            id: self.id,
            model: self.model,
            choices: vec![ChatChoice {
                message: ChatMessage {
                    role: "assistant".to_string(),
                    content: if self.text.is_empty() {
                        None
                    } else {
                        Some(self.text)
                    },
                    tool_call_id: None,
                    tool_calls: if tool_calls.is_empty() {
                        None
                    } else {
                        Some(tool_calls)
                    },
                    reasoning_details,
                },
            }],
            usage: self.usage,
            streamed_text,
            finish_reason: self.finish_reason,
        }
    }
}

struct AssistantStreamState {
    stream_id: String,
    version: i32,
    acc: StreamAccumulator,
}

impl AssistantStreamState {
    fn open(
        orch: &Orchestrator,
        run_id: &str,
        session_id: &str,
        turn_id: Option<&str>,
        sequence: i32,
    ) -> Result<Self, String> {
        let stream = open_stream(
            orch.db.local.clone(),
            run_id,
            Some(session_id),
            turn_id,
            OPENROUTER_BACKEND_KEY,
            Some(sequence),
        )?;
        let _ = orch.services.emitter.emit(
            "db-change",
            crate::notify::event_db_change(run_id, Some(session_id), "insert"),
        );
        Ok(Self {
            stream_id: stream.stream_id().to_string(),
            version: stream.version(),
            acc: StreamAccumulator::new(),
        })
    }

    fn append(&mut self, orch: &Orchestrator, run_id: &str, text: &str) -> Result<(), String> {
        self.acc.push_content(text);
        let now = std::time::Instant::now();
        if self.acc.should_flush(now) {
            self.flush(orch)?;
        }
        if self.acc.should_emit(now) {
            let delta = self.acc.take_emit_delta();
            emit_streaming_delta(orch, run_id, &self.stream_id, &delta);
        }
        Ok(())
    }

    fn append_thinking(
        &mut self,
        orch: &Orchestrator,
        run_id: &str,
        text: &str,
    ) -> Result<(), String> {
        self.acc.push_thinking(text);
        let now = std::time::Instant::now();
        if self.acc.should_flush(now) {
            self.flush(orch)?;
        }
        if self.acc.should_emit(now) {
            let delta = self.acc.take_emit_delta();
            emit_streaming_delta(orch, run_id, &self.stream_id, &delta);
        }
        Ok(())
    }

    fn flush(&mut self, orch: &Orchestrator) -> Result<(), String> {
        if self.acc.pending_is_empty() {
            return Ok(());
        }
        let pending = self.acc.take_pending();
        let appended = append_chunks(
            orch.db.local.clone(),
            &self.stream_id,
            self.version,
            &pending,
        )?;
        self.version = appended.version;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn finalize(
        mut self,
        orch: &Orchestrator,
        run_id: &str,
        session_id: &str,
        text: String,
        thinking: String,
        reasoning_details: Vec<Value>,
        usage: Option<&OpenRouterUsage>,
        generation_id: Option<&str>,
        model: Option<&str>,
    ) -> Result<(), String> {
        self.flush(orch)?;
        if self.acc.has_unemitted() {
            let delta = self.acc.take_emit_delta();
            emit_streaming_delta(orch, run_id, &self.stream_id, &delta);
        }
        let event = TranscriptEvent {
            event_type: "assistant".to_string(),
            session_id: Some(session_id.to_string()),
            parent_tool_use_id: None,
            // None on an empty body so a reasoning-only turn (think then call a
            // tool) yields a valid thinking-only event instead of content: "".
            content: if text.is_empty() { None } else { Some(text) },
            thinking: (!thinking.is_empty()).then_some(thinking),
            tool_name: None,
            tool_input: None,
            tool_uses: None,
            tool_use_id: None,
            tool_result: None,
            is_error: false,
            thinking_ms: None,
            raw: Some(json!({
                "backend": OPENROUTER_BACKEND_KEY,
                "generationId": generation_id,
                "model": model,
                "usage": usage,
                "streamed": true,
                "reasoningDetails": reasoning_details,
            })),
        };
        let finalized = finalize_stream_emit(
            orch.db.local.clone(),
            &orch.services.emitter,
            &self.stream_id,
            self.version,
            Some(event),
            usage.map(OpenRouterUsage::token_counts).unwrap_or_default(),
        )?;
        process_post_commit_outbox(orch, &finalized.outbox_entries);
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OpenRouterUsage {
    #[serde(default)]
    prompt_tokens: Option<i32>,
    #[serde(default)]
    completion_tokens: Option<i32>,
    #[serde(default)]
    total_tokens: Option<i32>,
    #[serde(default)]
    reasoning_tokens: Option<i32>,
    #[serde(default)]
    prompt_tokens_details: Option<Value>,
    #[serde(default)]
    completion_tokens_details: Option<Value>,
    #[serde(default)]
    cost: Option<f64>,
    #[serde(default)]
    cost_details: Option<Value>,
}

impl OpenRouterUsage {
    fn token_counts(&self) -> TokenCounts {
        TokenCounts {
            input: self.prompt_tokens,
            output: self.completion_tokens,
            cache_read: self.prompt_tokens_details.as_ref().and_then(|v| {
                v.get("cached_tokens")
                    .and_then(Value::as_i64)
                    .map(|n| n as i32)
            }),
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

#[allow(clippy::too_many_arguments)]
fn store_assistant_message(
    orch: &Orchestrator,
    run_id: &str,
    session_id: &str,
    turn_id: Option<&str>,
    sequence: i32,
    text: &str,
    usage: Option<&OpenRouterUsage>,
    generation_id: Option<&str>,
    model: Option<&str>,
    total_cost: Option<f64>,
) -> Result<(), String> {
    let stream = open_stream(
        orch.db.local.clone(),
        run_id,
        Some(session_id),
        turn_id,
        OPENROUTER_BACKEND_KEY,
        Some(sequence),
    )?;
    let appended = append_chunks(
        orch.db.local.clone(),
        stream.stream_id(),
        stream.version(),
        &[StreamChunkInput::content(text.to_string())],
    )?;
    let event = TranscriptEvent {
        event_type: "assistant".to_string(),
        session_id: Some(session_id.to_string()),
        parent_tool_use_id: None,
        content: Some(text.to_string()),
        thinking: None,
        tool_name: None,
        tool_input: None,
        tool_uses: None,
        tool_use_id: None,
        tool_result: None,
        is_error: false,
        thinking_ms: None,
        raw: Some(json!({
            "backend": OPENROUTER_BACKEND_KEY,
            "generationId": generation_id,
            "model": model,
            "totalCost": total_cost,
            "usage": usage,
        })),
    };
    let finalized = finalize_stream_emit(
        orch.db.local.clone(),
        &orch.services.emitter,
        stream.stream_id(),
        appended.version,
        Some(event),
        usage.map(OpenRouterUsage::token_counts).unwrap_or_default(),
    )?;
    process_post_commit_outbox(orch, &finalized.outbox_entries);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn emit_streaming_delta(orch: &Orchestrator, run_id: &str, event_id: &str, delta: &EmitDelta) {
    let _ = orch.services.emitter.emit(
        "streaming-update",
        json!({
            "run_id": run_id,
            "event_id": event_id,
            "content_delta": delta.content_delta,
            "content_len": delta.content_len,
            "thinking_delta": delta.thinking_delta,
            "thinking_len": delta.thinking_len,
        }),
    );
}

fn tool_use_infos(tool_calls: &[ToolCall]) -> Vec<ToolUseInfo> {
    tool_calls
        .iter()
        .map(|call| ToolUseInfo {
            id: call.id.clone(),
            name: call.function.name.clone(),
            input: repair::parse_tool_arguments(&call.function.arguments).value(),
        })
        .collect()
}

fn single_tool_summary(tool_uses: &[ToolUseInfo]) -> (Option<String>, Option<Value>) {
    if tool_uses.len() == 1 {
        (
            Some(tool_uses[0].name.clone()),
            Some(tool_uses[0].input.clone()),
        )
    } else {
        (None, None)
    }
}

/// The usage to record on a non-streamed tool-call assistant event.
///
/// When the generation's content or reasoning streamed, its usage already landed
/// on the finalized streamed assistant event (`AssistantStreamState::finalize`),
/// so attaching it again here would double-count in the token rollup. A
/// non-streamed tool-call response (no content/reasoning deltas, so no stream was
/// opened) has no other assistant event for that generation, so its tokens must
/// be recorded on this event or they are dropped from the usage breakdown even
/// though the generation's cost is still counted on the cumulative result event.
fn tool_call_usage(
    streamed_text: bool,
    usage: Option<&OpenRouterUsage>,
) -> Option<&OpenRouterUsage> {
    if streamed_text {
        None
    } else {
        usage
    }
}

#[allow(clippy::too_many_arguments)]
fn store_assistant_tool_call(
    orch: &Orchestrator,
    run_id: &str,
    session_id: &str,
    turn_id: Option<&str>,
    sequence: i32,
    text: &str,
    tool_calls: &[ToolCall],
    usage: Option<&OpenRouterUsage>,
    generation_id: Option<&str>,
    model: Option<&str>,
    reasoning_details: Option<&Value>,
) -> Result<(), String> {
    let tool_uses = tool_use_infos(tool_calls);
    let (tool_name, tool_input) = single_tool_summary(&tool_uses);
    insert_transcript_event(
        orch,
        run_id,
        session_id,
        turn_id,
        sequence,
        TranscriptEvent {
            event_type: "assistant".to_string(),
            session_id: Some(session_id.to_string()),
            parent_tool_use_id: None,
            content: if text.is_empty() {
                None
            } else {
                Some(text.to_string())
            },
            thinking: None,
            tool_name,
            tool_input,
            tool_uses: Some(tool_uses),
            tool_use_id: None,
            tool_result: None,
            is_error: false,
            thinking_ms: None,
            raw: Some(json!({
                "backend": OPENROUTER_BACKEND_KEY,
                "generationId": generation_id,
                "model": model,
                "usage": usage,
                // Replayed verbatim on resume so the tool-requesting assistant
                // message carries its original thinking block before tool_use.
                "reasoningDetails": reasoning_details,
            })),
        },
        usage.map(OpenRouterUsage::token_counts).unwrap_or_default(),
        None,
    )
}

fn store_tool_result(
    orch: &Orchestrator,
    run_id: &str,
    session_id: &str,
    turn_id: Option<&str>,
    sequence: i32,
    tool_call_id: &str,
    result: &DispatchOutput,
) -> Result<(), String> {
    insert_transcript_event(
        orch,
        run_id,
        session_id,
        turn_id,
        sequence,
        TranscriptEvent {
            event_type: "tool_result".to_string(),
            session_id: Some(session_id.to_string()),
            parent_tool_use_id: None,
            content: None,
            thinking: None,
            tool_name: None,
            tool_input: None,
            tool_uses: None,
            tool_use_id: Some(tool_call_id.to_string()),
            tool_result: Some(render_tool_result(result.clone())),
            is_error: false,
            thinking_ms: None,
            raw: Some(json!({"backend": OPENROUTER_BACKEND_KEY, "reminders": result.reminders})),
        },
        TokenCounts::default(),
        None,
    )
}

#[allow(clippy::too_many_arguments)]
fn store_success_result(
    orch: &Orchestrator,
    run_id: &str,
    session_id: &str,
    turn_id: Option<&str>,
    sequence: i32,
    usage: Option<&OpenRouterUsage>,
    generation_id: Option<&str>,
    model: Option<&str>,
    total_cost: Option<f64>,
) -> Result<(), String> {
    insert_transcript_event(
        orch,
        run_id,
        session_id,
        turn_id,
        sequence,
        TranscriptEvent {
            event_type: "result:success".to_string(),
            session_id: Some(session_id.to_string()),
            parent_tool_use_id: None,
            content: None,
            thinking: None,
            tool_name: None,
            tool_input: None,
            tool_uses: None,
            tool_use_id: None,
            tool_result: None,
            is_error: false,
            thinking_ms: None,
            raw: Some(json!({
                "backend": OPENROUTER_BACKEND_KEY,
                "generationId": generation_id,
                "model": model,
                "totalCost": total_cost,
                "usage": usage,
            })),
        },
        usage.map(OpenRouterUsage::token_counts).unwrap_or_default(),
        total_cost,
    )
}

#[allow(clippy::too_many_arguments)]
fn insert_transcript_event(
    orch: &Orchestrator,
    run_id: &str,
    session_id: &str,
    turn_id: Option<&str>,
    sequence: i32,
    event: TranscriptEvent,
    counts: TokenCounts,
    cost_usd: Option<f64>,
) -> Result<(), String> {
    let now = chrono::Utc::now().timestamp() as i32;
    let data = serde_json::to_string(&event).map_err(|error| error.to_string())?;
    insert_event_emit(
        orch.db.local.clone(),
        &orch.services.emitter,
        EventInsert {
            id: Uuid::new_v4().to_string(),
            run_id: run_id.to_string(),
            session_id: Some(session_id.to_string()),
            sequence,
            timestamp: now,
            event_type: event.event_type.clone(),
            data,
            parent_tool_use_id: event.parent_tool_use_id.clone(),
            created_at: now,
            input_tokens: counts.input,
            cache_read_tokens: counts.cache_read,
            cache_create_tokens: counts.cache_create,
            output_tokens: counts.output,
            thinking_tokens: counts.thinking,
            turn_id: turn_id.map(str::to_string),
            cost_usd,
        },
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        build_provider_object, collapse_envelope_text, context_fit_budget, default_function_type,
        estimate_conversation_tokens, fit_conversation, normalize_tool_payload, prepare_tool_call,
        render_tool_result, reorder_tool_results_by_call_order, resolve_home_target,
        store_assistant_tool_call, store_tool_result, tool_call_usage, tool_schemas,
        transcript_event_to_chat_message, trim_conversation_to_budget, AssistantStreamState,
        ChatMessage, ChatStreamChunk, ChatStreamDelta, OpenRouterUsage, PreparedCall,
        StreamingAggregate, ToolCall, ToolFunction,
    };
    use crate::agent_process::stream::{TokenCounts, ToolUseInfo, TranscriptEvent};
    use crate::dispatch::DispatchOutput;
    use crate::models::{OpenRouterRouting, OpenRouterSort};
    use serde_json::json;

    #[test]
    fn provider_object_default_only_requires_parameters() {
        let provider = build_provider_object(&OpenRouterRouting::default());
        assert_eq!(provider, json!({ "require_parameters": true }));
    }

    #[test]
    fn provider_object_includes_zdr_when_opted_in() {
        let provider = build_provider_object(&OpenRouterRouting {
            zero_data_retention: true,
            sort: None,
        });
        assert_eq!(provider["zdr"], json!(true));
        assert_eq!(provider["require_parameters"], json!(true));
        assert!(provider.get("sort").is_none());
    }

    #[test]
    fn provider_object_maps_each_sort_variant() {
        for (variant, expected) in [
            (OpenRouterSort::Price, "price"),
            (OpenRouterSort::Throughput, "throughput"),
            (OpenRouterSort::Latency, "latency"),
        ] {
            let provider = build_provider_object(&OpenRouterRouting {
                zero_data_retention: false,
                sort: Some(variant),
            });
            assert_eq!(provider["sort"], json!(expected));
            assert!(provider.get("zdr").is_none());
        }
    }

    #[test]
    fn provider_object_combines_zdr_and_sort() {
        let provider = build_provider_object(&OpenRouterRouting {
            zero_data_retention: true,
            sort: Some(OpenRouterSort::Latency),
        });
        assert_eq!(provider["require_parameters"], json!(true));
        assert_eq!(provider["zdr"], json!(true));
        assert_eq!(provider["sort"], json!("latency"));
    }

    #[test]
    fn tool_schemas_include_core_verbs() {
        let names: Vec<_> = tool_schemas()
            .into_iter()
            .filter_map(|schema| {
                schema
                    .pointer("/function/name")
                    .and_then(|v| v.as_str())
                    .map(str::to_string)
            })
            .collect();
        assert_eq!(names, vec!["read", "write", "run"]);
    }

    #[test]
    fn collapse_envelope_text_extracts_run_text_and_drops_images() {
        // The OpenAI tool-message format is text-only, so the run/read envelope
        // collapses to its text on the OpenRouter edge; image blocks are dropped
        // (they cannot ride a chat-completions tool result).
        let envelope = cairn_common::read::RunBatchEnvelope {
            text: "=== look ===\nAX tree".to_string(),
            images: vec![cairn_common::read::ImageBlock {
                mime_type: "image/png".to_string(),
                data: "b64".to_string(),
            }],
        };
        let json = serde_json::to_string(&envelope).unwrap();
        assert_eq!(collapse_envelope_text(json), "=== look ===\nAX tree");
    }

    #[test]
    fn collapse_envelope_text_passes_through_non_envelope() {
        let plain = "Exit code: 0".to_string();
        assert_eq!(collapse_envelope_text(plain.clone()), plain);
    }

    #[test]
    fn tool_result_renders_reminders_after_content() {
        let rendered = render_tool_result(DispatchOutput {
            content: "ok".into(),
            reminders: vec!["note".into()],
        });
        assert!(rendered.starts_with("ok"));
        assert!(rendered.contains("<system-reminder>"));
    }

    #[test]
    fn usage_maps_token_counts() {
        let usage = OpenRouterUsage {
            prompt_tokens: Some(10),
            completion_tokens: Some(5),
            total_tokens: Some(15),
            reasoning_tokens: None,
            prompt_tokens_details: Some(json!({"cached_tokens": 3})),
            completion_tokens_details: Some(json!({"reasoning_tokens": 2})),
            cost: None,
            cost_details: None,
        };
        assert_eq!(
            usage.token_counts(),
            TokenCounts {
                input: Some(10),
                output: Some(5),
                cache_read: Some(3),
                cache_create: None,
                thinking: Some(2)
            }
        );
    }

    fn function_call(id: &str) -> ToolCall {
        ToolCall {
            id: id.to_string(),
            r#type: default_function_type(),
            function: ToolFunction {
                name: "write".to_string(),
                arguments: "{}".to_string(),
            },
        }
    }

    fn assistant_with_calls(ids: &[&str]) -> ChatMessage {
        ChatMessage {
            role: "assistant".to_string(),
            content: None,
            tool_call_id: None,
            tool_calls: Some(ids.iter().map(|id| function_call(id)).collect()),
            reasoning_details: None,
        }
    }

    #[test]
    fn reorder_tool_results_follows_assistant_tool_call_order() {
        // A suspended first tool call (`w`) persists the later call's placeholder
        // (`r`) at suspend time and its own real result only on resume, so the
        // raw event order is [assistant, tool(r), tool(w)] — the wrong order for
        // the assistant's tool_calls [w, r].
        let messages = vec![
            ChatMessage::user("hi".to_string()),
            assistant_with_calls(&["w", "r"]),
            ChatMessage::tool("r".to_string(), "placeholder".to_string()),
            ChatMessage::tool("w".to_string(), "real answer".to_string()),
        ];
        let reordered = reorder_tool_results_by_call_order(messages);
        // Tool results now follow the assistant's tool_calls order: w then r.
        assert_eq!(reordered[2].tool_call_id.as_deref(), Some("w"));
        assert_eq!(reordered[2].content.as_deref(), Some("real answer"));
        assert_eq!(reordered[3].tool_call_id.as_deref(), Some("r"));
        // Non-tool messages keep their position.
        assert_eq!(reordered[0].role, "user");
        assert_eq!(reordered[1].role, "assistant");
    }

    #[test]
    fn reorder_tool_results_is_noop_when_already_ordered() {
        let messages = vec![
            assistant_with_calls(&["a", "b"]),
            ChatMessage::tool("a".to_string(), "ra".to_string()),
            ChatMessage::tool("b".to_string(), "rb".to_string()),
        ];
        let reordered = reorder_tool_results_by_call_order(messages);
        assert_eq!(reordered[1].tool_call_id.as_deref(), Some("a"));
        assert_eq!(reordered[2].tool_call_id.as_deref(), Some("b"));
    }

    #[test]
    fn chat_message_serializes_tool_call_id_for_tool_role() {
        let value =
            serde_json::to_value(ChatMessage::tool("call-1".into(), "result".into())).unwrap();
        assert_eq!(value["role"], "tool");
        assert_eq!(value["tool_call_id"], "call-1");
    }

    #[test]
    fn normalizes_home_relative_tool_targets() {
        let home = "cairn://p/CAIRN/1883/1/builder";
        assert_eq!(
            resolve_home_target("cairn:~/todos?limit=1", home),
            "cairn://p/CAIRN/1883/1/builder/todos?limit=1"
        );

        let mut read = json!({"paths": ["cairn:~/todos", "file:src/lib.rs"]});
        normalize_tool_payload("read", &mut read, home);
        assert_eq!(read["paths"][0], "cairn://p/CAIRN/1883/1/builder/todos");
        assert_eq!(read["paths"][1], "file:src/lib.rs");

        let mut write = json!({"changes": [{"target": "cairn:~/messages", "mode": "append"}]});
        normalize_tool_payload("write", &mut write, home);
        assert_eq!(
            write["changes"][0]["target"],
            "cairn://p/CAIRN/1883/1/builder/messages"
        );
    }

    #[test]
    fn delta_parses_reasoning_and_reasoning_details() {
        let delta: ChatStreamDelta = serde_json::from_value(json!({
            "reasoning": "let me think",
            "reasoning_details": [{
                "type": "reasoning.text",
                "id": "rd-1",
                "format": "anthropic-claude-v1",
                "index": 0,
                "signature": "sig-abc",
                "text": "structured thought"
            }]
        }))
        .unwrap();
        assert_eq!(delta.reasoning.as_deref(), Some("let me think"));
        let details = delta.reasoning_details.expect("reasoning_details present");
        assert_eq!(details.len(), 1);
        assert_eq!(details[0]["type"], "reasoning.text");
        assert_eq!(details[0]["signature"], "sig-abc");
        assert_eq!(details[0]["index"], 0);
        assert_eq!(details[0]["format"], "anthropic-claude-v1");
    }

    #[test]
    fn reasoning_details_round_trip_through_conversation() {
        let details = json!([
            {"type": "reasoning.text", "id": "rd-0", "index": 0, "text": "first"},
            {"type": "reasoning.encrypted", "id": "rd-1", "index": 1, "data": "xxx"}
        ]);
        let event = TranscriptEvent {
            event_type: "assistant".to_string(),
            session_id: Some("s1".to_string()),
            parent_tool_use_id: None,
            content: None,
            thinking: None,
            tool_name: None,
            tool_input: None,
            tool_uses: Some(vec![ToolUseInfo {
                id: "call-1".to_string(),
                name: "read".to_string(),
                input: json!({"paths": ["file:lib.rs"]}),
            }]),
            tool_use_id: None,
            tool_result: None,
            is_error: false,
            thinking_ms: None,
            raw: Some(json!({"reasoning_details": details.clone()})),
        };
        let message = transcript_event_to_chat_message("assistant", event)
            .expect("assistant message reconstructed");
        assert_eq!(message.reasoning_details, Some(details.clone()));
        let serialized = serde_json::to_value(&message).unwrap();
        assert_eq!(serialized["reasoning_details"], details);
        // Original order is preserved through store and reconstruct.
        assert_eq!(serialized["reasoning_details"][0]["id"], "rd-0");
        assert_eq!(serialized["reasoning_details"][1]["id"], "rd-1");

        // A message built without any reasoning omits the key entirely.
        let plain = serde_json::to_value(ChatMessage::user("hi".to_string())).unwrap();
        assert!(plain.get("reasoning_details").is_none());
    }

    #[test]
    fn tool_call_without_reasoning_details_omits_key_on_resume() {
        // Ordinary (non-reasoning) tool-call turn: store_assistant_tool_call writes
        // `"reasoningDetails": null`, and a missing key behaves the same. Neither
        // should reconstruct a `reasoning_details: null` chat message.
        for raw in [
            json!({"backend": "openrouter", "reasoningDetails": serde_json::Value::Null}),
            json!({"backend": "openrouter"}),
            json!({"backend": "openrouter", "reasoningDetails": []}),
        ] {
            let event = TranscriptEvent {
                event_type: "assistant".to_string(),
                session_id: Some("s1".to_string()),
                parent_tool_use_id: None,
                content: None,
                thinking: None,
                tool_name: None,
                tool_input: None,
                tool_uses: Some(vec![ToolUseInfo {
                    id: "call-1".to_string(),
                    name: "read".to_string(),
                    input: json!({"paths": ["file:lib.rs"]}),
                }]),
                tool_use_id: None,
                tool_result: None,
                is_error: false,
                thinking_ms: None,
                raw: Some(raw),
            };
            let message = transcript_event_to_chat_message("assistant", event)
                .expect("assistant message reconstructed");
            assert_eq!(message.reasoning_details, None);
            let serialized = serde_json::to_value(&message).unwrap();
            assert!(serialized.get("reasoning_details").is_none());
        }
    }

    #[test]
    fn system_prompt_carries_uri_shapes_workspace_and_orientation() {
        // The wire system message must be the full assembled prompt, not the agent
        // role content alone — otherwise the model never gets Cairn's path/URI
        // conventions or its own working directory and mis-paths file reads.
        use crate::orchestrator::session::assemble_prompt_segments;
        let cairn = crate::system_prompt::cairn_system_prompt();
        let agent = "<agent_role>\nbuilder body\n\n## Orientation\n\n- Working directory (cwd): `/work/dir/x`\n</agent_role>";
        let dynamic =
            "\n\n## Orientation\n\n- Working directory (cwd): `/work/dir/x`\n</agent_role>";
        let segments = assemble_prompt_segments(
            &cairn,
            Some("workspace doctrine"),
            Some("project doctrine"),
            Some(agent),
            Some(dynamic),
        );
        let flattened = super::flatten_prompt_segments(&segments);
        // Cairn system-prompt layer: read/write/run verbs and path schemas.
        assert!(
            flattened.contains("## URI Shapes"),
            "missing Cairn URI guidance"
        );
        // Workspace + project instruction layers.
        assert!(
            flattened.contains("workspace doctrine"),
            "missing workspace instructions"
        );
        assert!(
            flattened.contains("project doctrine"),
            "missing project instructions"
        );
        // Orientation cwd from the dynamic tail.
        assert!(flattened.contains("/work/dir/x"), "missing orientation cwd");
        // Wire prompt equals the persisted concatenation (same segment source).
        let persisted: String = segments.iter().map(|s| s.text.as_str()).collect();
        assert_eq!(flattened, persisted);
    }

    #[test]
    fn reasoning_details_merge_by_index_across_chunks() {
        // A thinking block streams across chunks: text fragments first, signature
        // last. They must coalesce into ONE block (merged by index), not three
        // fragmented entries, or Anthropic rejects the replayed signature.
        let mut aggregate = StreamingAggregate::default();
        let chunk1: ChatStreamChunk = serde_json::from_value(json!({
            "choices": [{"delta": {"reasoning_details": [{
                "type": "reasoning.text",
                "id": "rd-0",
                "format": "anthropic-claude-v1",
                "index": 0,
                "text": "Let me "
            }]}}]
        }))
        .unwrap();
        let chunk2: ChatStreamChunk = serde_json::from_value(json!({
            "choices": [{"delta": {"reasoning_details": [{
                "index": 0,
                "text": "think.",
                "signature": "sig-final"
            }]}}]
        }))
        .unwrap();
        aggregate.apply_chunk(&chunk1);
        aggregate.apply_chunk(&chunk2);
        let details = aggregate.reasoning_detail_values();
        assert_eq!(
            details.len(),
            1,
            "deltas for one index merge into one block"
        );
        assert_eq!(details[0]["text"], "Let me think.");
        assert_eq!(details[0]["signature"], "sig-final");
        assert_eq!(details[0]["type"], "reasoning.text");
        assert_eq!(details[0]["id"], "rd-0");
        assert_eq!(details[0]["format"], "anthropic-claude-v1");
        assert_eq!(details[0]["index"], 0);
    }

    #[test]
    fn streaming_aggregate_accumulates_split_tool_call_deltas() {
        // The tool name/id arrive in the first delta and the JSON arguments stream
        // in fragments across later deltas. They must coalesce (merged by index)
        // into one complete `tool_calls` entry in the response, or `run_http_turn`
        // sees `tool_calls.is_empty()`, skips execution, and the read never fires.
        let mut aggregate = StreamingAggregate::default();
        let chunk1: ChatStreamChunk = serde_json::from_value(json!({
            "choices": [{"delta": {
                "reasoning": "thinking",
                "tool_calls": [{
                    "index": 0,
                    "id": "call-1",
                    "type": "function",
                    "function": {"name": "read", "arguments": "{\"paths\":[\"fi"}
                }]
            }}]
        }))
        .unwrap();
        let chunk2: ChatStreamChunk = serde_json::from_value(json!({
            "choices": [{"delta": {
                "tool_calls": [{
                    "index": 0,
                    "function": {"arguments": "le:src/lib.rs\"]}"}
                }]
            }}]
        }))
        .unwrap();
        aggregate.apply_chunk(&chunk1);
        aggregate.apply_chunk(&chunk2);

        let response = aggregate.into_response(true);
        let calls = response.choices[0]
            .message
            .tool_calls
            .as_ref()
            .expect("tool_calls accumulated across chunks");
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].id, "call-1");
        assert_eq!(calls[0].function.name, "read");
        // Argument fragments concatenate into valid JSON the dispatch path parses.
        let input: serde_json::Value = serde_json::from_str(&calls[0].function.arguments).unwrap();
        assert_eq!(input["paths"][0], "file:src/lib.rs");
    }

    #[test]
    fn mid_stream_error_chunk_is_detected() {
        let chunk: ChatStreamChunk = serde_json::from_value(json!({
            "error": {"code": 502, "message": "upstream failed"},
            "choices": [{"delta": {}, "finish_reason": "error"}]
        }))
        .unwrap();
        assert_eq!(chunk.error_message().as_deref(), Some("upstream failed"));
    }

    #[test]
    fn tool_call_usage_recorded_only_when_not_streamed() {
        let usage: OpenRouterUsage = serde_json::from_value(json!({
            "prompt_tokens": 100,
            "completion_tokens": 50,
            "total_tokens": 150,
            "cost": 0.42
        }))
        .unwrap();

        // Non-streamed pure tool-call response: no streamed assistant event
        // carried this generation's usage, so its tokens must be recorded on the
        // tool-call event or they vanish from the breakdown.
        let recorded = tool_call_usage(false, Some(&usage)).expect("usage recorded");
        let counts = recorded.token_counts();
        assert_eq!(counts.input, Some(100));
        assert_eq!(counts.output, Some(50));

        // Streamed: the usage already landed on the finalized streamed assistant
        // event, so recording it again here would double-count.
        assert!(tool_call_usage(true, Some(&usage)).is_none());
    }

    /// End-to-end storage contract for a streamed reasoning-plus-tool-call turn,
    /// reproducing exactly what `run_http_turn` does on the streaming branch:
    /// the streamed reasoning lands as one finalized assistant event (sequence
    /// S), and the tool call lands as a SEPARATE assistant event carrying
    /// `tool_uses` at the next sequence (S+1). Both must survive and be returned
    /// by the same `list_events_for_session_delta` query the transcript reads, so
    /// the read renders alongside the reasoning — the OpenRouter transcript bug
    /// (CAIRN-1909) is reasoning surfacing while the tool call vanishes.
    #[tokio::test]
    async fn streamed_reasoning_then_tool_call_persists_both_events() {
        use crate::db::DbState;
        use crate::orchestrator::OrchestratorBuilder;
        use crate::services::testing::TestServicesBuilder;
        use crate::storage::{LocalDb, MigrationRunner, SearchIndex, TURSO_MIGRATIONS};
        use std::sync::Arc;
        use turso::params;

        let db_dir = tempfile::tempdir().unwrap();
        let db = LocalDb::open(db_dir.path().join("or-transcript-test.db"))
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
        let orch = OrchestratorBuilder::new(db_state, services, config_dir).build();

        let now = chrono::Utc::now().timestamp() as i32;
        orch.db
            .local
            .write(|conn| {
                Box::pin(async move {
                    conn.execute(
                        "INSERT INTO runs(id, status, session_id, created_at, updated_at)
                         VALUES ('run-1', 'live', 'session-1', ?1, ?2)",
                        params![now, now],
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
            .unwrap();

        // Sequence S: the streamed reasoning event, finalized exactly as the live
        // SSE reader does (open stream, append thinking deltas, finalize).
        let mut state = AssistantStreamState::open(&orch, "run-1", "session-1", None, 2).unwrap();
        state
            .append_thinking(&orch, "run-1", "Let me read the file.")
            .unwrap();
        state
            .finalize(
                &orch,
                "run-1",
                "session-1",
                String::new(),
                "Let me read the file.".to_string(),
                vec![],
                None,
                None,
                None,
            )
            .unwrap();

        // Sequence S+1: the tool-call assistant event (empty text, tool_uses set).
        let read_call = ToolCall {
            id: "call-read-1".to_string(),
            r#type: default_function_type(),
            function: ToolFunction {
                name: "read".to_string(),
                arguments: r#"{"paths":["file:src/lib.rs"]}"#.to_string(),
            },
        };
        store_assistant_tool_call(
            &orch,
            "run-1",
            "session-1",
            None,
            3,
            "",
            std::slice::from_ref(&read_call),
            None,
            None,
            None,
            None,
        )
        .unwrap();

        // Sequence S+2: the tool result.
        store_tool_result(
            &orch,
            "run-1",
            "session-1",
            None,
            4,
            "call-read-1",
            &DispatchOutput {
                content: "file body".to_string(),
                reminders: vec![],
            },
        )
        .unwrap();

        // Load through the exact query the transcript UI consumes.
        let delta = crate::runs::queries::list_events_for_session_delta(
            orch.db.local.clone(),
            "session-1",
            None,
        )
        .unwrap();
        let parsed: Vec<TranscriptEvent> = delta
            .events
            .iter()
            .map(|event| serde_json::from_str(&event.data).unwrap())
            .collect();

        // Reasoning surfaces (the part the user already sees).
        assert!(
            parsed.iter().any(|event| event.thinking.is_some()),
            "streamed reasoning event must persist"
        );
        // The tool call must ALSO surface, carrying tool_uses for the read.
        let tool_call = parsed.iter().find(|event| {
            event
                .tool_uses
                .as_ref()
                .map(|uses| uses.iter().any(|use_| use_.name == "read"))
                .unwrap_or(false)
        });
        let tool_call = tool_call.expect(
            "tool-call event with `tool_uses` for the read must be persisted and returned by the session delta",
        );
        let uses = tool_call.tool_uses.as_ref().unwrap();
        assert_eq!(uses.len(), 1);
        assert_eq!(uses[0].id, "call-read-1");
        assert_eq!(uses[0].input["paths"][0], "file:src/lib.rs");
        // And its result is matchable by tool_use_id for the renderer's pairing.
        assert!(
            parsed
                .iter()
                .any(|event| event.tool_use_id.as_deref() == Some("call-read-1")),
            "tool_result keyed to the read must persist"
        );
    }

    #[test]
    fn usage_parses_inline_cost() {
        let usage: OpenRouterUsage = serde_json::from_value(json!({
            "prompt_tokens": 100,
            "completion_tokens": 50,
            "total_tokens": 150,
            "reasoning_tokens": 20,
            "cost": 0.95,
            "cost_details": {"upstream_inference_cost": 0.40}
        }))
        .unwrap();
        assert_eq!(usage.cost, Some(0.95));
        assert_eq!(usage.token_counts().thinking, Some(20));
    }

    fn assistant_call(id: &str, name: &str) -> ChatMessage {
        ChatMessage {
            role: "assistant".to_string(),
            content: None,
            tool_call_id: None,
            tool_calls: Some(vec![ToolCall {
                id: id.to_string(),
                r#type: default_function_type(),
                function: ToolFunction {
                    name: name.to_string(),
                    arguments: "{}".to_string(),
                },
            }]),
            reasoning_details: None,
        }
    }

    #[test]
    fn fit_conversation_borrows_when_window_unknown_or_under_budget() {
        let messages = vec![
            ChatMessage::system("system".to_string()),
            ChatMessage::user("hello".to_string()),
        ];
        // Unknown window: pass the conversation through untouched.
        assert!(matches!(
            fit_conversation(&messages, None),
            std::borrow::Cow::Borrowed(_)
        ));
        // Known window with a tiny conversation: already under budget, untouched.
        assert!(matches!(
            fit_conversation(&messages, Some(200_000)),
            std::borrow::Cow::Borrowed(_)
        ));
    }

    #[test]
    fn trims_oldest_tool_outputs_first_and_protects_system_and_recent() {
        // 800 lines / 4000 chars (~1000 tokens) per tool output.
        let big = "line\n".repeat(800);
        let mut messages = vec![
            ChatMessage::system("system prompt".to_string()),
            ChatMessage::user("do the work".to_string()),
        ];
        // Six (assistant tool_call, tool result) pairs, read/run alternating, so
        // the conversation is 14 messages. PROTECT_RECENT_MESSAGES = 8 protects
        // indices 6..14, leaving only the tool results at index 3 and 5 eligible.
        for (id, name) in [
            ("a", "read"),
            ("b", "run"),
            ("c", "read"),
            ("d", "run"),
            ("e", "read"),
            ("f", "run"),
        ] {
            messages.push(assistant_call(id, name));
            messages.push(ChatMessage::tool(id.to_string(), big.clone()));
        }

        let budget = 4500;
        assert!(estimate_conversation_tokens(&messages) > budget);
        let trimmed = trim_conversation_to_budget(&messages, budget);

        // System and user turns are never collapsed.
        assert_eq!(trimmed[0].content.as_deref(), Some("system prompt"));
        assert_eq!(trimmed[1].content.as_deref(), Some("do the work"));
        // The oldest eligible tool results collapse to named, line-counted markers.
        assert_eq!(
            trimmed[3].content.as_deref(),
            Some("[read output elided — 800 lines]")
        );
        assert_eq!(
            trimmed[5].content.as_deref(),
            Some("[run output elided — 800 lines]")
        );
        // Assistant tool-call decisions stay intact (only `tool` messages collapse).
        assert!(trimmed[2].tool_calls.is_some());
        assert!(trimmed[2].content.is_none());
        // The most recent exchanges' tool outputs are protected in full.
        assert_eq!(trimmed[7].content.as_ref().unwrap().len(), big.len());
        assert_eq!(trimmed[13].content.as_ref().unwrap().len(), big.len());
        // The trimmed outgoing request now fits under budget.
        assert!(estimate_conversation_tokens(&trimmed) <= budget);
    }

    #[test]
    fn fit_conversation_returns_owned_trim_when_over_budget() {
        let big = "x".repeat(8000);
        let mut messages = vec![
            ChatMessage::system("s".to_string()),
            ChatMessage::user("u".to_string()),
        ];
        // Ten pairs (22 messages) so enough tool outputs are eligible to trim the
        // request back under budget rather than bottoming out on protected ones.
        for (id, name) in [
            ("a", "read"),
            ("b", "run"),
            ("c", "read"),
            ("d", "run"),
            ("e", "read"),
            ("f", "run"),
            ("g", "read"),
            ("h", "run"),
            ("i", "read"),
            ("j", "run"),
        ] {
            messages.push(assistant_call(id, name));
            messages.push(ChatMessage::tool(id.to_string(), big.clone()));
        }
        let window = 20_000;
        let fitted = fit_conversation(&messages, Some(window));
        assert!(matches!(fitted, std::borrow::Cow::Owned(_)));
        assert!(estimate_conversation_tokens(&fitted) <= context_fit_budget(window));
    }

    fn tool_call(name: &str, arguments: &str) -> ToolCall {
        ToolCall {
            id: "call-1".to_string(),
            r#type: default_function_type(),
            function: ToolFunction {
                name: name.to_string(),
                arguments: arguments.to_string(),
            },
        }
    }

    #[test]
    fn streaming_aggregate_captures_length_finish_reason() {
        // The output-token cutoff arrives as finish_reason "length" on the final
        // chunk; into_response must carry it so the turn loop can refuse a
        // partial side-effecting call.
        let mut aggregate = StreamingAggregate::default();
        let chunk: ChatStreamChunk = serde_json::from_value(json!({
            "choices": [{"delta": {"content": "partial"}, "finish_reason": "length"}]
        }))
        .unwrap();
        aggregate.apply_chunk(&chunk);
        let response = aggregate.into_response(false);
        assert_eq!(response.finish_reason.as_deref(), Some("length"));
    }

    #[test]
    fn prepare_tool_call_dispatches_valid_call() {
        let call = tool_call("run", r#"{"command": "ls"}"#);
        match prepare_tool_call(&call, false) {
            PreparedCall::Dispatch {
                verb,
                payload,
                repaired_args,
            } => {
                assert_eq!(verb, "run");
                assert_eq!(payload, json!({"command": "ls"}));
                assert!(!repaired_args);
            }
            PreparedCall::Reject(_) => panic!("valid call should dispatch"),
        }
    }

    #[test]
    fn prepare_tool_call_normalizes_name() {
        let call = tool_call("Write_File", r#"{"changes": []}"#);
        match prepare_tool_call(&call, false) {
            PreparedCall::Dispatch { verb, .. } => assert_eq!(verb, "write"),
            PreparedCall::Reject(_) => panic!("alias should normalize"),
        }
    }

    #[test]
    fn prepare_tool_call_rejects_unknown_name_without_crashing() {
        let call = tool_call("frobnicate", "{}");
        match prepare_tool_call(&call, false) {
            PreparedCall::Reject(output) => {
                assert!(output.content.contains("Unsupported OpenRouter tool"));
                assert!(output.content.contains("read, write, run"));
            }
            PreparedCall::Dispatch { .. } => panic!("unknown tool should reject"),
        }
    }

    #[test]
    fn prepare_tool_call_unrecoverable_args_reject_not_crash() {
        // The CAIRN-1784 failure shape: a large write whose JSON was truncated
        // mid-stream after a key/colon (unrecoverable). It must become a
        // tool-result error, never a propagated Err that crashes the run.
        let call = tool_call("write", r#"{"changes": [{"target": "file:x", "mode":"#);
        match prepare_tool_call(&call, false) {
            PreparedCall::Reject(output) => {
                assert!(output.content.contains("were not valid JSON"));
                assert!(output.content.contains("split it into smaller calls"));
            }
            PreparedCall::Dispatch { .. } => panic!("unrecoverable args should reject"),
        }
    }

    #[test]
    fn prepare_tool_call_rejects_truncation_balanced_write() {
        // Truncation mid-string in a write balances into valid JSON with
        // shortened content. Dispatching it would write a partial file, so it
        // must be refused and surfaced as a retry instead.
        let call = tool_call(
            "write",
            r#"{"changes": [{"target": "file:x", "content": "partial body"#,
        );
        match prepare_tool_call(&call, false) {
            PreparedCall::Reject(output) => {
                assert!(output.content.contains("appear truncated"));
                assert!(output
                    .content
                    .contains("split it into several smaller calls"));
            }
            PreparedCall::Dispatch { .. } => panic!("truncated write must not dispatch"),
        }
    }

    #[test]
    fn prepare_tool_call_rejects_truncation_balanced_run() {
        // A truncated shell command must not run partially (a half-formed `rm`).
        let call = tool_call("run", r#"{"command": "echo hello"#);
        assert!(matches!(
            prepare_tool_call(&call, false),
            PreparedCall::Reject(_)
        ));
    }

    #[test]
    fn prepare_tool_call_allows_truncation_balanced_read() {
        // read is non-destructive: a recovered partial is acceptable rather than
        // forcing a retry round-trip.
        let call = tool_call("read", r#"{"paths": ["file:a", "file:b"#);
        match prepare_tool_call(&call, false) {
            PreparedCall::Dispatch {
                verb,
                repaired_args,
                ..
            } => {
                assert_eq!(verb, "read");
                assert!(repaired_args);
            }
            PreparedCall::Reject(_) => panic!("truncated read may dispatch"),
        }
    }

    #[test]
    fn prepare_tool_call_rejects_length_capped_write_even_when_valid() {
        // finish_reason "length" => the generation was cut off. Even though this
        // write's JSON parses cleanly, the cutoff may have dropped later changes,
        // so a side-effecting call is refused.
        let call = tool_call(
            "write",
            r#"{"changes": [{"target": "file:x", "mode": "delete"}]}"#,
        );
        assert!(matches!(
            prepare_tool_call(&call, true),
            PreparedCall::Reject(_)
        ));
        // The same call dispatches when the generation finished normally.
        assert!(matches!(
            prepare_tool_call(&call, false),
            PreparedCall::Dispatch { .. }
        ));
    }

    #[test]
    fn prepare_tool_call_allows_length_capped_read() {
        // A length cutoff on a read is harmless: dispatch the (possibly partial)
        // read rather than forcing a retry.
        let call = tool_call("read", r#"{"paths": ["file:a"]}"#);
        assert!(matches!(
            prepare_tool_call(&call, true),
            PreparedCall::Dispatch { .. }
        ));
    }

    #[test]
    fn prepare_tool_call_unrecoverable_truncated_gives_split_guidance() {
        // Unrepairable JSON that was ALSO cut off at the output-token cap
        // (suspected_truncated) gets the truncation-specific message that tells
        // the model to split the call, not the generic invalid-JSON message.
        let call = tool_call("write", r#"{"changes": [{"target": "file:x", "mode":"#);
        match prepare_tool_call(&call, true) {
            PreparedCall::Reject(output) => {
                assert!(output.content.contains("appear truncated"));
                assert!(output
                    .content
                    .contains("split it into several smaller calls"));
            }
            PreparedCall::Dispatch { .. } => {
                panic!("unrecoverable + truncated args should reject")
            }
        }
    }
}
