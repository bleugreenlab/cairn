//! Provider-agnostic tool dispatch for the HTTP turn loop: classify and execute
//! model-emitted tool calls, normalize home-relative (`cairn:~/`) targets to
//! absolute node URIs, collapse batch envelopes to text on the text-only tool
//! edge, and render a dispatch result (with reminders) into tool-message text.
//! Repairs tool names and truncated JSON arguments, refuses partial
//! side-effecting calls, and dispatches through the Cairn MCP callback.

use super::repair;
use super::TurnToolCall;
use crate::backends::SessionConfig;
use crate::dispatch::DispatchOutput;
use crate::orchestrator::Orchestrator;
use crate::storage::run_db_blocking;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Mutex;

/// The outcome of classifying a model-emitted tool call: either a verb and
/// payload ready to dispatch, or a tool-result error to hand back to the model.
pub(super) enum PreparedCall {
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
pub(super) fn prepare_tool_call(
    tool_call: &TurnToolCall,
    suspected_truncated: bool,
) -> PreparedCall {
    let raw_name = tool_call.name.as_str();
    let Some(verb) = repair::normalize_tool_name(raw_name) else {
        return PreparedCall::Reject(DispatchOutput {
            content: format!(
                "Unsupported OpenRouter tool: {raw_name:?}. Valid tools are: read, write, run."
            ),
            reminders: Vec::new(),
        });
    };
    let side_effecting = matches!(verb, "write" | "run");

    let (payload, repaired_args) = match repair::parse_tool_arguments(&tool_call.arguments) {
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

pub(super) fn execute_tool_call(
    orch: &Orchestrator,
    config: &SessionConfig,
    tool_call: &TurnToolCall,
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
                    tool_call.arguments.len()
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
        thread_id: None,
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
pub(super) fn collapse_envelope_text(content: String) -> String {
    if let Ok(envelope) = serde_json::from_str::<cairn_common::read::RunBatchEnvelope>(&content) {
        return envelope.text;
    }
    if let Ok(envelope) = serde_json::from_str::<cairn_common::read::ReadBatchEnvelope>(&content) {
        return envelope.text;
    }
    content
}

pub(super) fn normalize_tool_payload(tool_name: &str, payload: &mut Value, home_uri: &str) {
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

pub(super) fn resolve_home_target(target: &str, home_uri: &str) -> String {
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

/// Render a dispatch result into tool-message text, appending any reminders as
/// `<system-reminder>` blocks after the content. Provider-neutral: both the
/// stored `tool_result` transcript event and the adapter's conversation tool
/// message render through this so the two stay byte-identical.
pub(in crate::backends) fn render_tool_result(output: DispatchOutput) -> String {
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
