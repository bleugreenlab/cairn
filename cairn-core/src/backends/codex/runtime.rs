use super::app_server::AppServerClient;
use super::auth::{refresh_codex_tokens_for_session, CodexAuthState};
use super::events::{
    build_codex_compaction_event, codex_rate_limit_snapshot_from_value,
    emit_codex_command_complete, emit_codex_command_output_delta, emit_streaming_delta,
    extract_app_server_delta, extract_command_execution, extract_mcp_result,
    extract_raw_response_message_text, finalize_agent_message, finalize_streaming,
    handle_agent_message_delta, handle_codex_interrupted_turn, handle_reasoning_delta,
    handle_turn_completed, store_event, summarize_command_result, summarize_file_change_result,
    terminal_tool_called_for_run,
};
use super::json_string;
use super::permissions::{
    decline_codex_native_file_change, handle_codex_approval_request,
    handle_codex_mcp_server_elicitation_request,
};
use super::protocol::StreamingState;
use super::CodexBackend;
use crate::agent_process::stream::{OutputTokensDetails, ToolUseInfo, TranscriptEvent, Usage};
use crate::models::ContextTokenState;
use crate::orchestrator::session::insert_error_event;
use crate::orchestrator::Orchestrator;
use crate::storage::LocalDb;
use crate::transcripts::stream_store::append_chunks;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use uuid::Uuid;

const CODEX_TURN_NO_PROGRESS_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const CODEX_TURN_WATCHDOG_POLL: Duration = Duration::from_secs(1);

fn u64_pointer_as_u32(value: &Value, pointer: &str) -> u32 {
    value.pointer(pointer).and_then(|v| v.as_u64()).unwrap_or(0) as u32
}

fn u64_pointer_as_i64(value: &Value, pointer: &str) -> i64 {
    value.pointer(pointer).and_then(|v| v.as_u64()).unwrap_or(0) as i64
}

/// Extract Codex's token notification into the per-turn DB usage columns and
/// normalized context state. Codex sends both `total` (cumulative across turns)
/// and `last` (current context occupancy). Cairn's context gauge must use
/// `last`; using `total` recreates the historical >100% context bug.
fn codex_context_tokens_from_notification(
    params: &Value,
    run_id: &str,
    session_id: Option<String>,
    backend: String,
    model: Option<String>,
    captured_at: i64,
) -> Option<(Usage, ContextTokenState)> {
    let last = params.pointer("/tokenUsage/last")?;
    let input_tokens = u64_pointer_as_u32(last, "/inputTokens");
    let cached_input_tokens = u64_pointer_as_u32(last, "/cachedInputTokens");
    let output_tokens = u64_pointer_as_u32(last, "/outputTokens");
    let reasoning_output_tokens = u64_pointer_as_i64(last, "/reasoningOutputTokens");
    let last_total_tokens = u64_pointer_as_i64(last, "/totalTokens");
    let context_window = params
        .pointer("/tokenUsage/modelContextWindow")
        .or_else(|| params.pointer("/modelContextWindow"))
        .and_then(|v| v.as_u64())
        .map(|v| v as i64);

    let usage = Usage {
        input_tokens,
        output_tokens,
        cache_creation_input_tokens: None,
        cache_read_input_tokens: Some(cached_input_tokens),
        output_tokens_details: Some(OutputTokensDetails {
            thinking_tokens: Some(reasoning_output_tokens as u32),
        }),
    };
    let state = ContextTokenState {
        run_id: run_id.to_string(),
        session_id,
        backend,
        model,
        used_tokens: last_total_tokens,
        context_window,
        // Codex only streams the effective modelContextWindow. Its separate
        // auto-compact threshold is not on the notification, so leave this for
        // a future Codex-protocol surface rather than guessing.
        auto_compact_limit: None,
        reasoning_tokens: Some(reasoning_output_tokens),
        last_output_tokens: Some(output_tokens as i64),
        captured_at,
    };

    Some((usage, state))
}

fn codex_tool_result_event(
    session_id: Option<String>,
    tool_use_id: Option<String>,
    tool_result: String,
    is_error: bool,
    raw: Option<Value>,
) -> TranscriptEvent {
    TranscriptEvent {
        event_type: "tool_result".to_string(),
        session_id,
        parent_tool_use_id: None,
        content: None,
        thinking: None,
        tool_name: None,
        tool_input: None,
        tool_uses: None,
        tool_use_id,
        tool_result: Some(tool_result),
        is_error,
        thinking_ms: None,
        raw,
    }
}

fn codex_compaction_event_from_completed_item(msg: &Value) -> Option<TranscriptEvent> {
    if msg.get("method").and_then(|v| v.as_str()) != Some("item/completed") {
        return None;
    }
    let item = msg.pointer("/params/item")?;
    if item.get("type").and_then(|v| v.as_str()) != Some("contextCompaction") {
        return None;
    }
    Some(build_codex_compaction_event(item.clone()))
}

#[derive(Debug)]
struct CodexTurnProgressWatchdog {
    active_turn_id: Option<String>,
    last_forward_progress_at: Instant,
    timeout: Duration,
    pending_tool_count: usize,
    fired: bool,
}

impl CodexTurnProgressWatchdog {
    fn new(timeout: Duration) -> Self {
        Self {
            active_turn_id: None,
            last_forward_progress_at: Instant::now(),
            timeout,
            pending_tool_count: 0,
            fired: false,
        }
    }

    fn start_turn(&mut self, turn_id: &str, now: Instant) {
        self.active_turn_id = Some(turn_id.to_string());
        self.last_forward_progress_at = now;
        self.pending_tool_count = 0;
        self.fired = false;
    }

    fn clear_turn(&mut self) {
        self.active_turn_id = None;
        self.pending_tool_count = 0;
        self.fired = false;
    }

    fn record_forward_progress(&mut self, now: Instant) {
        if self.active_turn_id.is_some() {
            self.last_forward_progress_at = now;
        }
    }

    fn set_pending_tool_count(&mut self, count: usize) {
        self.pending_tool_count = count;
    }

    fn expired(&mut self, now: Instant) -> Option<String> {
        if self.fired || self.pending_tool_count > 0 {
            return None;
        }
        let turn_id = self.active_turn_id.as_ref()?;
        if now.duration_since(self.last_forward_progress_at) < self.timeout {
            return None;
        }
        self.fired = true;
        Some(turn_id.clone())
    }
}

impl CodexBackend {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn reader_thread_app_server(
        orch: &Orchestrator,
        emitter: &Arc<dyn crate::services::EventEmitter>,
        run_id: &str,
        session_id: Option<String>,
        notifications: crossbeam_channel::Receiver<Value>,
        client: Arc<AppServerClient>,
        current_turn_id: Arc<Mutex<Option<String>>>,
        oauth_state: Option<Arc<Mutex<CodexAuthState>>>,
        initial_sequence: i32,
        backend_key: String,
        run_db: Arc<LocalDb>,
    ) {
        log::debug!("codex_app_server: reader started");
        let mut sequence: i32 = initial_sequence;
        let mut streaming_state: Option<StreamingState> = None;
        let mut pending_usage: Option<Usage> = None;
        let mut last_direct_assistant_text: Option<String> = None;
        let mut pending_tool_ids: HashSet<String> = HashSet::new();
        let mut tool_output_chars: HashMap<String, i32> = HashMap::new();
        let mut terminal_tool_suspended = false;
        let progress_watchdog = Arc::new(Mutex::new(CodexTurnProgressWatchdog::new(
            CODEX_TURN_NO_PROGRESS_TIMEOUT,
        )));
        if let Some(turn_id) = current_turn_id.lock().ok().and_then(|guard| guard.clone()) {
            if let Ok(mut watchdog) = progress_watchdog.lock() {
                watchdog.start_turn(&turn_id, Instant::now());
            }
        }
        let watchdog_alive = Arc::new(AtomicBool::new(true));
        {
            let watchdog = progress_watchdog.clone();
            let alive = watchdog_alive.clone();
            let orch = orch.clone();
            let run_id = run_id.to_string();
            let client = client.clone();
            thread::spawn(move || {
                while alive.load(Ordering::Acquire) {
                    thread::sleep(CODEX_TURN_WATCHDOG_POLL);
                    if !alive.load(Ordering::Acquire) {
                        break;
                    }
                    let Some(turn_id) = watchdog
                        .lock()
                        .ok()
                        .and_then(|mut guard| guard.expired(Instant::now()))
                    else {
                        continue;
                    };
                    log::error!(
                        "Codex turn {} for run {} made no forward progress for {:?}; aborting app-server stream",
                        turn_id,
                        &run_id[..run_id.len().min(8)],
                        CODEX_TURN_NO_PROGRESS_TIMEOUT
                    );
                    insert_error_event(
                        &orch,
                        &run_id,
                        None,
                        &format!(
                            "Codex turn made no forward progress for {} seconds; aborting so it can retry.",
                            CODEX_TURN_NO_PROGRESS_TIMEOUT.as_secs()
                        ),
                    );
                    client.shutdown();
                    let _ = crate::orchestrator::lifecycle::kill_session_with_reason(
                        &orch, &run_id, "crash",
                    );
                    break;
                }
            });
        }

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
            thinking_ms: None,
            raw: None,
        };
        store_event(
            orch,
            &run_db,
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
                                &run_db,
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
                            &run_db,
                            run_id,
                            "codex/file_read",
                            msg.get("params").cloned().unwrap_or(Value::Null),
                            client.as_ref(),
                            id_value,
                            false,
                        ),
                        Some("item/mcpToolCall/requestApproval") => handle_codex_approval_request(
                            orch,
                            &run_db,
                            run_id,
                            "codex/mcp_tool_call",
                            msg.get("params").cloned().unwrap_or(Value::Null),
                            client.as_ref(),
                            id_value,
                            false,
                        ),
                        Some("item/permissions/requestApproval") => handle_codex_approval_request(
                            orch,
                            &run_db,
                            run_id,
                            "codex/permissions",
                            msg.get("params").cloned().unwrap_or(Value::Null),
                            client.as_ref(),
                            id_value,
                            false,
                        ),
                        Some("mcpServer/elicitation/request") => {
                            handle_codex_mcp_server_elicitation_request(
                                orch,
                                &run_db,
                                run_id,
                                msg.get("params").cloned().unwrap_or(Value::Null),
                                client.as_ref(),
                                id_value,
                            )
                        }
                        Some("account/chatgptAuthTokens/refresh") => {
                            if let Some(state_arc) = oauth_state.as_ref() {
                                match refresh_codex_tokens_for_session(orch, state_arc) {
                                    Ok(new_tokens) => {
                                        if let Some(account_id) = new_tokens.chatgpt_account_id {
                                            client.respond(
                                                id_value,
                                                serde_json::json!({
                                                    "accessToken": new_tokens.access_token,
                                                    "chatgptAccountId": account_id,
                                                }),
                                            )
                                        } else {
                                            client.respond_error(
                                                id_value,
                                                -32000,
                                                "Codex token refresh did not provide a ChatGPT account id; run connect_codex_auth",
                                            )
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

            if terminal_tool_called_for_run(orch, run_id)
                && matches!(
                    method,
                    Some(
                        "item/agentMessage/delta"
                            | "item/reasoning/textDelta"
                            | "item/reasoning/summaryTextDelta"
                            | "rawResponseItem/completed"
                    )
                )
            {
                continue;
            }

            macro_rules! interrupt_terminal_tool_at_boundary {
                () => {
                    if !terminal_tool_suspended
                        && pending_tool_ids.is_empty()
                        && terminal_tool_called_for_run(orch, run_id)
                    {
                        terminal_tool_suspended = true;
                        log::info!(
                            "Terminal artifact/tool reached Codex tool boundary for run {}; interrupting and warming",
                            &run_id[..run_id.len().min(8)]
                        );
                        if let Err(error) =
                            crate::backends::stdin::send_interrupt(&orch.process_state, run_id)
                        {
                            log::warn!(
                                "Failed to interrupt Codex run {} after terminal artifact/tool boundary: {}",
                                &run_id[..run_id.len().min(8)],
                                error
                            );
                        }
                        crate::orchestrator::lifecycle::transition_to_warm_state(orch, run_id);
                        crate::backends::codex::events::emit_codex_run_turn_completed(emitter, run_id);
                    }
                };
            }

            match method {
                Some("turn/started") => {
                    if let Some(turn_id) = msg.pointer("/params/turn/id").and_then(|v| v.as_str()) {
                        if let Ok(mut guard) = current_turn_id.lock() {
                            *guard = Some(turn_id.to_string());
                        }
                        if let Ok(mut watchdog) = progress_watchdog.lock() {
                            watchdog.start_turn(turn_id, Instant::now());
                        }
                    }
                    pending_usage = None;
                }
                Some("item/agentMessage/delta") => {
                    if let Some(delta) = extract_app_server_delta(&msg) {
                        handle_agent_message_delta(
                            orch,
                            &run_db,
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
                            &run_db,
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
                                    &run_db,
                                    emitter,
                                    &mut streaming_state,
                                    session_id.as_deref(),
                                    &mut sequence,
                                );
                                let tool_use_id = json_string(msg.pointer("/params/item/id"))
                                    .unwrap_or_else(|| Uuid::new_v4().to_string());
                                pending_tool_ids.insert(tool_use_id.clone());
                                if let Ok(mut watchdog) = progress_watchdog.lock() {
                                    watchdog.record_forward_progress(Instant::now());
                                    watchdog.set_pending_tool_count(pending_tool_ids.len());
                                }
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
                                    thinking_ms: None,
                                    raw: None,
                                };
                                store_event(
                                    orch,
                                    &run_db,
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
                                    &run_db,
                                    emitter,
                                    &mut streaming_state,
                                    session_id.as_deref(),
                                    &mut sequence,
                                );
                                let tool_use_id = json_string(msg.pointer("/params/item/id"))
                                    .unwrap_or_else(|| Uuid::new_v4().to_string());
                                pending_tool_ids.insert(tool_use_id.clone());
                                if let Ok(mut watchdog) = progress_watchdog.lock() {
                                    watchdog.record_forward_progress(Instant::now());
                                    watchdog.set_pending_tool_count(pending_tool_ids.len());
                                }
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
                                    thinking_ms: None,
                                    raw: None,
                                };
                                store_event(
                                    orch,
                                    &run_db,
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
                                    &run_db,
                                    emitter,
                                    &mut streaming_state,
                                    session_id.as_deref(),
                                    &mut sequence,
                                );
                                let tool_use_id = json_string(msg.pointer("/params/item/id"))
                                    .unwrap_or_else(|| Uuid::new_v4().to_string());
                                pending_tool_ids.insert(tool_use_id.clone());
                                if let Ok(mut watchdog) = progress_watchdog.lock() {
                                    watchdog.record_forward_progress(Instant::now());
                                    watchdog.set_pending_tool_count(pending_tool_ids.len());
                                }
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
                                    thinking_ms: None,
                                    raw: None,
                                };
                                store_event(
                                    orch,
                                    &run_db,
                                    emitter,
                                    run_id,
                                    session_id.as_deref(),
                                    sequence,
                                    &event,
                                );
                                sequence += 1;
                            }
                            _ => {
                                log::debug!("Unhandled Codex item/started type: {}", item_type);
                            }
                        }
                    }
                }
                Some("item/completed") => {
                    if let Some(item_type) =
                        msg.pointer("/params/item/type").and_then(|v| v.as_str())
                    {
                        if let Ok(mut watchdog) = progress_watchdog.lock() {
                            watchdog.record_forward_progress(Instant::now());
                        }
                        match item_type {
                            "agentMessage" => {
                                if terminal_tool_called_for_run(orch, run_id) {
                                    continue;
                                }
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
                                        &run_db,
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
                                let tool_use_id = json_string(msg.pointer("/params/item/id"));
                                let (result_text, is_error) = summarize_command_result(
                                    msg.pointer("/params/item").unwrap_or(&Value::Null),
                                );
                                if let Some(id) = tool_use_id.as_deref() {
                                    let output_chars = tool_output_chars.remove(id);
                                    emit_codex_command_complete(emitter, run_id, id, output_chars);
                                }
                                let event = codex_tool_result_event(
                                    session_id.clone(),
                                    tool_use_id.clone(),
                                    result_text,
                                    is_error,
                                    None,
                                );
                                store_event(
                                    orch,
                                    &run_db,
                                    emitter,
                                    run_id,
                                    session_id.as_deref(),
                                    sequence,
                                    &event,
                                );
                                if let Some(id) = tool_use_id.as_deref() {
                                    pending_tool_ids.remove(id);
                                }
                                if let Ok(mut watchdog) = progress_watchdog.lock() {
                                    watchdog.set_pending_tool_count(pending_tool_ids.len());
                                }
                                interrupt_terminal_tool_at_boundary!();
                                sequence += 1;
                            }
                            "fileChange" => {
                                let tool_use_id = json_string(msg.pointer("/params/item/id"));
                                let (result_text, is_error) = summarize_file_change_result(
                                    msg.pointer("/params/item").unwrap_or(&Value::Null),
                                );
                                let event = codex_tool_result_event(
                                    session_id.clone(),
                                    tool_use_id.clone(),
                                    result_text,
                                    is_error,
                                    None,
                                );
                                store_event(
                                    orch,
                                    &run_db,
                                    emitter,
                                    run_id,
                                    session_id.as_deref(),
                                    sequence,
                                    &event,
                                );
                                if let Some(id) = tool_use_id.as_deref() {
                                    pending_tool_ids.remove(id);
                                }
                                if let Ok(mut watchdog) = progress_watchdog.lock() {
                                    watchdog.set_pending_tool_count(pending_tool_ids.len());
                                }
                                interrupt_terminal_tool_at_boundary!();
                                sequence += 1;
                            }
                            "mcpToolCall" => {
                                let tool_use_id = json_string(msg.pointer("/params/item/id"));
                                let result_value = msg.pointer("/params/item/result").cloned();
                                let (result_text, is_err, raw_value) =
                                    extract_mcp_result(&result_value);
                                let event = codex_tool_result_event(
                                    session_id.clone(),
                                    tool_use_id.clone(),
                                    result_text,
                                    is_err,
                                    raw_value,
                                );
                                store_event(
                                    orch,
                                    &run_db,
                                    emitter,
                                    run_id,
                                    session_id.as_deref(),
                                    sequence,
                                    &event,
                                );
                                if let Some(id) = tool_use_id.as_deref() {
                                    pending_tool_ids.remove(id);
                                }
                                if let Ok(mut watchdog) = progress_watchdog.lock() {
                                    watchdog.set_pending_tool_count(pending_tool_ids.len());
                                }
                                interrupt_terminal_tool_at_boundary!();
                                sequence += 1;
                            }
                            "contextCompaction" => {
                                if let Some(event) =
                                    codex_compaction_event_from_completed_item(&msg)
                                {
                                    store_event(
                                        orch,
                                        &run_db,
                                        emitter,
                                        run_id,
                                        session_id.as_deref(),
                                        sequence,
                                        &event,
                                    );
                                    sequence += 1;
                                }
                            }
                            _ => {
                                log::debug!("Unhandled Codex item/completed type: {}", item_type);
                            }
                        }
                    }
                }
                Some("rawResponseItem/completed") => {
                    if let Ok(mut watchdog) = progress_watchdog.lock() {
                        watchdog.record_forward_progress(Instant::now());
                    }
                    if let Some(text) = extract_raw_response_message_text(
                        msg.pointer("/params/item").unwrap_or(&Value::Null),
                    ) {
                        if let Some(ref mut state) = streaming_state {
                            // Codex streamed no content deltas but produced a final raw
                            // message: seed it once so the stream isn't empty.
                            if state.acc.content_is_empty() {
                                state.acc.push_content(&text);
                                match append_chunks(
                                    run_db.clone(),
                                    &state.stream_id,
                                    state.version,
                                    &state.acc.take_pending(),
                                ) {
                                    Ok(result) => {
                                        state.version = result.version;
                                        let d = state.acc.take_emit_delta();
                                        emit_streaming_delta(emitter, run_id, &state.stream_id, &d);
                                    }
                                    Err(error) => {
                                        log::warn!(
                                            "Failed to append Codex raw message for {}: {}",
                                            run_id,
                                            error
                                        );
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
                                thinking_ms: None,
                                raw: None,
                            };
                            store_event(
                                orch,
                                &run_db,
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
                        &run_db,
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
                    if let Ok(mut watchdog) = progress_watchdog.lock() {
                        watchdog.clear_turn();
                    }
                }
                Some("turn/aborted") => {
                    finalize_streaming(
                        orch,
                        &run_db,
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
                            handle_codex_interrupted_turn(orch, &run_db, emitter, run_id);
                        }
                        _ => {
                            insert_error_event(
                                orch,
                                run_id,
                                session_id.as_deref(),
                                &format!("Turn aborted: {}", reason),
                            );
                            // Not one of the recoverable aborts handled above
                            // (interrupted/replaced/review_ended): this is an
                            // unrecoverable failure, so finalize task-aware.
                            crate::orchestrator::lifecycle::fail_run(orch, run_id, "turn_aborted");
                        }
                    }
                    if let Ok(mut guard) = current_turn_id.lock() {
                        *guard = None;
                    }
                    if let Ok(mut watchdog) = progress_watchdog.lock() {
                        watchdog.clear_turn();
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
                            &run_db,
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
                        // Non-retryable fatal error: finalize task-aware so a
                        // delegated child fails terminally and resumes its parent.
                        crate::orchestrator::lifecycle::fail_run(orch, run_id, "turn_failed");
                    }
                }
                Some("thread/tokenUsage/updated") => {
                    if let Some(params) = msg.pointer("/params") {
                        let model = orch.process_state.get_model(run_id);
                        if let Some((usage, state)) = codex_context_tokens_from_notification(
                            params,
                            run_id,
                            session_id.clone(),
                            backend_key.clone(),
                            model,
                            chrono::Utc::now().timestamp(),
                        ) {
                            pending_usage = Some(usage);
                            orch.store_context_token_snapshot(state);
                        }
                    }
                }
                Some("account/rateLimits/updated") => {
                    if let Some(snapshot) = codex_rate_limit_snapshot_from_value(
                        msg.pointer("/params/rateLimits")
                            .cloned()
                            .unwrap_or(Value::Null),
                    ) {
                        orch.store_provider_usage_snapshot(snapshot);
                    }
                }
                Some("item/commandExecution/outputDelta") => {
                    let delta = msg
                        .pointer("/params/delta")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    let item_id = msg
                        .pointer("/params/itemId")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    let output_chars = if item_id.is_empty() {
                        0
                    } else {
                        let count = tool_output_chars.entry(item_id.to_string()).or_insert(0);
                        *count += delta.chars().count() as i32;
                        *count
                    };
                    emit_codex_command_output_delta(emitter, run_id, item_id, delta, output_chars);
                    if let Ok(mut watchdog) = progress_watchdog.lock() {
                        watchdog.record_forward_progress(Instant::now());
                    }
                }
                Some("item/fileChange/outputDelta") => {}
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

        watchdog_alive.store(false, Ordering::Release);
        finalize_streaming(
            orch,
            &run_db,
            emitter,
            &mut streaming_state,
            session_id.as_deref(),
            &mut sequence,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn notification(total_tokens: i64, last_tokens: i64) -> Value {
        json!({
            "tokenUsage": {
                "total": {
                    "totalTokens": total_tokens,
                    "inputTokens": 39_995,
                    "cachedInputTokens": 9_999,
                    "outputTokens": 5,
                    "reasoningOutputTokens": 0
                },
                "last": {
                    "totalTokens": last_tokens,
                    "inputTokens": 18_671,
                    "cachedInputTokens": 3_456,
                    "outputTokens": 5,
                    "reasoningOutputTokens": 0
                },
                "modelContextWindow": 258_400
            }
        })
    }

    #[test]
    fn context_compaction_event_only_comes_from_completed_item() {
        let started = json!({
            "method": "item/started",
            "params": {
                "item": {
                    "id": "compact-1",
                    "type": "contextCompaction",
                    "previousContextWindow": 200_000
                }
            }
        });
        let completed = json!({
            "method": "item/completed",
            "params": {
                "item": {
                    "id": "compact-1",
                    "type": "contextCompaction",
                    "previousContextWindow": 200_000
                }
            }
        });

        let events: Vec<_> = [&started, &completed]
            .into_iter()
            .filter_map(codex_compaction_event_from_completed_item)
            .collect();

        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(event.event_type, "system:compact_boundary");
        assert_eq!(event.content.as_deref(), Some("Context compacted"));
        assert_eq!(event.raw.as_ref().unwrap()["provider"], "codex");
        assert_eq!(
            event.raw.as_ref().unwrap()["compaction"],
            completed.pointer("/params/item").unwrap().clone()
        );
    }

    #[test]
    fn turn_progress_watchdog_expires_without_forward_progress() {
        let timeout = Duration::from_secs(10);
        let start = Instant::now();
        let mut watchdog = CodexTurnProgressWatchdog::new(timeout);
        watchdog.start_turn("turn-1", start);

        assert_eq!(
            watchdog.expired(start + timeout - Duration::from_millis(1)),
            None
        );
        assert_eq!(
            watchdog.expired(start + timeout),
            Some("turn-1".to_string())
        );
        assert_eq!(
            watchdog.expired(start + timeout + Duration::from_secs(1)),
            None
        );
    }

    #[test]
    fn turn_progress_watchdog_resets_on_progress_and_ignores_pending_tools() {
        let timeout = Duration::from_secs(10);
        let start = Instant::now();
        let mut watchdog = CodexTurnProgressWatchdog::new(timeout);
        watchdog.start_turn("turn-1", start);
        watchdog.record_forward_progress(start + Duration::from_secs(7));

        assert_eq!(watchdog.expired(start + Duration::from_secs(12)), None);
        watchdog.set_pending_tool_count(1);
        assert_eq!(watchdog.expired(start + Duration::from_secs(30)), None);
        watchdog.set_pending_tool_count(0);
        assert_eq!(
            watchdog.expired(start + Duration::from_secs(30)),
            Some("turn-1".to_string())
        );
    }

    #[test]
    fn turn_progress_watchdog_clear_turn_disarms() {
        let timeout = Duration::from_secs(10);
        let start = Instant::now();
        let mut watchdog = CodexTurnProgressWatchdog::new(timeout);
        watchdog.start_turn("turn-1", start);
        watchdog.clear_turn();

        assert_eq!(watchdog.expired(start + timeout), None);
    }

    #[test]
    fn codex_token_usage_maps_last_for_single_turn() {
        let (usage, state) = codex_context_tokens_from_notification(
            &notification(18_676, 18_676),
            "run-1",
            Some("session-1".to_string()),
            "codex".to_string(),
            Some("gpt-5".to_string()),
            123,
        )
        .expect("token usage");

        assert_eq!(usage.input_tokens, 18_671);
        assert_eq!(usage.cache_read_input_tokens, Some(3_456));
        assert_eq!(usage.cache_creation_input_tokens, None);
        assert_eq!(usage.output_tokens, 5);
        assert_eq!(state.used_tokens, 18_676);
        assert_eq!(state.context_window, Some(258_400));
        assert_eq!(state.reasoning_tokens, Some(0));
        assert_eq!(state.last_output_tokens, Some(5));
        assert_eq!(state.auto_compact_limit, None);
    }

    #[test]
    fn codex_token_usage_ignores_cumulative_total() {
        let (usage, state) = codex_context_tokens_from_notification(
            &notification(40_000, 18_676),
            "run-1",
            Some("session-1".to_string()),
            "codex".to_string(),
            Some("gpt-5".to_string()),
            123,
        )
        .expect("token usage");

        assert_eq!(state.used_tokens, 18_676);
        assert_eq!(usage.input_tokens, 18_671);
        assert_eq!(usage.cache_read_input_tokens, Some(3_456));
        assert_eq!(usage.output_tokens, 5);
    }
}
