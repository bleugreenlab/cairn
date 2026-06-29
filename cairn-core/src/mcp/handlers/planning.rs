//! Communication-related MCP handlers.
//!
//! Handles: ask_user

use crate::mcp::types::{AskUserPayload, McpCallbackRequest, Question};

use crate::execution::jobs::{continue_job_impl, ResumeContext};
use crate::models::{TurnStartReason, TurnState, TurnYieldReason};
use crate::orchestrator::Orchestrator;
use crate::storage::{DbError, DbResult, RowExt};
use cairn_common::ids;
use serde::Deserialize;
use serde_json::Value;
use turso::params;

use super::permission::{
    emit_successor_turn_events, ensure_and_start_successor_turn, get_issue_title, issue_id_for_run,
    recompute_issue_status_for_issue, yield_turn_for_host,
};
use super::{emit_attention, AttentionEvent};

const INLINE_PROMPT_WAIT_BUDGET: std::time::Duration = std::time::Duration::from_secs(45);

// ============================================================================
// Handlers
// ============================================================================

/// Ask the user one or more questions.
///
/// Stores a prompt addressable at `.../questions/{segment}`. With `background`
/// false (the default), yields the current turn and blocks until the user
/// responds, resuming on the prompt_responses broadcast. With `background`
/// true, inserts the prompt and returns its URI immediately without yielding;
/// the answer becomes observable later via the question read resolver.
pub async fn ask_questions(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    payload: AskUserPayload,
    background: bool,
    tool_use_id: Option<&str>,
) -> String {
    log::info!(
        "Prompting user for cwd={} with {} questions (background={})",
        request.cwd,
        payload.questions.len(),
        background
    );

    let services = &orch.services;

    // Resolve the run AND its owning database ONCE (CAIRN-2229, mirroring
    // CAIRN-2227's prompt/permission response routing): a team run's rows — the
    // prompt we insert, its yielded turn, the issue we recompute — live WHOLLY in
    // the synced replica. Hardcoding `orch.db.local` here was the live
    // `No run found with id '…~…'` bug: a team run is absent from the private DB,
    // so this first lookup failed before a prompt could ever be stored. Routing
    // is fail-closed (a closed replica errors, never a silent private read); a
    // bare (local) run resolves to the private DB byte-for-byte unchanged.
    let (ctx, owning_db) = match super::run_context::lookup_run_routed(&orch.db, request).await {
        Ok(pair) => pair,
        Err(e) => return e,
    };

    // Look up the run and store prompt in DB
    let (
        run_id,
        prompt_id,
        uri_segment,
        node_segment,
        project_key,
        issue_number,
        issue_title,
        node_name,
        exec_seq,
        current_turn_id,
        owns_turn_loop,
    ) = {
        // Owned-loop backends (OpenRouter) suspend the turn on a foreground
        // question instead of inline-waiting and entering AwaitingHost.
        let owns_turn_loop = !background && orch.process_state.run_owns_turn_loop(&ctx.run_id);

        let now = chrono::Utc::now().timestamp() as i32;
        let prompt_id = ids::mint_child(&ctx.run_id);
        let questions_json = serde_json::to_string(&payload.questions).unwrap_or_default();
        // Originating `write` tool_use id. Persisted so the slow-path (>45s)
        // resume can attach a synthetic tool_result to the same Question call
        // the fast path resolves inline.
        let request_tool_use_id = tool_use_id
            .map(str::to_string)
            .or_else(|| request.tool_use_id.clone());

        // Current turn ID for this run (used to yield/resume in the foreground path).
        let current_turn_id = orch.process_state.get_current_turn_id(&ctx.run_id);

        let insert_result = owning_db
            .write(|conn| {
                let prompt_id = prompt_id.clone();
                let run_id = ctx.run_id.clone();
                let questions_json = questions_json.clone();
                let current_turn_id = current_turn_id.clone();
                let issue_id = ctx.issue_id.clone();
                let request_tool_use_id = request_tool_use_id.clone();
                Box::pin(async move {
                    // Stable per-node ordinal: count this node's existing prompts.
                    let mut count_rows = conn
                        .query(
                            "SELECT COUNT(*) FROM prompts p JOIN runs r ON p.run_id = r.id \
                             WHERE r.job_id = (SELECT job_id FROM runs WHERE id = ?1)",
                            (run_id.as_str(),),
                        )
                        .await?;
                    let ordinal = count_rows
                        .next()
                        .await?
                        .and_then(|row| row.i64(0).ok())
                        .unwrap_or(0)
                        + 1;
                    let uri_segment = format!("q-{}", ordinal);

                    // Owning node job + segment: job_id scopes the prompt for the
                    // unique (job_id, uri_segment) index; node_segment builds the URI.
                    let mut node_rows = conn
                        .query(
                            "SELECT j.id, j.uri_segment FROM jobs j JOIN runs r ON r.job_id = j.id \
                             WHERE r.id = ?1",
                            (run_id.as_str(),),
                        )
                        .await?;
                    let (job_id, node_segment) = match node_rows.next().await? {
                        Some(row) => (row.opt_text(0)?, row.opt_text(1)?),
                        None => (None, None),
                    };

                    conn.execute(
                        "
                        INSERT INTO prompts (
                            id, run_id, job_id, questions, response,
                            created_at, answered_at, turn_id, uri_segment, tool_use_id
                        )
                        VALUES (?1, ?2, ?3, ?4, NULL, ?5, NULL, ?6, ?7, ?8)
                        ",
                        params![
                            prompt_id.as_str(),
                            run_id.as_str(),
                            job_id.as_deref(),
                            questions_json.as_str(),
                            now,
                            current_turn_id.as_deref(),
                            uri_segment.as_str(),
                            request_tool_use_id.as_deref()
                        ],
                    )
                    .await?;

                    // Background prompts never yield the parent turn.
                    let yielded_turn = if !background {
                        if let Some(ref turn_id) = current_turn_id {
                            match yield_turn_for_host(conn, turn_id, TurnYieldReason::UserInput)
                                .await
                            {
                                Ok(yielded) => yielded,
                                Err(e) => {
                                    log::warn!(
                                        "Failed to yield turn {} for prompt: {}",
                                        turn_id,
                                        e
                                    );
                                    false
                                }
                            }
                        } else {
                            false
                        }
                    } else {
                        false
                    };

                    if let Some(ref issue_id) = issue_id {
                        if let Err(e) =
                            crate::transitions::outcome::recompute_issue_status_conn(conn, issue_id)
                                .await
                        {
                            log::warn!("Failed to recompute issue status {}: {}", issue_id, e);
                        }
                    }

                    Ok((yielded_turn, uri_segment, node_segment))
                })
            })
            .await;

        let (yielded_turn, uri_segment, node_segment) = match insert_result {
            Ok(tuple) => tuple,
            Err(e) => {
                log::error!("Failed to insert prompt: {}", e);
                return format!("Failed to store prompt: {}", e);
            }
        };

        // Typed emit: the question is the actionable fact. Build the event
        // with the full question payload inline so the watch long-poll returns
        // it without a follow-up read.
        if let (Some(issue_id), Some(issue_number), Some(node)) = (
            ctx.issue_id.as_deref(),
            ctx.issue_number,
            node_segment.as_deref(),
        ) {
            let detail_uri = cairn_common::uri::build_node_question_uri(
                &ctx.project_key,
                issue_number,
                ctx.exec_seq.unwrap_or(1),
                node,
                &uri_segment,
            );
            if let Ok(issue_ctx) =
                crate::orchestrator::attention::read_issue_for_attention(&owning_db, issue_id).await
            {
                // Push the question to the issue's watchers (CAIRN-1887): a
                // `wake` + `event` push keyed `question:{issue}`, ref'd to the
                // prompt, excluding the asking node (it is self-suspended on its
                // own question). The legacy emit below still drives `cairn watch`
                // and the desktop toast.
                let issue_uri = issue_ctx.issue_uri();
                match crate::orchestrator::attention_delivery::push_to_issue_watchers(
                    &owning_db,
                    &issue_uri,
                    Some(ctx.job_id.as_str()),
                    &detail_uri,
                    crate::orchestrator::attention_push::Wake::Wake,
                    crate::orchestrator::attention_push::Boundary::Event,
                    &format!("question:{issue_uri}"),
                )
                .await
                {
                    // CAIRN-1889: actively resume each idle watcher so it wakes
                    // now instead of only noticing the question when something
                    // else happens to resume it. `nudge_job_for_urgency` is the
                    // shared resume-ladder primitive (idle -> resume; busy or
                    // self-suspended -> no-op).
                    Ok(recipients) => {
                        for recipient in &recipients {
                            if let Err(e) = crate::messages::delivery::nudge_job_for_urgency(
                                orch,
                                recipient,
                                crate::messages::queued::DeliveryUrgency::Steer,
                            ) {
                                log::warn!("question push wake failed: {}", e);
                            }
                        }
                    }
                    Err(e) => log::warn!("question push creation failed: {}", e),
                }
                orch.emit_attention_event(crate::orchestrator::AttentionEvent {
                    issue_id: issue_id.to_string(),
                    issue_uri: issue_ctx.issue_uri(),
                    fact: crate::orchestrator::AttentionFact::Question {
                        detail_uri,
                        content: crate::orchestrator::attention::QuestionContent {
                            questions: payload.questions.clone(),
                        },
                    },
                    attention: issue_ctx.attention,
                    status: issue_ctx.status,
                    updated_at: issue_ctx.updated_at,
                });
            }
        } else if let Some(issue_id) = ctx.issue_id.as_deref() {
            // Fallback for the rare path where URI components are missing.
            orch.wake_for_issue(issue_id).await;
        }

        // Yield the process for host interaction (foreground only). Owned-loop
        // backends skip AwaitingHost so `should_resume` stays true at answer
        // time; they suspend the turn from the owned loop instead.
        if !background && !owns_turn_loop {
            if let Some(ref turn_id) = current_turn_id {
                orch.process_state.yield_for_host(&ctx.run_id, turn_id);
            }
        }

        let issue_title = match ctx.issue_id.as_deref() {
            Some(issue_id) => get_issue_title(&owning_db, issue_id).await,
            None => None,
        };
        if yielded_turn {
            let _ = services.emitter.emit(
                "db-change",
                serde_json::json!({"table": "turns", "action": "update"}),
            );
        }

        (
            ctx.run_id,
            prompt_id,
            uri_segment,
            node_segment,
            ctx.project_key,
            ctx.issue_number,
            issue_title,
            ctx.job_name,
            ctx.exec_seq,
            current_turn_id,
            owns_turn_loop,
        )
    };

    // Emit events for frontend
    let _ = services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "prompts", "action": "insert"}),
    );
    let run_job_id = crate::messages::side_channel::job_id_for_run(&owning_db, &run_id).await;
    let _ = services.emitter.emit(
        "db-change",
        crate::notify::run_db_change_ids("update", &run_id, run_job_id.as_deref()),
    );

    if background {
        let question_uri = match (issue_number, node_segment.as_deref()) {
            (Some(number), Some(node)) => Some(cairn_common::uri::build_node_question_uri(
                &project_key,
                number,
                exec_seq.unwrap_or(1),
                node,
                &uri_segment,
            )),
            _ => None,
        };
        let _ = (&node_name, &current_turn_id);
        return match question_uri {
            Some(uri) => format!(
                "Question '{}' recorded; the answer will be available at {}",
                uri_segment, uri
            ),
            None => format!("Question '{}' recorded.", uri_segment),
        };
    }

    // Emit run-paused event for frontend to show prompt UI
    let _ = services
        .emitter
        .emit("run-paused", serde_json::json!(&run_id));

    // Emit attention event for toast notification
    emit_attention(
        &*services.emitter,
        &AttentionEvent {
            attention_type: "prompt",
            project_key: &project_key,
            issue_number,
            issue_title: issue_title.as_deref(),
            node_name: node_name.as_deref(),
            exec_seq,
            tool_name: None,
        },
    );

    // Owned-loop backends (OpenRouter) never inline-wait: there is no warm
    // process, so a resume cold-restarts the turn regardless. Suspend now and let
    // the answer drive a fresh resumed turn. The DB turn was already yielded above
    // so a successor can form; we deliberately did not enter AwaitingHost, keeping
    // `should_resume` true at answer time.
    if owns_turn_loop {
        orch.process_state
            .request_suspend(&run_id, crate::agent_process::process::SuspendKind::Prompt);
        return "Prompt suspended; the run will resume when the user answers.".to_string();
    }

    // Block waiting for user response (like the permission wait does)
    let mut rx = orch.prompt_responses.subscribe();
    let prompt_id_clone = prompt_id.clone();

    // Keep a short inline fast-path, then durably suspend the run before the
    // surrounding callback/session budget expires.
    let result = tokio::time::timeout(INLINE_PROMPT_WAIT_BUDGET, async {
        loop {
            match rx.recv().await {
                Ok((resp_prompt_id, response_text)) => {
                    if resp_prompt_id == prompt_id_clone {
                        return Ok(response_text);
                    }
                    // Not our prompt, keep waiting
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    return Err("Channel closed");
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                    // Missed some messages, keep going
                    continue;
                }
            }
        }
    })
    .await;

    match result {
        Ok(Ok(response)) => {
            // Create successor turn for the prompt response and transition run back to Running
            if let Some(ref pred_turn_id) = current_turn_id {
                match ensure_and_start_successor_turn(
                    &owning_db,
                    &run_id,
                    pred_turn_id,
                    TurnStartReason::PromptResponse,
                )
                .await
                {
                    Ok(Some(successor)) => {
                        emit_successor_turn_events(&*services.emitter, &successor);
                        orch.process_state
                            .set_current_turn_id(&run_id, Some(&successor.turn_id));
                    }
                    Ok(None) => {}
                    Err(e) => log::warn!("Failed to ensure successor turn: {}", e),
                }
            }

            // Run stays Live — no status change needed on prompt response.
            // The successor turn handles the semantic state change.

            match issue_id_for_run(&owning_db, &run_id).await {
                Ok(Some(issue_id)) => {
                    if let Err(e) = recompute_issue_status_for_issue(&owning_db, &issue_id).await {
                        log::warn!("Failed to recompute issue status {}: {}", issue_id, e);
                    }
                    // Answer recorded — attention likely cleared; wake `watch`.
                    orch.wake_for_issue(&issue_id).await;
                }
                Ok(None) => {}
                Err(e) => {
                    log::warn!("Failed to look up issue for run {}: {}", run_id, e);
                }
            }
            let run_job_id =
                crate::messages::side_channel::job_id_for_run(&owning_db, &run_id).await;
            let _ = services.emitter.emit(
                "db-change",
                crate::notify::run_db_change_ids("update", &run_id, run_job_id.as_deref()),
            );
            let _ = services.emitter.emit(
                "db-change",
                serde_json::json!({"table": "issues", "action": "update"}),
            );

            response
        }
        Ok(Err(msg)) => {
            log::warn!("Prompt wait channel ended for run {}: {}", run_id, msg);
            let _ = crate::orchestrator::lifecycle::suspend_run_for_durable_wait(
                orch,
                &run_id,
                "prompt_wait_suspended",
            );
            "Prompt suspended; resume will continue from the real response.".to_string()
        }
        Err(_) => {
            let _ = crate::orchestrator::lifecycle::suspend_run_for_durable_wait(
                orch,
                &run_id,
                "prompt_wait_suspended",
            );
            "Prompt suspended; resume will continue from the real response.".to_string()
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromptAnswerOutcome {
    pub duplicate: bool,
    pub response: String,
}

#[derive(Debug, Clone)]
struct PromptResume {
    run_id: String,
    session_id: Option<String>,
    predecessor_turn_id: Option<String>,
    successor_turn_id: Option<String>,
    job_id: Option<String>,
    issue_id: Option<String>,
    tool_use_id: Option<String>,
    duplicate: bool,
}

#[derive(Debug, Deserialize)]
struct PromptAnswerPayload {
    answer: Option<String>,
    answers: Option<Vec<PromptAnswerEntry>>,
}

/// An `answers[]` element. A bare string is accepted as shorthand for a
/// free-form answer at its array position, matching the leniency of the
/// single-question `answer` shorthand.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum PromptAnswerEntry {
    Text(String),
    Item(PromptAnswerItem),
}

impl PromptAnswerEntry {
    /// Resolve into a concrete item, using the array `position` as the index
    /// for the bare-string shorthand.
    fn into_item(self, position: usize) -> PromptAnswerItem {
        match self {
            PromptAnswerEntry::Text(text) => PromptAnswerItem {
                index: position,
                header: None,
                selection: None,
                selections: None,
                text: Some(text),
            },
            PromptAnswerEntry::Item(item) => item,
        }
    }
}

#[derive(Debug, Deserialize)]
struct PromptAnswerItem {
    index: usize,
    header: Option<String>,
    selection: Option<String>,
    selections: Option<Vec<String>>,
    text: Option<String>,
}

pub fn normalize_prompt_answer_payload(
    questions_json: &str,
    payload: &Value,
) -> Result<String, String> {
    let questions: Vec<Question> = serde_json::from_str(questions_json)
        .map_err(|e| format!("stored prompt questions are invalid: {e}"))?;
    if questions.is_empty() {
        return Err("stored prompt has no questions".to_string());
    }

    let payload: PromptAnswerPayload = serde_json::from_value(payload.clone())
        .map_err(|e| format!("invalid answer payload: {e}"))?;

    if let Some(answer) = payload
        .answer
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        if questions.len() != 1 {
            return Err("payload.answer shorthand is only valid for single-question prompts; use payload.answers with indices".to_string());
        }
        return Ok(answer.to_string());
    }

    let answers = payload
        .answers
        .ok_or_else(|| "payload.answer or payload.answers is required".to_string())?;
    if answers.is_empty() {
        return Err("payload.answers must contain at least one answer".to_string());
    }

    let mut seen = std::collections::HashSet::new();
    let mut rendered = Vec::new();
    for (position, entry) in answers.into_iter().enumerate() {
        let answer = entry.into_item(position);
        if answer.index >= questions.len() {
            return Err(format!(
                "answer index {} is out of range for {} question(s)",
                answer.index,
                questions.len()
            ));
        }
        if !seen.insert(answer.index) {
            return Err(format!("duplicate answer index {}", answer.index));
        }
        if let Some(header) = answer.header.as_deref() {
            if questions[answer.index].header.as_deref() != Some(header) {
                return Err(format!(
                    "answer index {} header mismatch: expected {:?}, got {:?}",
                    answer.index, questions[answer.index].header, header
                ));
            }
        }
        let value = render_answer_item(&questions[answer.index], &answer)?;
        rendered.push(format!("Question {}: {}", answer.index + 1, value));
    }

    Ok(rendered.join("\n"))
}

fn render_answer_item(question: &Question, answer: &PromptAnswerItem) -> Result<String, String> {
    let mut parts = Vec::new();
    if let Some(selection) = answer
        .selection
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        validate_selection(question, selection)?;
        parts.push(selection.to_string());
    }
    if let Some(selections) = &answer.selections {
        if selections.is_empty() {
            return Err(format!(
                "answer index {} selections must not be empty",
                answer.index
            ));
        }
        if !question.multi_select && selections.len() > 1 {
            return Err(format!(
                "answer index {} does not allow multiple selections",
                answer.index
            ));
        }
        for selection in selections {
            let selection = selection.trim();
            if selection.is_empty() {
                return Err(format!(
                    "answer index {} contains an empty selection",
                    answer.index
                ));
            }
            validate_selection(question, selection)?;
            parts.push(selection.to_string());
        }
    }
    if let Some(text) = answer
        .text
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        parts.push(text.to_string());
    }
    if parts.is_empty() {
        return Err(format!(
            "answer index {} requires selection, selections, or text",
            answer.index
        ));
    }
    Ok(parts.join(", "))
}

fn validate_selection(question: &Question, selection: &str) -> Result<(), String> {
    if question.options.is_empty()
        || question
            .options
            .iter()
            .any(|option| option.label == selection)
    {
        Ok(())
    } else {
        Err(format!(
            "invalid selection {:?}; expected one of: {}",
            selection,
            question
                .options
                .iter()
                .map(|option| option.label.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ))
    }
}

pub async fn answer_node_question(
    orch: &Orchestrator,
    project_key: &str,
    issue_number: i32,
    exec_seq: i32,
    node_segment: &str,
    prompt_segment: &str,
    payload: &Value,
) -> Result<PromptAnswerOutcome, String> {
    let (prompt_id, questions_json) = lookup_prompt_for_node_question(
        orch,
        project_key,
        issue_number,
        exec_seq,
        node_segment,
        prompt_segment,
    )
    .await?;
    let response = normalize_prompt_answer_payload(&questions_json, payload)?;
    answer_prompt_id(orch, &prompt_id, response).await
}

pub async fn validate_node_question_answer(
    orch: &Orchestrator,
    project_key: &str,
    issue_number: i32,
    exec_seq: i32,
    node_segment: &str,
    prompt_segment: &str,
    payload: &Value,
) -> Result<String, String> {
    let (_, questions_json) = lookup_prompt_for_node_question(
        orch,
        project_key,
        issue_number,
        exec_seq,
        node_segment,
        prompt_segment,
    )
    .await?;
    normalize_prompt_answer_payload(&questions_json, payload)
}

pub async fn answer_prompt_id(
    orch: &Orchestrator,
    prompt_id: &str,
    response: String,
) -> Result<PromptAnswerOutcome, String> {
    let now = chrono::Utc::now().timestamp();
    let prompt_id_owned = prompt_id.to_string();
    let response_for_db = response.clone();
    // Resolve the owning database ONCE (fail-closed, CAIRN-2227): a team
    // execution's `prompts` row — with the successor turn and issue-status
    // recompute `record_prompt_response_conn` performs in the same write — lives
    // wholly in the synced replica. Writing the private DB instead would no-op
    // against an absent row and the run would never resume. The resume mechanics
    // (store_tool_result_event_with_turn / continue_job_impl) self-route by
    // run/job; wake/emit side effects stay host-local.
    let owning_db = crate::execution::routing::owning_db_for_prompt(&orch.db, prompt_id)
        .await
        .map_err(|e| e.to_string())?;
    let resume = owning_db
        .write(|conn| {
            let prompt_id = prompt_id_owned.clone();
            let response = response_for_db.clone();
            Box::pin(
                async move { record_prompt_response_conn(conn, &prompt_id, &response, now).await },
            )
        })
        .await
        .map_err(|e| e.to_string())?;

    if !resume.duplicate {
        let _ = orch
            .prompt_responses
            .send((prompt_id_owned.clone(), response.clone()));
    }

    if let Some(ref issue_id) = resume.issue_id {
        // Answer recorded — attention likely cleared. Re-read and wake watch.
        orch.wake_for_issue(issue_id).await;
    }

    let should_resume = !resume.duplicate
        && resume.successor_turn_id.is_some()
        && !orch
            .process_state
            .is_awaiting_host(&resume.run_id, resume.predecessor_turn_id.as_deref());

    let emit_job_id = resume.job_id.clone();
    let job_id = if should_resume { resume.job_id } else { None };

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "prompts", "action": "update"}),
    );
    if resume.successor_turn_id.is_some() {
        let _ = orch.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "turns", "action": "update"}),
        );
    }
    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "issues", "action": "update"}),
    );
    let _ = orch.services.emitter.emit(
        "db-change",
        crate::notify::run_db_change_ids("update", &resume.run_id, emit_job_id.as_deref()),
    );

    if let Some(job_id) = job_id {
        let suppress_user_event = if let (Some(tool_use_id), Some(session_id)) =
            (resume.tool_use_id.as_deref(), resume.session_id.as_deref())
        {
            let now = chrono::Utc::now().timestamp() as i32;
            if let Err(e) = crate::execution::jobs::store_tool_result_event_with_turn(
                orch,
                &resume.run_id,
                session_id,
                tool_use_id,
                &response,
                false,
                now,
                resume.predecessor_turn_id.as_deref(),
            ) {
                log::warn!("Failed to store synthetic prompt tool_result: {}", e);
                false
            } else {
                true
            }
        } else {
            false
        };

        let prompt_resume = ResumeContext {
            suppress_user_event,
        };
        continue_job_impl(orch, &job_id, Some(&response), None, Some(prompt_resume))
            .map_err(|e| format!("Failed to resume prompt response: {}", e))?;
    }

    Ok(PromptAnswerOutcome {
        duplicate: resume.duplicate,
        response,
    })
}

async fn lookup_prompt_for_node_question(
    orch: &Orchestrator,
    project_key: &str,
    issue_number: i32,
    exec_seq: i32,
    node_segment: &str,
    prompt_segment: &str,
) -> Result<(String, String), String> {
    let project_key = project_key.to_string();
    let node_segment = node_segment.to_string();
    let prompt_segment = prompt_segment.to_string();
    // Route the node-URI answer path by project (CAIRN-2229): a team issue's
    // prompt row lives in the synced replica, so resolving the prompt id from its
    // node coordinates must read the replica, not the private DB — a private read
    // raises `question not found` and the answer never reaches the routed,
    // fail-closed `answer_prompt_id`. A local project resolves to the private DB.
    let db = orch.db.for_project(&project_key).await;
    db.read(|conn| {
            let project_key = project_key.clone();
            let node_segment = node_segment.clone();
            let prompt_segment = prompt_segment.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "
                        SELECT pr.id, pr.questions
                        FROM prompts pr
                        JOIN runs r ON pr.run_id = r.id
                        JOIN jobs j ON COALESCE(pr.job_id, r.job_id) = j.id
                        JOIN executions e ON j.execution_id = e.id
                        JOIN issues i ON j.issue_id = i.id
                        JOIN projects p ON i.project_id = p.id
                        WHERE p.key = ?1
                          AND i.number = ?2
                          AND e.seq = ?3
                          AND j.uri_segment = ?4
                          AND pr.uri_segment = ?5
                        LIMIT 1
                        ",
                        params![
                            project_key.as_str(),
                            issue_number as i64,
                            exec_seq as i64,
                            node_segment.as_str(),
                            prompt_segment.as_str()
                        ],
                    )
                    .await?;
                rows.next()
                    .await?
                    .map(|row| Ok::<_, DbError>((row.text(0)?, row.text(1)?)))
                    .transpose()?
                    .ok_or_else(|| {
                        DbError::internal(format!(
                            "question {prompt_segment} not found for {project_key}-{issue_number}/{exec_seq}/{node_segment}"
                        ))
                    })
            })
        })
        .await
        .map_err(|e| e.to_string())
}

async fn record_prompt_response_conn(
    conn: &turso::Connection,
    prompt_id: &str,
    response: &str,
    answered_at: i64,
) -> DbResult<PromptResume> {
    let mut rows = conn
        .query(
            "
            SELECT p.run_id, r.issue_id, p.turn_id, r.job_id, r.session_id,
                   p.tool_use_id,
                   CASE WHEN p.response IS NULL THEN 0 ELSE 1 END
            FROM prompts p
            JOIN runs r ON p.run_id = r.id
            WHERE p.id = ?1
            ",
            params![prompt_id],
        )
        .await?;
    let row = rows
        .next()
        .await?
        .ok_or_else(|| DbError::internal(format!("prompt not found: {prompt_id}")))?;

    let run_id = row.text(0)?;
    let issue_id = row.opt_text(1)?;
    let predecessor_turn_id = row.opt_text(2)?;
    let job_id = row.opt_text(3)?;
    let session_id = row.opt_text(4)?;
    let tool_use_id = row.opt_text(5)?;
    let already_answered = row.i64(6)? != 0;

    let duplicate = if already_answered {
        true
    } else {
        conn.execute(
            "
            UPDATE prompts
            SET response = ?1, answered_at = ?2
            WHERE id = ?3 AND response IS NULL
            ",
            params![response, answered_at, prompt_id],
        )
        .await?
            == 0
    };

    let successor_turn_id = if !duplicate {
        if let (Some(pred_turn_id), Some(job_id), Some(session_id)) = (
            predecessor_turn_id.as_deref(),
            job_id.as_deref(),
            session_id.as_deref(),
        ) {
            Some(ensure_prompt_successor_turn_conn(conn, session_id, job_id, pred_turn_id).await?)
        } else {
            None
        }
    } else {
        None
    };

    if let Some(issue_id) = issue_id.as_deref() {
        crate::transitions::outcome::recompute_issue_status_conn(conn, issue_id).await?;
    }

    Ok(PromptResume {
        run_id,
        session_id,
        predecessor_turn_id,
        successor_turn_id,
        job_id,
        issue_id,
        tool_use_id,
        duplicate,
    })
}

async fn ensure_prompt_successor_turn_conn(
    conn: &turso::Connection,
    session_id: &str,
    job_id: &str,
    predecessor_id: &str,
) -> DbResult<String> {
    if let Some(existing) = get_successor_turn_id_conn(conn, predecessor_id).await? {
        return Ok(existing);
    }

    let mut pred_rows = conn
        .query(
            "SELECT state FROM turns WHERE id = ?1",
            params![predecessor_id],
        )
        .await?;
    let pred_row = pred_rows.next().await?.ok_or_else(|| {
        DbError::internal(format!("predecessor turn not found: {predecessor_id}"))
    })?;
    let pred_state: TurnState = pred_row
        .text(0)?
        .parse()
        .map_err(|e| DbError::internal(format!("invalid predecessor state: {e}")))?;
    if !pred_state.is_terminal() {
        return Err(DbError::internal(format!(
            "predecessor turn {predecessor_id} is in non-terminal state {pred_state:?}"
        )));
    }

    let mut sequence_rows = conn
        .query(
            "SELECT COALESCE(MAX(sequence), 0) + 1 FROM turns WHERE session_id = ?1",
            params![session_id],
        )
        .await?;
    let sequence = sequence_rows
        .next()
        .await?
        .ok_or_else(|| DbError::internal("missing next turn sequence"))?
        .i64(0)?;

    let now = chrono::Utc::now().timestamp();
    let turn_id = ids::mint_child(job_id);
    let state = TurnState::Pending.to_string();
    let start_reason = TurnStartReason::PromptResponse.to_string();

    conn.execute(
        "
        INSERT INTO turns (
            id, session_id, run_id, job_id, sequence, predecessor_id,
            state, yield_reason, start_reason, created_at, started_at, ended_at, updated_at
        )
        VALUES (?1, ?2, NULL, ?3, ?4, ?5, ?6, NULL, ?7, ?8, NULL, NULL, ?9)
        ",
        params![
            turn_id.as_str(),
            session_id,
            job_id,
            sequence,
            predecessor_id,
            state.as_str(),
            start_reason.as_str(),
            now,
            now
        ],
    )
    .await?;

    conn.execute(
        "UPDATE jobs SET current_turn_id = ?1, updated_at = ?2 WHERE id = ?3",
        params![turn_id.as_str(), now, job_id],
    )
    .await?;

    Ok(turn_id)
}

async fn get_successor_turn_id_conn(
    conn: &turso::Connection,
    predecessor_id: &str,
) -> DbResult<Option<String>> {
    let mut rows = conn
        .query(
            "
            SELECT id
            FROM turns
            WHERE predecessor_id = ?1
            ORDER BY sequence ASC
            LIMIT 1
            ",
            params![predecessor_id],
        )
        .await?;
    crate::storage::next_text(&mut rows, 0).await
}

#[cfg(test)]
mod prompt_answer_tests {
    use super::*;
    use crate::mcp::types::QuestionOption;
    use serde_json::json;

    fn option(label: &str) -> QuestionOption {
        QuestionOption {
            label: label.to_string(),
            description: String::new(),
        }
    }

    fn questions_json() -> String {
        serde_json::to_string(&vec![
            Question {
                question: "Continue?".to_string(),
                header: Some("Confirm".to_string()),
                options: vec![option("Yes"), option("No")],
                multi_select: false,
            },
            Question {
                question: "Pick flags".to_string(),
                header: None,
                options: vec![option("A"), option("B")],
                multi_select: true,
            },
        ])
        .unwrap()
    }

    #[test]
    fn normalizes_single_question_shorthand() {
        let questions = serde_json::to_string(&vec![Question {
            question: "Continue?".to_string(),
            header: None,
            options: vec![],
            multi_select: false,
        }])
        .unwrap();
        assert_eq!(
            normalize_prompt_answer_payload(&questions, &json!({"answer":"Yes"})).unwrap(),
            "Yes"
        );
    }

    #[test]
    fn normalizes_multi_question_answers() {
        assert_eq!(
            normalize_prompt_answer_payload(
                &questions_json(),
                &json!({"answers":[{"index":0,"selection":"Yes"},{"index":1,"text":"custom"}]})
            )
            .unwrap(),
            "Question 1: Yes\nQuestion 2: custom"
        );
    }

    #[test]
    fn normalizes_multi_select_answers() {
        assert_eq!(
            normalize_prompt_answer_payload(
                &questions_json(),
                &json!({"answers":[{"index":1,"selections":["A","B"]}]})
            )
            .unwrap(),
            "Question 2: A, B"
        );
    }

    #[test]
    fn normalizes_free_text_only_answer() {
        assert_eq!(
            normalize_prompt_answer_payload(
                &questions_json(),
                &json!({"answers":[{"index":0,"text":"Something else"}]})
            )
            .unwrap(),
            "Question 1: Something else"
        );
    }

    #[test]
    fn normalizes_bare_string_shorthand_items() {
        assert_eq!(
            normalize_prompt_answer_payload(
                &questions_json(),
                &json!({"answers":[{"index":0,"selection":"Yes"},"free text for q2"]})
            )
            .unwrap(),
            "Question 1: Yes\nQuestion 2: free text for q2"
        );
    }

    #[test]
    fn rejects_invalid_selection() {
        let error = normalize_prompt_answer_payload(
            &questions_json(),
            &json!({"answers":[{"index":1,"selection":"C"}]}),
        )
        .unwrap_err();
        assert!(error.contains("invalid selection"));
    }

    #[test]
    fn rejects_invalid_index() {
        let error = normalize_prompt_answer_payload(
            &questions_json(),
            &json!({"answers":[{"index":2,"text":"oops"}]}),
        )
        .unwrap_err();
        assert!(error.contains("out of range"));
    }
}
