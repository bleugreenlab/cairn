//! Communication-related MCP handlers.
//!
//! Handles: ask_user

use crate::agent_process::process::GoalPauseOwner;
use crate::execution::jobs::GoalReactivation;
use crate::mcp::types::{AskUserPayload, McpCallbackRequest, Question};
use crate::models::question::{PromptAnswers, QuestionAnswer};

use crate::execution::jobs::{continue_job_impl, ResumeContext};
use crate::models::{TurnStartReason, TurnState, TurnYieldReason};
use crate::orchestrator::Orchestrator;
use crate::storage::{DbError, DbResult, RowExt};
use cairn_common::ids;
use cairn_db::turso::params;
use serde::Deserialize;
use serde_json::Value;

use super::permission::{
    emit_successor_turn_events, ensure_and_start_successor_turn, issue_id_for_run,
    recompute_issue_status_for_issue, yield_turn_for_host, WithdrawalCause,
};

/// What the agent reads for a question that outlived the inline budget.
///
/// This is the whole agent-visible surface of a durably-suspended question, so
/// it says what actually happened -- the call is continuing -- and never implies
/// anyone declined it. `super::suspension_markers` pins that property.
pub(crate) const PROMPT_SUSPENDED_MARKER: &str =
    "Prompt suspended; resume will continue from the real response.";

/// What an owned-loop backend reads for the same suspension.
pub(crate) const PROMPT_SUSPENDED_OWNED_LOOP_MARKER: &str =
    "Prompt suspended; the run will resume when the user answers.";

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
) -> QuestionsOutcome {
    ask_questions_owned(orch, request, payload, background, tool_use_id, None).await
}

/// What asking returns to the asking call: the text the agent reads and, when
/// the answer arrived inline, the answers keyed by question index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuestionsOutcome {
    pub text: String,
    pub answers: Option<Vec<QuestionAnswer>>,
}

impl From<String> for QuestionsOutcome {
    fn from(text: String) -> Self {
        Self {
            text,
            answers: None,
        }
    }
}

/// A recorded answer as broadcast to an inline waiter: the answers keyed by
/// question index and their one rendering for the agent to read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnsweredPrompt {
    pub answers: PromptAnswers,
    pub rendered: String,
}

impl From<AnsweredPrompt> for QuestionsOutcome {
    fn from(answered: AnsweredPrompt) -> Self {
        Self {
            text: answered.rendered,
            answers: Some(answered.answers.into_answers()),
        }
    }
}

/// Create the ordinary Cairn question surface for input requested by an
/// external MCP call. The durable MCP wait owns the answer, so this path never
/// binds a provider tool use and never yields or resumes the asking agent turn.
pub(crate) async fn ask_mcp_questions(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    payload: AskUserPayload,
    mcp_wait_id: &str,
) -> QuestionsOutcome {
    ask_questions_owned(orch, request, payload, true, None, Some(mcp_wait_id)).await
}

async fn ask_questions_owned(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    payload: AskUserPayload,
    background: bool,
    tool_use_id: Option<&str>,
    mcp_wait_id: Option<&str>,
) -> QuestionsOutcome {
    log::info!(
        "Prompting user for cwd={} with {} questions (background={})",
        request.cwd,
        payload.questions.len(),
        background
    );
    // Every answer is keyed by its question's index, so a prompt with no
    // questions could never be answered.
    if payload.questions.is_empty() {
        return "A questions append needs at least one question in payload.questions"
            .to_string()
            .into();
    }

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
        Err(e) => return e.into(),
    };

    // Look up the run and store prompt in DB
    let (
        run_id,
        prompt_id,
        uri_segment,
        node_segment,
        project_key,
        issue_number,
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
        let request_tool_use_id = if mcp_wait_id.is_some() {
            None
        } else {
            tool_use_id
                .map(str::to_string)
                .or_else(|| request.tool_use_id.clone())
        };
        let mcp_wait_id = mcp_wait_id.map(str::to_string);

        // An MCP-owned question belongs to its durable wait, not to the agent's
        // current turn. Leaving turn_id null is what prevents an independent
        // prompt successor even if answer handling regresses.
        let current_turn_id = if mcp_wait_id.is_some() {
            None
        } else {
            orch.process_state.get_current_turn_id(&ctx.run_id)
        };

        let insert_result = owning_db
            .write(|conn| {
                let prompt_id = prompt_id.clone();
                let run_id = ctx.run_id.clone();
                let questions_json = questions_json.clone();
                let current_turn_id = current_turn_id.clone();
                let issue_id = ctx.issue_id.clone();
                let request_tool_use_id = request_tool_use_id.clone();
                let mcp_wait_id = mcp_wait_id.clone();
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

                    let inserted = conn
                        .execute(
                            "
                        INSERT INTO prompts (
                            id, run_id, job_id, questions, response,
                            created_at, answered_at, turn_id, uri_segment, tool_use_id,
                            mcp_wait_id
                        )
                        SELECT ?1, ?2, ?3, ?4, NULL, ?5, NULL, ?6, ?7, ?8, ?9
                        WHERE ?9 IS NULL OR EXISTS (
                            SELECT 1 FROM agent_waits
                            WHERE id = ?9 AND state IN ('pending', 'resolving')
                        )
                        ",
                            params![
                                prompt_id.as_str(),
                                run_id.as_str(),
                                job_id.as_deref(),
                                questions_json.as_str(),
                                now,
                                current_turn_id.as_deref(),
                                uri_segment.as_str(),
                                request_tool_use_id.as_deref(),
                                mcp_wait_id.as_deref()
                            ],
                        )
                        .await?;
                    if mcp_wait_id.is_some() && inserted == 0 {
                        return Err(DbError::internal("MCP continuation wait is not active"));
                    }

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

                    if let Some(ref job_id) = job_id {
                        if let Err(e) =
                            crate::transitions::outcome::recompute_job_owner_attention_conn(
                                conn,
                                job_id,
                                issue_id.as_deref(),
                            )
                            .await
                        {
                            log::warn!("Failed to recompute owner attention for {job_id}: {e}");
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
                return format!("Failed to store prompt: {}", e).into();
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
                    &format!("question:{detail_uri}"),
                )
                .await
                {
                    // CAIRN-1889: actively resume each idle watcher so it wakes
                    // now instead of only noticing the question when something
                    // else happens to resume it. `nudge_job_for_urgency` is the
                    // shared resume-ladder primitive (idle -> resume; busy or
                    // self-suspended -> no-op).
                    Ok(recipients) => {
                        orch.notifier.emit_change("attention_pushes");
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
                    route_provenance: None,
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

        if yielded_turn {
            if let Some(turn_id) = current_turn_id.as_deref() {
                let change =
                    crate::notify::turn_db_change_for_id(&owning_db, turn_id, "update").await;
                let _ = services.emitter.emit("db-change", change);
            }
        }

        (
            ctx.run_id,
            prompt_id,
            uri_segment,
            node_segment,
            ctx.project_key,
            ctx.issue_number,
            ctx.exec_seq,
            current_turn_id,
            owns_turn_loop,
        )
    };

    // Emit events for frontend
    let prompt_change =
        crate::notify::prompt_db_change_for_id(&owning_db, &prompt_id, "insert").await;
    let _ = services.emitter.emit("db-change", prompt_change);
    let run_change = crate::notify::run_db_change_for_id(&owning_db, &run_id, "update").await;
    let _ = services.emitter.emit("db-change", run_change);

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
        let _ = &current_turn_id;
        return match question_uri {
            Some(uri) => format!(
                "Question '{}' recorded; the answer will be available at {}",
                uri_segment, uri
            ),
            None => format!("Question '{}' recorded.", uri_segment),
        }
        .into();
    }

    // Emit run-paused event for frontend to show prompt UI
    let _ = services
        .emitter
        .emit("run-paused", serde_json::json!(&run_id));

    // Owned-loop backends (OpenRouter) never inline-wait: there is no warm
    // process, so a resume cold-restarts the turn regardless. Suspend now and let
    // the answer drive a fresh resumed turn. The DB turn was already yielded above
    // so a successor can form; we deliberately did not enter AwaitingHost, keeping
    // `should_resume` true at answer time.
    if owns_turn_loop {
        orch.process_state
            .request_suspend(&run_id, cairn_models::turn::SuspendKind::Prompt);
        return PROMPT_SUSPENDED_OWNED_LOOP_MARKER.to_string().into();
    }

    // Block waiting for user response (like the permission wait does)
    let mut rx = orch.prompt_responses.subscribe();
    let prompt_id_clone = prompt_id.clone();

    // Keep a short inline fast-path, then durably suspend the run before the
    // surrounding callback/session budget expires.
    let result = tokio::time::timeout(
        cairn_common::callback_contract::INLINE_DURABLE_WAIT_BUDGET,
        async {
            loop {
                match rx.recv().await {
                    Ok((resp_prompt_id, answered)) => {
                        if resp_prompt_id == prompt_id_clone {
                            return Ok(answered);
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
        },
    )
    .await;

    match result {
        Ok(Ok(answered)) => {
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
                        emit_successor_turn_events(&owning_db, &*services.emitter, &successor)
                            .await;
                        orch.process_state
                            .set_current_turn_id(&run_id, Some(&successor.turn_id));
                    }
                    Ok(None) => {}
                    Err(e) => log::warn!("Failed to ensure successor turn: {}", e),
                }
            }

            // Run stays Live — no status change needed on prompt response.
            // The successor turn handles the semantic state change.

            let changed_issue = match issue_id_for_run(&owning_db, &run_id).await {
                Ok(Some(issue_id)) => {
                    if let Err(e) = recompute_issue_status_for_issue(&owning_db, &issue_id).await {
                        log::warn!("Failed to recompute issue status {}: {}", issue_id, e);
                    }
                    // Answer recorded — attention likely cleared; wake `watch`.
                    orch.wake_for_issue(&issue_id).await;
                    crate::issues::crud::get(&owning_db, &issue_id)
                        .await
                        .ok()
                        .flatten()
                }
                Ok(None) => None,
                Err(e) => {
                    log::warn!("Failed to look up issue for run {}: {}", run_id, e);
                    None
                }
            };
            let run_change =
                crate::notify::run_db_change_for_id(&owning_db, &run_id, "update").await;
            let _ = services.emitter.emit("db-change", run_change);
            if let Some(issue) = changed_issue.as_ref() {
                let _ = services
                    .emitter
                    .emit("db-change", crate::notify::issue_db_change(issue, "update"));
            }

            answered.into()
        }
        Ok(Err(msg)) => {
            log::warn!("Prompt wait channel ended for run {}: {}", run_id, msg);
            crate::orchestrator::lifecycle::suspend_run_for_durable_wait_after_handoff(
                orch,
                &run_id,
                "prompt_wait_suspended",
                GoalPauseOwner::UserInput {
                    request_id: prompt_id.clone(),
                },
            );
            PROMPT_SUSPENDED_MARKER.to_string().into()
        }
        Err(_) => {
            crate::orchestrator::lifecycle::suspend_run_for_durable_wait_after_handoff(
                orch,
                &run_id,
                "prompt_wait_suspended",
                GoalPauseOwner::UserInput {
                    request_id: prompt_id.clone(),
                },
            );
            PROMPT_SUSPENDED_MARKER.to_string().into()
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromptAnswerOutcome {
    pub duplicate: bool,
    pub response: String,
    /// The question had been withdrawn when this answer reached it: what the
    /// answer surface shows instead of an acknowledgement. Nothing was
    /// recorded, replayed, or resumed.
    pub withdrawn: Option<String>,
}

impl PromptAnswerOutcome {
    fn withdrawn(cause: Option<WithdrawalCause>) -> Self {
        Self {
            duplicate: false,
            response: String::new(),
            withdrawn: Some(withdrawn_question_notice(cause)),
        }
    }
}

/// What every answer surface says to an answer that arrived after its question
/// was withdrawn. Mirrors the permission wording.
pub fn withdrawn_question_notice(cause: Option<WithdrawalCause>) -> String {
    let cause = cause
        .map(WithdrawalCause::sentence)
        .unwrap_or("its recorded cause could not be read");
    format!(
        "This question was withdrawn before it was answered: {cause}. Your answer was not \
         recorded, nothing was replayed, and no turn was started."
    )
}

/// Withdraw a pending question: it stops being pending everywhere, and a later
/// answer is refused with `cause` rather than recorded. Reports whether this
/// call withdrew it (an answered or already-withdrawn row is left alone).
pub(crate) async fn withdraw_prompt_conn(
    conn: &cairn_db::turso::Connection,
    prompt_id: &str,
    cause: WithdrawalCause,
    now: i64,
) -> DbResult<bool> {
    let cause = withdrawal_cause_text(cause)?;
    let changed = conn
        .execute(
            "UPDATE prompts SET withdrawn_at = ?2, withdrawn_cause = ?3
              WHERE id = ?1 AND response IS NULL AND withdrawn_at IS NULL",
            params![prompt_id, now, cause],
        )
        .await?;
    Ok(changed > 0)
}

/// Withdraw every pending question raised on one of `job_id`'s turns. A
/// question owned by an MCP wait has no turn and is left to that wait.
pub(crate) async fn withdraw_job_prompts_conn(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
    cause: WithdrawalCause,
    now: i64,
) -> DbResult<u64> {
    let cause = withdrawal_cause_text(cause)?;
    Ok(conn
        .execute(
            "UPDATE prompts SET withdrawn_at = ?2, withdrawn_cause = ?3
              WHERE response IS NULL AND withdrawn_at IS NULL
                AND turn_id IN (SELECT id FROM turns WHERE job_id = ?1)",
            params![job_id, now, cause],
        )
        .await?)
}

fn withdrawal_cause_text(cause: WithdrawalCause) -> DbResult<String> {
    serde_json::to_value(cause)
        .ok()
        .and_then(|value| value.as_str().map(str::to_string))
        .ok_or_else(|| DbError::internal("withdrawal cause did not serialize to a string"))
}

/// The withdrawal recorded on a question: `None` when it is not withdrawn,
/// `Some(None)` when it is but the cause cannot be read.
async fn prompt_withdrawal_conn(
    conn: &cairn_db::turso::Connection,
    prompt_id: &str,
) -> DbResult<Option<Option<WithdrawalCause>>> {
    let mut rows = conn
        .query(
            "SELECT withdrawn_cause FROM prompts WHERE id = ?1 AND withdrawn_at IS NOT NULL",
            params![prompt_id],
        )
        .await?;
    let Some(row) = rows.next().await? else {
        return Ok(None);
    };
    let cause = row
        .opt_text(0)?
        .and_then(|cause| serde_json::from_value(serde_json::Value::String(cause)).ok());
    Ok(Some(cause))
}

/// What recording an answer did.
enum PromptRecord {
    Recorded(PromptResume),
    /// The question was withdrawn first; nothing was written.
    Withdrawn(Option<WithdrawalCause>),
}

#[derive(Debug, Clone)]
struct PromptResume {
    run_id: String,
    mcp_wait_id: Option<String>,
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

fn parse_prompt_questions(questions_json: &str) -> Result<Vec<Question>, String> {
    serde_json::from_str(questions_json)
        .map_err(|e| format!("stored prompt questions are invalid: {e}"))
}

/// Normalize an answer payload into one answer per question. `answers[]` items
/// are validated against their question's options; the `answer` shorthand is
/// typed text, parsed against the prompt's question count. Answer text is kept
/// byte for byte.
fn normalize_prompt_answer_payload(
    questions: &[Question],
    payload: &Value,
) -> Result<PromptAnswers, String> {
    if questions.is_empty() {
        return Err("stored prompt has no questions".to_string());
    }

    let payload: PromptAnswerPayload = serde_json::from_value(payload.clone())
        .map_err(|e| format!("invalid answer payload: {e}"))?;

    if let Some(answer) = payload.answer.as_deref().filter(|s| !s.trim().is_empty()) {
        return PromptAnswers::parse_typed(questions.len(), answer);
    }

    let answers = payload
        .answers
        .ok_or_else(|| "payload.answer or payload.answers is required".to_string())?;
    if answers.is_empty() {
        return Err("payload.answers must contain at least one answer".to_string());
    }

    let mut collected = Vec::with_capacity(answers.len());
    for (position, entry) in answers.into_iter().enumerate() {
        let item = entry.into_item(position);
        // An index with no question has nothing to validate against; it is
        // carried through so `PromptAnswers::new` refuses it by name.
        let answer = match questions.get(item.index) {
            Some(question) => {
                if let Some(header) = item.header.as_deref() {
                    if question.header.as_deref() != Some(header) {
                        return Err(format!(
                            "answer index {} header mismatch: expected {:?}, got {:?}",
                            item.index, question.header, header
                        ));
                    }
                }
                render_answer_item(question, &item)?
            }
            None => String::new(),
        };
        collected.push(QuestionAnswer {
            index: item.index,
            answer,
        });
    }

    PromptAnswers::new(questions.len(), collected)
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
    if let Some(text) = answer.text.as_deref().filter(|s| !s.trim().is_empty()) {
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
    let (prompt_id, answers, home_uri) = resolve_node_question(
        orch,
        project_key,
        issue_number,
        exec_seq,
        node_segment,
        prompt_segment,
        payload,
    )
    .await?;
    // Provenance names the job's CANONICAL home, never the coordinate the
    // caller happened to address it by. A thread is addressed through the
    // reserved `(0, 0, thread-name)` coordinate, so reassembling the actor from
    // these arguments would persist `cairn://p/{key}/0/0/{thread}` — a sentinel
    // that resolves to nothing — into every resolution receipt a thread ever
    // writes. The fallback keeps issue behavior identical when a job somehow has
    // no home to report.
    let actor = home_uri.unwrap_or_else(|| {
        format!("cairn://p/{project_key}/{issue_number}/{exec_seq}/{node_segment}")
    });
    answer_prompt_id(
        orch,
        &prompt_id,
        answers,
        crate::turns::queries::ResolutionProvenance::new(
            cairn_common::identity::AppearanceTransport::ResourcePatch,
            Some(actor),
        ),
    )
    .await
}

/// Resolve a node-coordinate question to its prompt, its normalized answers,
/// and the answering job's canonical home URI.
async fn resolve_node_question(
    orch: &Orchestrator,
    project_key: &str,
    issue_number: i32,
    exec_seq: i32,
    node_segment: &str,
    prompt_segment: &str,
    payload: &Value,
) -> Result<(String, PromptAnswers, Option<String>), String> {
    let (prompt_id, questions_json, home_uri) = lookup_prompt_for_node_question(
        orch,
        project_key,
        issue_number,
        exec_seq,
        node_segment,
        prompt_segment,
    )
    .await?;
    let questions = parse_prompt_questions(&questions_json)?;
    let answers = normalize_prompt_answer_payload(&questions, payload)?;
    Ok((prompt_id, answers, home_uri))
}

#[allow(clippy::too_many_arguments)]
pub async fn answer_node_question_with_provenance(
    orch: &Orchestrator,
    project_key: &str,
    issue_number: i32,
    exec_seq: i32,
    node_segment: &str,
    prompt_segment: &str,
    payload: &Value,
    provenance: crate::turns::queries::ResolutionProvenance,
) -> Result<PromptAnswerOutcome, String> {
    let (prompt_id, answers, _home_uri) = resolve_node_question(
        orch,
        project_key,
        issue_number,
        exec_seq,
        node_segment,
        prompt_segment,
        payload,
    )
    .await?;
    answer_prompt_id(orch, &prompt_id, answers, provenance).await
}

/// Answer a prompt by id from an answer payload (`answer` or `answers[]`), the
/// shape every answer surface sends. Normalized against the prompt's own
/// questions, read from the database that owns it.
pub async fn answer_prompt_payload(
    orch: &Orchestrator,
    prompt_id: &str,
    payload: &Value,
    provenance: crate::turns::queries::ResolutionProvenance,
) -> Result<PromptAnswerOutcome, String> {
    let db = crate::execution::routing::owning_db_for_prompt(&orch.db, prompt_id)
        .await
        .map_err(|error| error.to_string())?;
    let questions = load_prompt_questions(&db, prompt_id).await?;
    let answers = normalize_prompt_answer_payload(&questions, payload)?;
    answer_prompt_id(orch, prompt_id, answers, provenance).await
}

pub async fn answer_prompt_id(
    orch: &Orchestrator,
    prompt_id: &str,
    answers: PromptAnswers,
    provenance: crate::turns::queries::ResolutionProvenance,
) -> Result<PromptAnswerOutcome, String> {
    answer_prompt_id_impl(orch, prompt_id, answers, provenance, true).await
}

pub(crate) async fn answer_prompt_id_domain(
    orch: &Orchestrator,
    prompt_id: &str,
    answers: PromptAnswers,
    provenance: crate::turns::queries::ResolutionProvenance,
) -> Result<PromptAnswerOutcome, String> {
    answer_prompt_id_impl(orch, prompt_id, answers, provenance, false).await
}

/// A prompt's questions, as stored when it was asked.
pub(crate) async fn load_prompt_questions_conn(
    conn: &cairn_db::turso::Connection,
    prompt_id: &str,
) -> DbResult<Vec<Question>> {
    let mut rows = conn
        .query(
            "SELECT questions FROM prompts WHERE id = ?1",
            params![prompt_id],
        )
        .await?;
    let row = rows
        .next()
        .await?
        .ok_or_else(|| DbError::internal(format!("prompt not found: {prompt_id}")))?;
    parse_prompt_questions(&row.text(0)?).map_err(DbError::internal)
}

async fn load_prompt_questions(
    db: &crate::storage::LocalDb,
    prompt_id: &str,
) -> Result<Vec<Question>, String> {
    db.read(|conn| {
        let prompt_id = prompt_id.to_string();
        Box::pin(async move { load_prompt_questions_conn(conn, &prompt_id).await })
    })
    .await
    .map_err(|error| error.to_string())
}

fn check_answers_fit(answers: &PromptAnswers, questions: &[Question]) -> Result<(), String> {
    if answers.question_count() == questions.len() {
        Ok(())
    } else {
        Err(format!(
            "these answers cover {} question(s), but this prompt has {}",
            answers.question_count(),
            questions.len()
        ))
    }
}

async fn answer_prompt_id_impl(
    orch: &Orchestrator,
    prompt_id: &str,
    answers: PromptAnswers,
    provenance: crate::turns::queries::ResolutionProvenance,
    use_gate: bool,
) -> Result<PromptAnswerOutcome, String> {
    if use_gate {
        let gate_db = crate::execution::routing::owning_db_for_prompt(&orch.db, prompt_id)
            .await
            .map_err(|error| error.to_string())?;
        // Ahead of the claim: a withdrawn question has no winner to record, and
        // claiming one would leave the ledger naming an answer nothing took.
        let gate = gate_db
            .read(|conn| {
                let prompt_id = prompt_id.to_string();
                Box::pin(async move {
                    if let Some(cause) = prompt_withdrawal_conn(conn, &prompt_id).await? {
                        return Ok(Err(cause));
                    }
                    Ok(Ok(load_prompt_questions_conn(conn, &prompt_id).await?))
                })
            })
            .await
            .map_err(|error| error.to_string())?;
        let questions = match gate {
            Ok(questions) => questions,
            Err(cause) => return Ok(PromptAnswerOutcome::withdrawn(cause)),
        };
        check_answers_fit(&answers, &questions)?;
        let claim = crate::channels::ledger::claim_ask_resolution(
            &gate_db,
            prompt_id,
            &answers.to_stored(),
            provenance.surface,
            provenance.provider.as_deref(),
            provenance.conversation.as_deref(),
            provenance.actor.as_deref(),
            "question",
            prompt_id,
            chrono::Utc::now().timestamp_millis(),
        )
        .await?;
        let (winner, duplicate) = match claim {
            crate::channels::ledger::AskClaim::Won(winner) => (winner, false),
            crate::channels::ledger::AskClaim::Existing(winner) => (winner, true),
        };
        // The ledger holds the stored form. A winner claimed before answers
        // were stored by index reads back as legacy text.
        let winning = PromptAnswers::decode_stored(&winner.answer);
        let winning_response = winning.render(&questions);
        // The claim winner and the domain-action lease winner are independent.
        // Always execute with the persisted claim winner's identity, even when
        // this caller acquired the lease after losing the claim.
        let winning_provenance = winner.resolution_provenance()?;
        let now = chrono::Utc::now().timestamp_millis();
        let Some(lease) =
            crate::channels::ledger::try_lease_ask_action(&gate_db, prompt_id, now, 60_000).await?
        else {
            return Ok(PromptAnswerOutcome {
                duplicate: true,
                response: winning_response,
                withdrawn: None,
            });
        };
        // A legacy winner still to run was typed text, so the typed-input
        // parser recovers it; one it cannot read is an ordinary action failure.
        let result = match winning.into_answers(questions.len()) {
            Ok(winning) => {
                Box::pin(answer_prompt_id_impl(
                    orch,
                    prompt_id,
                    winning,
                    winning_provenance,
                    false,
                ))
                .await
            }
            Err(error) => Err(format!(
                "the recorded answer to this question cannot be read: {error}"
            )),
        };
        return match result {
            Ok(outcome) if outcome.withdrawn.is_some() => Ok(outcome),
            Ok(mut outcome) => {
                crate::channels::ledger::finalize_ask_resolution(
                    &gate_db,
                    prompt_id,
                    &format!("✓ answered: {winning_response}"),
                    chrono::Utc::now().timestamp_millis(),
                )
                .await?;
                outcome.duplicate |= duplicate;
                outcome.response = winning_response;
                Ok(outcome)
            }
            Err(error) => {
                crate::channels::ledger::record_ask_action_failure(
                    &gate_db,
                    &lease,
                    &error,
                    chrono::Utc::now().timestamp_millis(),
                )
                .await?;
                Err(error)
            }
        };
    }
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
    let questions = load_prompt_questions(&owning_db, prompt_id).await?;
    check_answers_fit(&answers, &questions)?;
    let answered = AnsweredPrompt {
        rendered: answers.render(&questions),
        answers,
    };
    let response = answered.rendered.clone();
    let now = chrono::Utc::now().timestamp();
    let prompt_id_owned = prompt_id.to_string();
    let record = owning_db
        .write(|conn| {
            let prompt_id = prompt_id_owned.clone();
            let answers = answered.answers.clone();
            let provenance = provenance.clone();
            Box::pin(
                async move {
                    let result = match record_prompt_response_conn(conn, &prompt_id, &answers, now).await? {
                        PromptRecord::Recorded(result) => result,
                        withdrawn @ PromptRecord::Withdrawn(_) => return Ok(withdrawn),
                    };
                    if !result.duplicate {
                        let surface = provenance.surface_name();
                        conn.execute(
                            "UPDATE prompts SET resolution_id = ?2, resolution_surface = ?3, resolution_provider = ?4, resolution_conversation = ?5, resolution_actor = ?6 WHERE id = ?1",
                            params![prompt_id, provenance.id, surface, provenance.provider, provenance.conversation, provenance.actor],
                        ).await?;
                    }
                    Ok(PromptRecord::Recorded(result))
                },
            )
        })
        .await
        .map_err(|e| e.to_string())?;
    let resume = match record {
        PromptRecord::Recorded(resume) => resume,
        PromptRecord::Withdrawn(cause) => return Ok(PromptAnswerOutcome::withdrawn(cause)),
    };

    if !resume.duplicate {
        let _ = orch
            .prompt_responses
            .send((prompt_id_owned.clone(), answered.clone()));
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

    let job_id = if should_resume {
        resume.job_id.clone()
    } else {
        None
    };

    let changed_issue = match resume.issue_id.as_deref() {
        Some(issue_id) => crate::issues::crud::get(&owning_db, issue_id)
            .await
            .ok()
            .flatten(),
        None => None,
    };
    // Scoped from the prompt's own job rather than the issue, so a thread's
    // question (no issue) still names its project and job.
    let prompt_change =
        crate::notify::prompt_db_change_for_id(&owning_db, &prompt_id_owned, "update").await;
    let _ = orch.services.emitter.emit("db-change", prompt_change);
    if let Some(turn_id) = resume.successor_turn_id.as_deref() {
        let change = crate::notify::turn_db_change_for_id(&owning_db, turn_id, "update").await;
        let _ = orch.services.emitter.emit("db-change", change);
    }
    if let Some(issue) = changed_issue.as_ref() {
        let _ = orch
            .services
            .emitter
            .emit("db-change", crate::notify::issue_db_change(issue, "update"));
    }
    let run_change =
        crate::notify::run_db_change_for_id(&owning_db, &resume.run_id, "update").await;
    let _ = orch.services.emitter.emit("db-change", run_change);

    // The committed answer is level-triggered persisted state. The singleton
    // scheduler observes it through the prompt-owner query; answer handling must
    // not create a second, per-wait drive mechanism.
    if resume.mcp_wait_id.is_some() {
        return Ok(PromptAnswerOutcome {
            duplicate: resume.duplicate,
            response,
            withdrawn: None,
        });
    }

    if let Some(job_id) = job_id {
        let suppress_user_event = if let (Some(tool_use_id), Some(session_id)) =
            (resume.tool_use_id.as_deref(), resume.session_id.as_deref())
        {
            let now = chrono::Utc::now().timestamp() as i32;
            let receipt = cairn_db::models::ResolutionReceipt {
                id: Some(provenance.id.clone()),
                surface: provenance.surface_name(),
                provider: provenance.provider.clone(),
                conversation: provenance.conversation.clone(),
                actor: provenance.actor.clone(),
                resolved_at: now as i64 * 1000,
            };
            if let Err(error) = crate::execution::jobs::store_blocking_write_result_with_resolution(
                orch,
                &resume.run_id,
                session_id,
                tool_use_id,
                crate::resources::mutations::BlockingResult::from(QuestionsOutcome::from(
                    answered.clone(),
                )),
                false,
                now,
                resume.predecessor_turn_id.as_deref(),
                Some(&receipt),
            ) {
                // The prompt answer and successor are already durable. Dispatching
                // with a visible user event keeps retry semantics intact, while
                // OpenRouter history normalization supplies the missing result.
                log::warn!(
                    "Failed to persist synthetic prompt tool_result for run {} and call {}: {}. Resuming with a user event",
                    resume.run_id,
                    tool_use_id,
                    error
                );
                false
            } else {
                true
            }
        } else {
            false
        };

        let prompt_resume = ResumeContext {
            suppress_user_event,
            goal_reactivation: Some(GoalReactivation::OwnedConditionResolved(
                GoalPauseOwner::UserInput {
                    request_id: prompt_id.to_string(),
                },
            )),
            ..Default::default()
        };
        continue_job_impl(orch, &job_id, Some(&response), None, Some(prompt_resume))
            .map_err(|e| format!("Failed to resume prompt response: {}", e))?;
    }

    Ok(PromptAnswerOutcome {
        duplicate: resume.duplicate,
        response,
        withdrawn: None,
    })
}

async fn lookup_prompt_for_node_question(
    orch: &Orchestrator,
    project_key: &str,
    issue_number: i32,
    exec_seq: i32,
    node_segment: &str,
    prompt_segment: &str,
) -> Result<(String, String, Option<String>), String> {
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
                let job_id = crate::jobs::queries::job_id_for_node_coordinate_conn(
                    conn,
                    &project_key,
                    issue_number,
                    exec_seq,
                    &node_segment,
                    None,
                )
                .await?
                .ok_or_else(|| DbError::internal(format!(
                    "node {project_key}-{issue_number}/{exec_seq}/{node_segment} not found"
                )))?;
                let mut rows = conn
                    .query(
                        "
                        SELECT pr.id, pr.questions
                        FROM prompts pr
                        JOIN runs r ON pr.run_id = r.id
                        JOIN jobs j ON COALESCE(pr.job_id, r.job_id) = j.id
                        WHERE j.id = ?1
                          AND pr.uri_segment = ?2
                        LIMIT 1
                        ",
                        params![
                            job_id.as_str(),
                            prompt_segment.as_str()
                        ],
                    )
                    .await?;
                let (prompt_id, questions) = rows
                    .next()
                    .await?
                    .map(|row| Ok::<_, DbError>((row.text(0)?, row.text(1)?)))
                    .transpose()?
                    .ok_or_else(|| {
                        DbError::internal(format!(
                            "question {prompt_segment} not found for {project_key}-{issue_number}/{exec_seq}/{node_segment}"
                        ))
                    })?;
                // The job's canonical home, resolved through the same
                // owner-aware walk that just found it, so provenance can name
                // the node rather than the coordinate it was addressed by.
                let home_uri =
                    crate::jobs::queries::home_uri_for_job_conn(conn, &job_id).await?;
                Ok((prompt_id, questions, home_uri))
            })
        })
        .await
        .map_err(|e| e.to_string())
}

async fn record_prompt_response_conn(
    conn: &cairn_db::turso::Connection,
    prompt_id: &str,
    answers: &PromptAnswers,
    answered_at: i64,
) -> DbResult<PromptRecord> {
    // First, before an MCP continuation can claim the answer: a withdrawn
    // question takes no answer from any path.
    if let Some(cause) = prompt_withdrawal_conn(conn, prompt_id).await? {
        return Ok(PromptRecord::Withdrawn(cause));
    }
    let mcp_claim = super::mcp_continuation::record_and_claim_answer_conn(
        conn,
        prompt_id,
        answers,
        answered_at.saturating_mul(1000),
    )
    .await?;
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
    let (mcp_wait_id, mcp_duplicate) = match mcp_claim {
        Some(super::mcp_continuation::AnswerClaim::Claimed { wait_id, .. }) => {
            (Some(wait_id), false)
        }
        Some(super::mcp_continuation::AnswerClaim::AlreadyConsumed { wait_id }) => {
            (Some(wait_id), true)
        }
        None => (None, false),
    };

    let duplicate = if mcp_wait_id.is_some() {
        mcp_duplicate
    } else if already_answered {
        true
    } else {
        conn.execute(
            "
            UPDATE prompts
            SET response = ?1, answered_at = ?2
            WHERE id = ?3 AND response IS NULL AND withdrawn_at IS NULL
            ",
            params![answers.to_stored(), answered_at, prompt_id],
        )
        .await?
            == 0
    };

    let successor_turn_id = if mcp_wait_id.is_none() && !duplicate {
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

    Ok(PromptRecord::Recorded(PromptResume {
        run_id,
        mcp_wait_id,
        session_id,
        predecessor_turn_id,
        successor_turn_id,
        job_id,
        issue_id,
        tool_use_id,
        duplicate,
    }))
}

async fn ensure_prompt_successor_turn_conn(
    conn: &cairn_db::turso::Connection,
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
    conn: &cairn_db::turso::Connection,
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

    fn questions() -> Vec<Question> {
        vec![
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
        ]
    }

    fn normalized(payload: Value) -> Result<Vec<(usize, String)>, String> {
        normalize_prompt_answer_payload(&questions(), &payload).map(|answers| {
            answers
                .into_answers()
                .into_iter()
                .map(|answer| (answer.index, answer.answer))
                .collect()
        })
    }

    fn pair(index: usize, answer: &str) -> (usize, String) {
        (index, answer.to_string())
    }

    #[test]
    fn normalizes_single_question_shorthand_verbatim() {
        let questions = vec![Question {
            question: "Continue?".to_string(),
            header: None,
            options: vec![],
            multi_select: false,
        }];
        let text = "  Yes.\n\n1. a\n2. b\n";
        let answers =
            normalize_prompt_answer_payload(&questions, &json!({ "answer": text })).unwrap();
        assert_eq!(answers.answers()[0].answer, text);
    }

    #[test]
    fn normalizes_multi_question_answers() {
        assert_eq!(
            normalized(
                json!({"answers":[{"index":0,"selection":"Yes"},{"index":1,"text":"custom"}]})
            )
            .unwrap(),
            vec![pair(0, "Yes"), pair(1, "custom")]
        );
    }

    #[test]
    fn normalizes_multi_select_answers() {
        assert_eq!(
            normalized(
                json!({"answers":[{"index":1,"selections":["A","B"]},{"index":0,"selection":"No"}]})
            )
            .unwrap(),
            vec![pair(0, "No"), pair(1, "A, B")]
        );
    }

    #[test]
    fn refuses_answers_that_leave_a_question_unanswered() {
        let error =
            normalized(json!({"answers":[{"index":1,"selections":["A","B"]}]})).unwrap_err();
        assert!(
            error.starts_with("question index 0 (Question 1) has no answer"),
            "{error}"
        );
    }

    #[test]
    fn keeps_free_text_byte_for_byte() {
        let text = "\nSomething else\n\n- with a list\n";
        assert_eq!(
            normalized(json!({"answers":[{"index":0,"text":text},{"index":1,"selection":"A"}]}))
                .unwrap(),
            vec![pair(0, text), pair(1, "A")]
        );
    }

    #[test]
    fn normalizes_bare_string_shorthand_items() {
        assert_eq!(
            normalized(json!({"answers":[{"index":0,"selection":"Yes"},"free text for q2"]}))
                .unwrap(),
            vec![pair(0, "Yes"), pair(1, "free text for q2")]
        );
    }

    #[test]
    fn answer_shorthand_on_several_questions_takes_canonical_lines() {
        assert_eq!(
            normalized(json!({"answer":"Question 1: Yes\nand why\nQuestion 2: A"})).unwrap(),
            vec![pair(0, "Yes\nand why"), pair(1, "A")]
        );
        let error = normalized(json!({"answer":"Yes\nA"})).unwrap_err();
        assert!(
            error.starts_with("line 1 must begin \"Question 1: \""),
            "{error}"
        );
    }

    #[test]
    fn rejects_invalid_selection() {
        let error = normalized(json!({"answers":[{"index":1,"selection":"C"}]})).unwrap_err();
        assert!(error.contains("invalid selection"));
    }

    #[test]
    fn rejects_invalid_index() {
        let error = normalized(
            json!({"answers":[{"index":0,"selection":"Yes"},{"index":2,"text":"oops"}]}),
        )
        .unwrap_err();
        assert!(
            error.contains("answer index 2 (Question 3) is out of range"),
            "{error}"
        );
    }

    #[test]
    fn rejects_duplicate_index() {
        let error = normalized(
            json!({"answers":[{"index":0,"selection":"Yes"},{"index":0,"selection":"No"}]}),
        )
        .unwrap_err();
        assert!(
            error.contains("question index 0 (Question 1) is answered more than once"),
            "{error}"
        );
    }
}
