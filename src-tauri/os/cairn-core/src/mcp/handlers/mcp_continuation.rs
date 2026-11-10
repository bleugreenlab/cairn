//! Durable state and atomic storage operations for external MCP continuations.
//!
//! The lifecycle row remains `agent_waits`; this module only owns the payload of
//! its `mcp_continuation` condition and the explicit link from an ordinary Cairn
//! prompt to that wait.

use crate::config::mcp_servers::McpServerConfig;
use crate::config::mcp_servers::McpServerConfigExt;
use crate::mcp::gateway::McpCallOutcome;
use crate::models::question::{PromptAnswers, StoredAnswer};
use crate::storage::{DbError, LocalDb, RowExt};
use cairn_db::turso::params;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock, Weak};
use tokio::sync::Mutex;

pub(crate) const MAX_MRTR_ROUNDS: u32 = 8;
pub(crate) const DEFAULT_CONTINUATION_TTL_MS: i64 =
    cairn_common::run_contract::RUN_BATCH_CEILING_MS as i64;

static DRIVE_LOCKS: OnceLock<Mutex<HashMap<String, Weak<Mutex<()>>>>> = OnceLock::new();

async fn drive_lock(wait_id: &str) -> Arc<Mutex<()>> {
    let locks = DRIVE_LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut locks = locks.lock().await;
    if let Some(lock) = locks.get(wait_id).and_then(Weak::upgrade) {
        return lock;
    }

    let lock = Arc::new(Mutex::new(()));
    locks.insert(wait_id.to_owned(), Arc::downgrade(&lock));
    lock
}

async fn dispatch_pending_operation(
    orch: &crate::orchestrator::Orchestrator,
    db: &LocalDb,
    wait_id: &str,
    state: &mut McpContinuationState,
    operation: PendingOperation,
) -> Result<(), String> {
    match operation {
        PendingOperation::ContinueTool {
            operation_id,
            input_responses,
            request_state,
        } => {
            let outcome = orch
                .mcp_gateway()
                .ok_or("MCP gateway is not available")?
                .call_tool_once(
                    &state.session_key,
                    &state.server,
                    &state.brokered("mcp continuation round")?,
                    &state.tool,
                    state.arguments.clone(),
                    Some(input_responses),
                    request_state,
                    state.timeout_ms,
                    Some(&operation_id),
                )
                .await?;
            apply_call_outcome(db, wait_id, state, outcome)?;
        }
        PendingOperation::UpdateTask {
            operation_id,
            task_id,
            input_responses,
        } => {
            orch.mcp_gateway()
                .ok_or("MCP gateway is not available")?
                .update_task(
                    &state.session_key,
                    &state.server,
                    &state.brokered("mcp task input")?,
                    &task_id,
                    input_responses,
                    Some(&operation_id),
                )
                .await?;
            state.task_input_pending = false;
            schedule_next_poll(state, chrono::Utc::now().timestamp_millis())?;
        }
    }
    state.pending_operation = None;
    store(db, wait_id, state).await?;
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum PendingOperation {
    ContinueTool {
        operation_id: String,
        input_responses: serde_json::Value,
        request_state: Option<String>,
    },
    UpdateTask {
        operation_id: String,
        task_id: String,
        input_responses: serde_json::Value,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct McpContinuationState {
    pub server: String,
    pub session_key: String,
    /// The server's **authored** configuration, `${VAR}` references intact.
    ///
    /// This row is persisted between protocol rounds, so an expanded config
    /// here would write the server's resolved credentials into the database and
    /// leave them there for the life of the record. The reference is expanded
    /// through the broker immediately before each gateway call instead, which
    /// also means a rotated credential takes effect on the next round rather
    /// than at the next fresh call.
    pub config: McpServerConfig,
    pub tool: String,
    #[serde(default)]
    pub arguments: serde_json::Value,
    #[serde(default)]
    pub request_state: Option<String>,
    #[serde(default)]
    pub timeout_ms: Option<u32>,
    #[serde(default)]
    pub pending_operation: Option<PendingOperation>,
    #[serde(default)]
    pub pending_input_requests: Option<serde_json::Value>,
    #[serde(default)]
    pub task_input_pending: bool,
    #[serde(default)]
    pub mrtr_round: u32,
    #[serde(default)]
    pub pending_prompt_id: Option<String>,
    #[serde(default)]
    pub task: Option<McpTaskCoordinates>,
    #[serde(default)]
    pub next_poll_at_ms: Option<i64>,
    #[serde(default)]
    pub deadline_ms: Option<i64>,
}

impl McpContinuationState {
    /// Expand this server's `${VAR}` references for one gateway call.
    ///
    /// `server` is the scoped credential key, which is also what the gateway
    /// receives, so the config is resolved against exactly the credentials the
    /// call will authenticate with.
    fn brokered(&self, purpose: &str) -> Result<crate::security::BrokeredMcpConfig, String> {
        self.config.brokered(&self.server, purpose)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct McpTaskCoordinates {
    pub id: String,
    #[serde(default)]
    pub poll_interval_ms: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum PersistedCondition {
    McpContinuation { state: McpContinuationState },
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum AnswerClaim {
    Claimed {
        wait_id: String,
        answer: serde_json::Value,
    },
    AlreadyConsumed {
        wait_id: String,
    },
}

pub(crate) async fn load(
    db: &LocalDb,
    wait_id: &str,
) -> Result<Option<McpContinuationState>, String> {
    let wait_id = wait_id.to_owned();
    db.read(|conn| {
        let wait_id = wait_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT condition_json FROM agent_waits WHERE id=?1",
                    params![wait_id],
                )
                .await?;
            let Some(row) = rows.next().await? else {
                return Ok(None);
            };
            decode(&row.text(0)?)
                .map(|mut state| {
                    ensure_deadline(&mut state, chrono::Utc::now().timestamp_millis());
                    Some(state)
                })
                .map_err(DbError::internal)
        })
    })
    .await
    .map_err(|error| error.to_string())
}

pub(crate) async fn store(
    db: &LocalDb,
    wait_id: &str,
    state: &McpContinuationState,
) -> Result<bool, String> {
    validate(state)?;
    super::durable_suspend::store_condition(
        db,
        wait_id,
        &super::durable_suspend::Condition::McpContinuation {
            state: state.clone(),
        },
        state.deadline_ms,
    )
    .await
}

/// What the scheduler reads of a continuation without opening its
/// `condition_json`. Stored beside it by
/// [`super::durable_suspend::ConditionColumns`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct McpSchedule {
    pub(crate) due_at_ms: Option<i64>,
    pub(crate) next_poll_at_ms: Option<i64>,
    pub(crate) pending_prompt_id: Option<String>,
}

/// The earliest moment this wait's own state makes it due; a committed answer
/// to `pending_prompt_id` is the one other trigger, read from `prompts`.
///
/// `deadline_ms` is the row's column, which is what expires the wait.
///
/// A state with work to do now is due at once: a stored completion that still
/// needs resolving, an outbound operation to (re)dispatch, or input requests no
/// prompt has asked yet. Otherwise it is due at its next task poll or its
/// deadline, whichever comes first, and a wait awaiting an unanswered prompt is
/// due only at its deadline.
pub(crate) fn schedule(state: &McpContinuationState, deadline_ms: Option<i64>) -> McpSchedule {
    // A serialized `Some(Null)` is absent, as the JSON predicate this rule
    // replaced (`json_type ... NOT IN ('null')`) read it.
    let unasked_input = state
        .pending_input_requests
        .as_ref()
        .is_some_and(|requests| !requests.is_null())
        && state.pending_prompt_id.is_none();
    let immediately = state.arguments.get("__cairn_complete").is_some()
        || state.pending_operation.is_some()
        || unasked_input;
    let due_at_ms = if immediately {
        Some(0)
    } else {
        let poll = state
            .task
            .as_ref()
            .map(|_| state.next_poll_at_ms.unwrap_or(0));
        poll.into_iter().chain(deadline_ms).min()
    };
    McpSchedule {
        due_at_ms,
        next_poll_at_ms: state.next_poll_at_ms,
        pending_prompt_id: state.pending_prompt_id.clone(),
    }
}

/// What an MCP tool receives for a stored question answer. The MCP input
/// prompt has exactly one question, so its single answer is the input: as JSON
/// when it parses as JSON, otherwise as a string. A legacy stored value is
/// taken whole as that single answer.
fn mcp_input_value(stored: &str) -> serde_json::Value {
    let answer = match PromptAnswers::decode_stored(stored) {
        StoredAnswer::Answers(answers) if answers.question_count() == 1 => {
            answers.into_answers().remove(0).answer
        }
        StoredAnswer::Answers(answers) => {
            return serde_json::to_value(answers.answers()).unwrap_or_default()
        }
        StoredAnswer::Legacy(text) => text,
    };
    serde_json::from_str(&answer).unwrap_or(serde_json::Value::String(answer))
}

pub(crate) async fn record_and_claim_answer_conn(
    conn: &cairn_db::storage::TrackedConnection,
    prompt_id: &str,
    answers: &PromptAnswers,
    now_ms: i64,
) -> Result<Option<AnswerClaim>, DbError> {
    let mut rows = conn
        .query(
            "SELECT mcp_wait_id,response,mcp_consumed_at FROM prompts WHERE id=?1",
            params![prompt_id],
        )
        .await?;
    let Some(row) = rows.next().await? else {
        return Ok(None);
    };
    let Some(wait_id) = row.opt_text(0)? else {
        return Ok(None);
    };
    if row.opt_text(1)?.is_some() || row.opt_i64(2)?.is_some() {
        return Ok(Some(AnswerClaim::AlreadyConsumed { wait_id }));
    }
    let stored = answers.to_stored();
    let changed = conn
        .execute(
            "UPDATE prompts SET response=?2,answered_at=?3,mcp_consumed_at=?3 WHERE id=?1 AND response IS NULL AND withdrawn_at IS NULL AND mcp_consumed_at IS NULL",
            params![prompt_id, stored.as_str(), now_ms],
        )
        .await?;
    if changed == 0 {
        return Ok(Some(AnswerClaim::AlreadyConsumed { wait_id }));
    }
    Ok(Some(AnswerClaim::Claimed {
        wait_id,
        answer: mcp_input_value(&stored),
    }))
}

pub(crate) fn advance_mrtr(
    state: &mut McpContinuationState,
    request_state: Option<String>,
) -> Result<(), String> {
    if state.mrtr_round >= MAX_MRTR_ROUNDS {
        return Err(format!("MCP input exceeded {MAX_MRTR_ROUNDS} rounds"));
    }
    state.mrtr_round += 1;
    state.request_state = request_state;
    state.pending_prompt_id = None;
    Ok(())
}

pub(crate) fn set_task(
    state: &mut McpContinuationState,
    id: String,
    poll_interval_ms: u64,
    now_ms: i64,
    ttl_ms: Option<u64>,
) {
    let poll_interval_ms = poll_interval_ms.max(100);
    state.task = Some(McpTaskCoordinates {
        id,
        poll_interval_ms,
    });
    state.next_poll_at_ms =
        Some(now_ms.saturating_add(poll_interval_ms.min(i64::MAX as u64) as i64));
    let default_deadline = now_ms.saturating_add(DEFAULT_CONTINUATION_TTL_MS);
    let task_deadline = ttl_ms
        .map(|ttl| now_ms.saturating_add(ttl.min(i64::MAX as u64) as i64))
        .unwrap_or(default_deadline);
    state.deadline_ms = Some(
        state
            .deadline_ms
            .unwrap_or(default_deadline)
            .min(task_deadline),
    );
}

pub(crate) fn ensure_deadline(state: &mut McpContinuationState, now_ms: i64) {
    state
        .deadline_ms
        .get_or_insert_with(|| now_ms.saturating_add(DEFAULT_CONTINUATION_TTL_MS));
}

pub(crate) fn schedule_next_poll(
    state: &mut McpContinuationState,
    now_ms: i64,
) -> Result<i64, String> {
    let task = state
        .task
        .as_ref()
        .ok_or_else(|| "MCP continuation has no task".to_string())?;
    let next = now_ms.saturating_add(task.poll_interval_ms.min(i64::MAX as u64) as i64);
    if state.deadline_ms.is_some_and(|deadline| next > deadline) {
        return Err("MCP task deadline expired".to_string());
    }
    state.next_poll_at_ms = Some(next);
    Ok(next)
}

pub(crate) fn clear_task(state: &mut McpContinuationState) {
    state.task = None;
    state.next_poll_at_ms = None;
}

fn decode(json: &str) -> Result<McpContinuationState, String> {
    let PersistedCondition::McpContinuation { state } = serde_json::from_str(json)
        .map_err(|error| format!("malformed MCP continuation state: {error}"))?;
    validate(&state)?;
    Ok(state)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DriveOutcome {
    Pending,
    Terminal(String),
}

/// Advance one durable MCP continuation by at most one persisted transition or
/// outbound operation. Waiting is represented by `Pending`; no worker is held.
pub(crate) async fn drive(
    orch: crate::orchestrator::Orchestrator,
    db: std::sync::Arc<LocalDb>,
    record: super::durable_suspend::Record,
) -> DriveOutcome {
    let lock = drive_lock(&record.id).await;
    let _guard = lock.lock().await;
    match drive_inner(&orch, &db, &record).await {
        Ok(Some(result)) => DriveOutcome::Terminal(
            serde_json::json!({"outcome":"completed","result":result.text}).to_string(),
        ),
        Ok(None) => DriveOutcome::Pending,
        Err(error) => {
            DriveOutcome::Terminal(serde_json::json!({"outcome":"error","error":error}).to_string())
        }
    }
}

async fn drive_inner(
    orch: &crate::orchestrator::Orchestrator,
    db: &LocalDb,
    record: &super::durable_suspend::Record,
) -> Result<Option<crate::mcp::gateway::McpToolCallResult>, String> {
    use crate::mcp::gateway::McpTaskOutcome;
    let mut state = load(db, &record.id)
        .await?
        .ok_or("MCP continuation was lost")?;
    if let Some(result) = take_complete(&state) {
        return Ok(Some(result));
    }
    if let Some(operation) = state.pending_operation.clone() {
        dispatch_pending_operation(orch, db, &record.id, &mut state, operation).await?;
        return Ok(take_complete(&state));
    }
    if let Some(prompt_id) = state.pending_prompt_id.clone() {
        let Some(response) = load_answer(db, &prompt_id).await? else {
            if state
                .deadline_ms
                .is_some_and(|deadline| chrono::Utc::now().timestamp_millis() >= deadline)
            {
                return Err("MCP input expired".into());
            }
            return Ok(None);
        };
        state.pending_prompt_id = None;
        state.pending_input_requests = None;
        if state.task_input_pending {
            let task_id = state.task.as_ref().ok_or("MCP task was lost")?.id.clone();
            state.pending_operation = Some(PendingOperation::UpdateTask {
                operation_id: cairn_common::ids::mint_child(&record.id),
                task_id,
                input_responses: response,
            });
        } else {
            let request_state = state.request_state.clone();
            advance_mrtr(&mut state, request_state)?;
            state.pending_operation = Some(PendingOperation::ContinueTool {
                operation_id: cairn_common::ids::mint_child(&record.id),
                input_responses: response,
                request_state: state.request_state.clone(),
            });
        }
        store(db, &record.id, &state).await?;
        return Ok(None);
    }
    if let Some(requests) = state.pending_input_requests.clone() {
        let payload = input_prompt(requests);
        let request = cairn_common::protocol::CallbackRequest {
            answer_bound_ms: None,
            cwd: ".".into(),
            run_id: Some(record.run_id.clone()),
            tool: "run".into(),
            payload: serde_json::Value::Null,
            tool_use_id: None,
            thread_id: None,
            origin: None,
        };
        let _ = super::planning::ask_mcp_questions(orch, &request, payload, &record.id).await;
        let prompt_id = latest_prompt(db, &record.id)
            .await?
            .ok_or("MCP input prompt was not persisted")?;
        state.pending_prompt_id = Some(prompt_id);
        store(db, &record.id, &state).await?;
        return Ok(None);
    }
    let task = state
        .task
        .clone()
        .ok_or("MCP continuation has neither input nor task")?;
    let now = chrono::Utc::now().timestamp_millis();
    if state.deadline_ms.is_some_and(|deadline| now >= deadline) {
        return Err("MCP task TTL expired".into());
    }
    if state.next_poll_at_ms.is_some_and(|next| next > now) {
        return Ok(None);
    }
    match orch
        .mcp_gateway()
        .ok_or("MCP gateway is not available")?
        .get_task(
            &state.session_key,
            &state.server,
            &state.brokered("mcp task poll")?,
            &task.id,
        )
        .await?
    {
        McpTaskOutcome::Working { poll_interval_ms } => {
            if let Some(ms) = poll_interval_ms {
                state.task.as_mut().unwrap().poll_interval_ms = ms.max(100);
            }
            schedule_next_poll(&mut state, chrono::Utc::now().timestamp_millis())?;
            store(db, &record.id, &state).await?;
            Ok(None)
        }
        McpTaskOutcome::InputRequired { input_requests } => {
            state.pending_input_requests = Some(input_requests);
            state.task_input_pending = true;
            store(db, &record.id, &state).await?;
            Ok(None)
        }
        McpTaskOutcome::Complete(result) => {
            state.arguments = serde_json::json!({"__cairn_complete": result});
            clear_task(&mut state);
            store(db, &record.id, &state).await?;
            Ok(take_complete(&state))
        }
        McpTaskOutcome::Failed { message } => Err(format!("MCP task failed: {message}")),
        McpTaskOutcome::Cancelled => Err("MCP task was cancelled".into()),
    }
}

pub(super) const SCHEDULER_SCAN_LIMIT: usize = 64;
/// Gives the suspended `run` response time to reach the provider before the
/// global scheduler can drive its first continuation transition.
const INITIAL_PARK_HANDOFF_MS: i64 = 1_000;
pub(crate) const SCHEDULER_INTERVAL: std::time::Duration = std::time::Duration::from_millis(250);

/// A pending wait by id, through the primary key.
pub(super) const LOAD_RECORD_SQL: &str = "SELECT id,job_id,run_id,session_id,predecessor_turn_id,tool_use_id,condition_json,deadline_ms,created_at FROM agent_waits WHERE id=?1 AND state='pending'";

async fn load_record(
    db: &LocalDb,
    wait_id: &str,
) -> Result<Option<super::durable_suspend::Record>, String> {
    let wait_id = wait_id.to_owned();
    db.read(|conn| {
        let wait_id = wait_id.clone();
        Box::pin(async move {
            let mut rows = conn.query(LOAD_RECORD_SQL, params![wait_id]).await?;
            let Some(row) = rows.next().await? else {
                return Ok(None);
            };
            let condition = serde_json::from_str(&row.text(6)?)
                .map_err(|error| DbError::internal(error.to_string()))?;
            Ok(Some(super::durable_suspend::Record {
                id: row.text(0)?,
                job_id: row.text(1)?,
                run_id: row.text(2)?,
                session_id: row.text(3)?,
                turn_id: row.text(4)?,
                tool_use_id: row.text(5)?,
                condition,
                deadline: row.opt_i64(7)?,
                created: row.i64(8)?,
            }))
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// The due MCP continuations, earliest first.
///
/// Everything it reads of `agent_waits` is in `idx_agent_waits_mcp_due`, which
/// holds only pending continuations, so a tick never touches a row's
/// `condition_json` or the table's resolved history. A wait awaiting an answer
/// probes the one prompt it awaits by primary key: an answer to an earlier
/// prompt the wait already consumed does not make it due again.
pub(super) const DUE_SQL: &str = "SELECT id FROM agent_waits WHERE state='pending' AND kind='mcp_continuation' AND created_at<=?1-?2 AND (due_at_ms<=?1 OR (pending_prompt_id IS NOT NULL AND EXISTS (SELECT 1 FROM prompts WHERE prompts.id=agent_waits.pending_prompt_id AND prompts.mcp_wait_id=agent_waits.id AND prompts.response IS NOT NULL))) ORDER BY COALESCE(next_poll_at_ms,deadline_ms,created_at),created_at LIMIT ?3";

/// The selection [`DUE_SQL`] replaced, read from `condition_json` on every
/// row. Kept so tests can prove the two agree everywhere but the intended
/// differences (see `due_selection_matches_the_reference_but_for_intended_differences`).
#[cfg(test)]
pub(super) const REFERENCE_DUE_SQL: &str = "SELECT id FROM agent_waits WHERE state='pending' AND created_at<=?1-?2 AND json_extract(condition_json,'$.kind')='mcp_continuation' AND (deadline_ms<=?1 OR json_type(condition_json,'$.state.pending_operation') NOT IN ('null') OR (json_type(condition_json,'$.state.pending_input_requests') NOT IN ('null') AND json_type(condition_json,'$.state.pending_prompt_id') IN ('null')) OR EXISTS (SELECT 1 FROM prompts WHERE prompts.mcp_wait_id=agent_waits.id AND prompts.response IS NOT NULL) OR (json_type(condition_json,'$.state.task') NOT IN ('null') AND COALESCE(json_extract(condition_json,'$.state.next_poll_at_ms'),0)<=?1)) ORDER BY COALESCE(json_extract(condition_json,'$.state.next_poll_at_ms'),deadline_ms,created_at),created_at LIMIT ?3";

pub(super) async fn due_wait_ids(db: &LocalDb, now_ms: i64) -> Result<Vec<String>, String> {
    query_due(db, DUE_SQL, now_ms, SCHEDULER_SCAN_LIMIT).await
}

/// Run a due-selection statement shaped like [`DUE_SQL`]: `?1` now, `?2` the
/// park handoff, `?3` the limit.
pub(super) async fn query_due(
    db: &LocalDb,
    sql: &'static str,
    now_ms: i64,
    limit: usize,
) -> Result<Vec<String>, String> {
    db.read(move |conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(sql, params![now_ms, INITIAL_PARK_HANDOFF_MS, limit as i64])
                .await?;
            let mut ids = Vec::new();
            while let Some(row) = rows.next().await? {
                ids.push(row.text(0)?);
            }
            Ok(ids)
        })
    })
    .await
    .map_err(|error| error.to_string())
}

async fn drive_and_resolve(
    orch: crate::orchestrator::Orchestrator,
    db: Arc<LocalDb>,
    wait_id: String,
) {
    let Ok(Some(record)) = load_record(&db, &wait_id).await else {
        return;
    };
    if let DriveOutcome::Terminal(result) = drive(orch.clone(), db.clone(), record.clone()).await {
        super::durable_suspend::drive_resolution(&orch, &db, &record, &result, false).await;
    }
}

/// The scheduler's single drive of one due wait.
///
/// Nothing else arms a drive. Every trigger, a committed prompt answer
/// included, is durable state that [`scheduler_tick`] reads, so a trigger that
/// lands while no host is up is seen on the next tick after one starts.
fn arm(orch: crate::orchestrator::Orchestrator, db: Arc<LocalDb>, wait_id: String) {
    tokio::spawn(drive_and_resolve(orch, db, wait_id));
}

pub(crate) async fn scheduler_tick(orch: &crate::orchestrator::Orchestrator) {
    for db in orch.db.all_dbs().await {
        match due_wait_ids(&db, chrono::Utc::now().timestamp_millis()).await {
            Ok(ids) => {
                for wait_id in ids {
                    arm(orch.clone(), db.clone(), wait_id);
                }
            }
            Err(error) => log::warn!("durable MCP scheduler scan failed: {error}"),
        }
    }
}

fn input_prompt(requests: serde_json::Value) -> crate::mcp::types::AskUserPayload {
    crate::mcp::types::AskUserPayload {
        questions: vec![crate::mcp::types::Question {
            question: format!("The MCP tool requires input: {requests}"),
            header: Some("MCP input".into()),
            options: vec![crate::mcp::types::QuestionOption {
                label: "Continue".into(),
                description: "Submit this response to the MCP tool".into(),
                recommended: false,
            }],
            multi_select: false,
        }],
    }
}

async fn latest_prompt(db: &LocalDb, wait_id: &str) -> Result<Option<String>, String> {
    let wait_id = wait_id.to_string();
    db.read(|conn| {
        let wait_id = wait_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id FROM prompts WHERE mcp_wait_id=?1 ORDER BY created_at DESC LIMIT 1",
                    params![wait_id],
                )
                .await?;
            rows.next().await?.map(|row| row.text(0)).transpose()
        })
    })
    .await
    .map_err(|e| e.to_string())
}

async fn load_answer(db: &LocalDb, prompt_id: &str) -> Result<Option<serde_json::Value>, String> {
    let id = prompt_id.to_string();
    let stored = db
        .read(|conn| {
            let id = id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query("SELECT response FROM prompts WHERE id=?1", params![id])
                    .await?;
                Ok(rows
                    .next()
                    .await?
                    .and_then(|row| row.opt_text(0).ok().flatten()))
            })
        })
        .await
        .map_err(|e| e.to_string())?;
    Ok(stored.map(|answer| mcp_input_value(&answer)))
}

fn apply_call_outcome(
    db: &LocalDb,
    wait_id: &str,
    state: &mut McpContinuationState,
    outcome: McpCallOutcome,
) -> Result<(), String> {
    let _ = (db, wait_id);
    match outcome {
        McpCallOutcome::Complete(result) => {
            state.arguments = serde_json::json!({"__cairn_complete": result});
            clear_task(state);
        }
        McpCallOutcome::InputRequired {
            input_requests,
            request_state,
        } => {
            state.pending_input_requests = Some(input_requests);
            state.request_state = request_state;
        }
        McpCallOutcome::Task {
            task_id,
            poll_interval_ms,
            ttl_ms,
        } => {
            set_task(
                state,
                task_id,
                poll_interval_ms.unwrap_or(500),
                chrono::Utc::now().timestamp_millis(),
                ttl_ms,
            );
        }
    }
    Ok(())
}

fn take_complete(state: &McpContinuationState) -> Option<crate::mcp::gateway::McpToolCallResult> {
    serde_json::from_value(state.arguments.get("__cairn_complete")?.clone()).ok()
}

fn validate(state: &McpContinuationState) -> Result<(), String> {
    for (name, value) in [
        ("server", state.server.as_str()),
        ("session key", state.session_key.as_str()),
        ("tool", state.tool.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(format!("MCP continuation {name} is empty"));
        }
    }
    if state.mrtr_round > MAX_MRTR_ROUNDS {
        return Err(format!("MCP continuation round exceeds {MAX_MRTR_ROUNDS}"));
    }
    if state.task.is_none() && state.next_poll_at_ms.is_some() {
        return Err("MCP continuation has a poll time without a task".to_string());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::durable_suspend::{Condition, ConditionColumns};
    use super::*;
    use crate::storage::LocalDb;

    /// The row JSON the writer stores for `state`.
    fn condition_json(state: &McpContinuationState) -> String {
        ConditionColumns::of(
            &Condition::McpContinuation {
                state: state.clone(),
            },
            state.deadline_ms,
        )
        .unwrap()
        .json
    }

    fn state() -> McpContinuationState {
        McpContinuationState {
            server: "mock".into(),
            session_key: "job".into(),
            config: serde_json::from_value(serde_json::json!({})).unwrap(),
            tool: "long_task".into(),
            arguments: serde_json::json!({"x": 1}),
            request_state: None,
            timeout_ms: None,
            pending_operation: None,
            pending_input_requests: None,
            task_input_pending: false,
            mrtr_round: 0,
            pending_prompt_id: None,
            task: None,
            next_poll_at_ms: None,
            deadline_ms: None,
        }
    }

    /// The row that survives between protocol rounds must carry the `${VAR}`
    /// reference, not what it resolves to.
    ///
    /// Multi-round MCP calls used to persist the *expanded* config here, which
    /// wrote the server's resolved credentials into the database and left them
    /// there for the life of the record — a durable disclosure that no amount of
    /// output scrubbing reaches. The expansion happens per gateway call now.
    #[test]
    fn the_persisted_row_holds_the_reference_and_never_the_resolved_value() {
        crate::config::secrets::mock_keychain::install();
        let secret = "cont-Rt48Km19Zc03Qb";
        crate::config::secrets::set_secret("mock", "CONT_TOKEN", secret).unwrap();

        let mut state = state();
        state.config = serde_json::from_value(serde_json::json!({
            "type": "http",
            "url": "https://example.test/mcp",
            "headers": {"Authorization": "Bearer ${CONT_TOKEN}"}
        }))
        .unwrap();

        let encoded = condition_json(&state);
        assert!(
            encoded.contains("${CONT_TOKEN}"),
            "the durable row must keep the reference: {encoded}"
        );
        assert!(
            !encoded.contains(secret),
            "the durable row must not carry the resolved value"
        );

        // And the reference still resolves when the call actually happens.
        let brokered = state.brokered("test").unwrap();
        assert_eq!(
            brokered
                .resolved_for_connect()
                .headers
                .get("Authorization")
                .unwrap(),
            &format!("Bearer {secret}")
        );
    }

    async fn db() -> LocalDb {
        let db = LocalDb::open(":memory:").await.unwrap();
        db.exclusive(|conn| Box::pin(async move {
            conn.execute_batch("CREATE TABLE agent_waits(id TEXT PRIMARY KEY,state TEXT NOT NULL,condition_json TEXT NOT NULL,deadline_ms INTEGER,created_at INTEGER DEFAULT 0,kind TEXT,due_at_ms INTEGER,next_poll_at_ms INTEGER,pending_prompt_id TEXT); CREATE TABLE prompts(id TEXT PRIMARY KEY,response TEXT,answered_at INTEGER,mcp_wait_id TEXT REFERENCES agent_waits(id),mcp_consumed_at INTEGER,withdrawn_at INTEGER);").await?;
            Ok(())
        })).await.unwrap();
        db
    }

    #[test]
    fn old_optional_fields_default_and_malformed_state_is_rejected() {
        let json = serde_json::json!({"kind":"mcp_continuation","state":{"server":"s","session_key":"j","config":{},"tool":"t"}}).to_string();
        let decoded = decode(&json).unwrap();
        assert_eq!(decoded.mrtr_round, 0);
        assert!(decoded.task.is_none());
        assert!(decoded.pending_operation.is_none());
        assert!(decode(r#"{"kind":"mcp_continuation","state":{"server":"","session_key":"j","config":{},"tool":"t"}}"#).is_err());
    }

    #[test]
    fn continuation_deadline_defaults_and_task_ttl_can_only_shorten_it() {
        let mut defaulted = state();
        ensure_deadline(&mut defaulted, 1_000);
        assert_eq!(
            defaulted.deadline_ms,
            Some(1_000 + DEFAULT_CONTINUATION_TTL_MS)
        );

        set_task(&mut defaulted, "task".into(), 100, 2_000, None);
        assert_eq!(
            defaulted.deadline_ms,
            Some(1_000 + DEFAULT_CONTINUATION_TTL_MS),
            "a server without a TTL must retain Cairn's maximum deadline"
        );
        set_task(&mut defaulted, "task".into(), 100, 2_000, Some(5_000));
        assert_eq!(defaulted.deadline_ms, Some(7_000));

        let mut excessive = state();
        ensure_deadline(&mut excessive, 1_000);
        set_task(
            &mut excessive,
            "task".into(),
            100,
            2_000,
            Some((DEFAULT_CONTINUATION_TTL_MS as u64) * 2),
        );
        assert_eq!(
            excessive.deadline_ms,
            Some(1_000 + DEFAULT_CONTINUATION_TTL_MS)
        );
    }

    #[tokio::test]
    async fn legacy_row_without_deadline_receives_default_on_load() {
        let db = db().await;
        let json = condition_json(&state());
        db.write(|conn| {
            let json = json.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO agent_waits(id,state,condition_json,deadline_ms) VALUES('legacy','pending',?1,NULL)",
                    params![json],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
        let before = chrono::Utc::now().timestamp_millis();
        let loaded = load(&db, "legacy").await.unwrap().unwrap();
        let deadline = loaded.deadline_ms.unwrap();
        assert!(deadline >= before + DEFAULT_CONTINUATION_TTL_MS);
        assert!(deadline <= chrono::Utc::now().timestamp_millis() + DEFAULT_CONTINUATION_TTL_MS);
    }

    #[tokio::test]
    async fn state_round_trips_and_updates_deadline() {
        let db = db().await;
        let initial = state();
        let json = condition_json(&initial);
        db.write(|conn| {
            let json = json.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO agent_waits(id,state,condition_json,deadline_ms) VALUES('w','pending',?1,NULL)",
                    params![json],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
        let mut changed = load(&db, "w").await.unwrap().unwrap();
        set_task(&mut changed, "task-1".into(), 1, 1_000, Some(5_000));
        assert!(store(&db, "w", &changed).await.unwrap());
        assert_eq!(load(&db, "w").await.unwrap(), Some(changed));
    }

    #[tokio::test]
    async fn recording_an_owned_answer_claims_it_once_while_ordinary_prompts_stay_unclaimed() {
        let db = db().await;
        let json = condition_json(&state());
        db.write(|conn| {
            let json = json.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO agent_waits(id,state,condition_json,deadline_ms) VALUES('w','pending',?1,NULL)",
                    params![json],
                )
                .await?;
                conn.execute("INSERT INTO prompts(id,mcp_wait_id) VALUES('owned','w')", ())
                    .await?;
                conn.execute(
                    "INSERT INTO prompts(id) VALUES('ordinary')",
                    (),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();

        let one = |text: &str| {
            PromptAnswers::new(
                1,
                vec![crate::models::question::QuestionAnswer {
                    index: 0,
                    answer: text.to_string(),
                }],
            )
            .unwrap()
        };
        let (yes, no, json_answer) = (one("yes"), one("no"), one(r#"{"approval":true}"#));
        let first = db
            .write(|conn| {
                let yes = yes.clone();
                Box::pin(async move { record_and_claim_answer_conn(conn, "owned", &yes, 10).await })
            })
            .await
            .unwrap();
        assert!(
            matches!(first, Some(AnswerClaim::Claimed { wait_id, answer }) if wait_id == "w" && answer == serde_json::json!("yes"))
        );
        // The stored value is the indexed form, and the MCP input read back
        // from it is the same single answer the claim reported.
        let stored = db
            .query_opt_text("SELECT response FROM prompts WHERE id='owned'", ())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored, yes.to_stored());
        assert_eq!(mcp_input_value(&stored), serde_json::json!("yes"));
        assert_eq!(
            mcp_input_value(&json_answer.to_stored()),
            serde_json::json!({"approval": true})
        );
        assert_eq!(
            mcp_input_value(r#"{"approval":true}"#),
            serde_json::json!({"approval": true}),
            "a legacy stored value is the single answer"
        );
        let duplicate = db
            .write(|conn| {
                let no = no.clone();
                Box::pin(async move { record_and_claim_answer_conn(conn, "owned", &no, 11).await })
            })
            .await
            .unwrap();
        assert_eq!(
            duplicate,
            Some(AnswerClaim::AlreadyConsumed {
                wait_id: "w".into()
            })
        );
        let ordinary = db
            .write(|conn| {
                let yes = yes.clone();
                Box::pin(
                    async move { record_and_claim_answer_conn(conn, "ordinary", &yes, 12).await },
                )
            })
            .await
            .unwrap();
        assert_eq!(ordinary, None);
    }

    #[tokio::test]
    async fn scheduler_selects_due_and_pending_operations_but_not_future_polls() {
        let db = db().await;
        let now = 10_000;
        let mut due = state();
        set_task(&mut due, "due".into(), 100, now - 200, Some(10_000));
        let mut future = state();
        set_task(&mut future, "future".into(), 5_000, now, Some(10_000));
        let mut operation = state();
        operation.pending_operation = Some(PendingOperation::ContinueTool {
            operation_id: "op".into(),
            input_responses: serde_json::json!({}),
            request_state: None,
        });
        for (id, value) in [("due", due), ("future", future), ("operation", operation)] {
            db.execute(
                "INSERT INTO agent_waits(id,state,condition_json,created_at) VALUES(?1,'pending','{}',1)",
                params![id],
            )
            .await
            .unwrap();
            assert!(store(&db, id, &value).await.unwrap());
        }

        let selected = due_wait_ids(&db, now).await.unwrap();
        assert!(selected.contains(&"due".to_string()), "{selected:?}");
        assert!(selected.contains(&"operation".to_string()), "{selected:?}");
        assert!(!selected.contains(&"future".to_string()), "{selected:?}");
    }

    #[test]
    fn mrtr_and_polling_are_bounded() {
        let mut value = state();
        for _ in 0..MAX_MRTR_ROUNDS {
            advance_mrtr(&mut value, None).unwrap();
        }
        assert!(advance_mrtr(&mut value, None).is_err());
        set_task(&mut value, "task".into(), 0, 100, Some(150));
        assert_eq!(value.next_poll_at_ms, Some(200));
        assert!(schedule_next_poll(&mut value, 200).is_err());
        clear_task(&mut value);
        assert!(value.task.is_none());
    }

    // ---- The due selection against the real schema -----------------------
}
