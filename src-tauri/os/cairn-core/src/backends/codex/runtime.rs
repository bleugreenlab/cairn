use super::permissions::{
    decline_codex_native_file_change, handle_codex_approval_request,
    handle_codex_mcp_server_elicitation_request,
};
use super::CodexBackend;
use crate::agent_process::completion::{SessionTurnCompletion, SessionTurnOutcome};
use crate::models::ContextTokenState;
use crate::orchestrator::lifecycle::{apply_codex_turn_completion, CodexTurnContext};
use crate::orchestrator::session::insert_error_event;
use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};
use cairn_models::codex::app_server::AppServerClient;
use cairn_models::codex::token_requests::{standing_refusal_notice, token_request_reply};
use cairn_models::codex::tokens::CodexAuthState;
use cairn_session::agent_process::completion::TurnFailureDisposition;
use serde_json::Value;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

pub(super) use cairn_session::backends::codex::runtime::*;
fn goal_job_id_for_run(db: &Arc<LocalDb>, run_id: &str) -> Result<Option<String>, String> {
    let run = crate::storage::run_db_blocking({
        let db = db.clone();
        let run_id = run_id.to_string();
        move || async move {
            db.read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query("SELECT job_id FROM runs WHERE id = ?1", (run_id.as_str(),))
                        .await?;
                    rows.next().await?.map(|row| row.opt_text(0)).transpose()
                })
            })
            .await
            .map_err(|error| error.to_string())
        }
    })?;
    run.ok_or_else(|| "native Codex turn has no Cairn run".to_string())
}

fn admit_native_goal_turn(
    orch: &Orchestrator,
    db: &Arc<LocalDb>,
    run_id: &str,
    session_id: Option<&str>,
    provider_turn_id: &str,
) -> Result<Option<crate::execution::jobs::CodexGoalTurnAdmission>, String> {
    let session_id =
        session_id.ok_or_else(|| "native Codex goal turn has no Cairn session".to_string())?;
    let Some(job_id) = goal_job_id_for_run(db, run_id)? else {
        // Project-level chats intentionally have no job and no provider goal.
        // Their ordinary turn/started notification belongs to the existing
        // nodeless lifecycle and must not be admitted as a goal successor.
        return Ok(None);
    };
    let launch_lock = orch.job_launch_lock(&job_id);
    let _launch_guard = launch_lock
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let current = orch.process_state.get_current_turn_id(run_id);
    let admission = crate::execution::jobs::admit_codex_goal_turn(
        orch,
        db.clone(),
        run_id,
        session_id,
        &job_id,
        provider_turn_id,
        current.as_deref(),
    )?;
    if let crate::execution::jobs::CodexGoalTurnAdmission::Admitted(turn_id) = &admission {
        if current.as_deref() != Some(turn_id.as_str())
            && !orch.process_state.begin_turn(run_id, turn_id)
        {
            return Err("native Codex goal turn lost its warm process".to_string());
        }
    } else if matches!(
        admission,
        crate::execution::jobs::CodexGoalTurnAdmission::Deferred { .. }
    ) {
        crate::backends::stdin::defer_codex_goal_turn(
            &orch.process_state,
            run_id,
            provider_turn_id,
            crate::agent_process::process::PauseIntent::Reassert,
        )?;
    }

    Ok(Some(admission))
}

fn capacity_retry_target(
    db: &Arc<LocalDb>,
    run_id: &str,
    orch: &Orchestrator,
) -> Result<Option<CapacityRetryTarget>, String> {
    let cairn_turn_id = orch
        .process_state
        .get_current_turn_id(run_id)
        .ok_or_else(|| "capacity failure has no active Cairn turn".to_string())?;
    crate::storage::run_db_blocking({
        let db = db.clone();
        let run_id = run_id.to_string();
        let cairn_turn_id = cairn_turn_id.clone();
        move || async move {
            db.read(|conn| {
                let run_id = run_id.clone();
                let cairn_turn_id = cairn_turn_id.clone();
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT j.id, j.current_session_id, j.current_turn_id
                               FROM runs r
                               JOIN jobs j ON j.id = r.job_id
                              WHERE r.id = ?1
                                AND COALESCE(j.agent_config_id, '') != 'workflow'
                              LIMIT 1",
                            (run_id.as_str(),),
                        )
                        .await?;
                    let Some(row) = rows.next().await? else {
                        return Ok(None);
                    };
                    let job_id = row.text(0)?;
                    let session_id = row.opt_text(1)?;
                    let head_turn_id = row.opt_text(2)?;
                    if head_turn_id.as_deref() != Some(cairn_turn_id.as_str()) {
                        return Ok(None);
                    }
                    Ok(session_id.map(|session_id| CapacityRetryTarget {
                        job_id,
                        session_id,
                        cairn_turn_id,
                    }))
                })
            })
            .await
            .map_err(|error| error.to_string())
        }
    })
}

fn handle_selected_model_capacity_failure(
    orch: &Orchestrator,
    run_db: &Arc<LocalDb>,
    run_id: &str,
    session_id: Option<&str>,
) -> bool {
    let target = match capacity_retry_target(run_db, run_id, orch) {
        Ok(Some(target)) => target,
        Ok(None) => return false,
        Err(error) => {
            log::warn!(
                "Unable to prepare Codex capacity retry for run {}: {}",
                run_id,
                error
            );
            return false;
        }
    };
    let completed_retries = match crate::execution::jobs::consecutive_retry_turn_count(
        run_db.clone(),
        &target.cairn_turn_id,
    ) {
        Ok(count) => count,
        Err(error) => {
            log::warn!(
                "Unable to count Codex capacity retries for run {}: {}",
                run_id,
                error
            );
            crate::orchestrator::lifecycle::fail_run(orch, run_id, "turn_failed");
            return true;
        }
    };
    let next_attempt = completed_retries + 1;
    if next_attempt > CAPACITY_RETRY_DELAYS.len() as u32 {
        if hold_at_capacity(orch, run_db, run_id, session_id, &target) {
            return true;
        }
        insert_error_event(
            orch,
            run_id,
            session_id,
            "The selected Codex model remained at capacity after three automatic retries. You can continue this job manually to try again.",
        );
        settle_capacity_failed_turn(orch, run_db, run_id, "turn_failed");
        return true;
    }

    settle_capacity_failed_turn(orch, run_db, run_id, "capacity_retry");
    let retry_turn_id = match crate::execution::jobs::claim_retry_successor_if_head_matches(
        orch,
        run_db.clone(),
        &target.job_id,
        &target.session_id,
        &target.cairn_turn_id,
    ) {
        Ok(Some(turn_id)) => turn_id,
        Ok(None) => return true,
        Err(error) => {
            log::warn!(
                "Failed to claim Codex capacity retry successor for run {}: {}",
                run_id,
                error
            );
            return true;
        }
    };
    let delay = CAPACITY_RETRY_DELAYS[(next_attempt - 1) as usize];
    let delay_seconds = delay.as_secs();
    let status = format!(
        "Codex is at capacity. Retrying in {} seconds ({}/3).",
        delay_seconds, next_attempt
    );
    if let Err(error) = crate::messages::transcript::insert_system_message_sync(
        orch,
        run_id,
        session_id,
        Some(&target.cairn_turn_id),
        &status,
        serde_json::json!({
            "provider": "codex",
            "kind": "selected_model_capacity_retry",
            "attempt": next_attempt,
            "maxAttempts": CAPACITY_RETRY_DELAYS.len(),
            "delaySeconds": delay_seconds,
        }),
    ) {
        log::warn!(
            "Failed to persist Codex capacity retry status for run {}: {}",
            run_id,
            error
        );
    }

    let orch = orch.clone();
    let retry_db = run_db.clone();
    let job_id = target.job_id;
    let retry_job_id = job_id.clone();
    let retry_turn_for_thread = retry_turn_id.clone();
    if let Err(error) = thread::Builder::new()
        .name(format!("codex-capacity-retry-{next_attempt}"))
        .spawn(move || {
            thread::sleep(delay);
            if let Err(error) = crate::execution::jobs::continue_automatic_retry(
                &orch,
                &retry_job_id,
                &retry_turn_for_thread,
            ) {
                log::warn!(
                    "Codex capacity retry for job {} did not launch: {}",
                    retry_job_id,
                    error
                );
                if let Err(cleanup_error) =
                    crate::execution::jobs::abandon_pending_retry_if_head_matches(
                        retry_db,
                        &retry_job_id,
                        &retry_turn_for_thread,
                    )
                {
                    log::warn!(
                        "Failed to release unstarted Codex retry turn {}: {}",
                        retry_turn_for_thread,
                        cleanup_error
                    );
                }
            }
        })
    {
        log::warn!(
            "Failed to schedule Codex capacity retry for job {}: {}",
            job_id,
            error
        );
        if let Err(cleanup_error) = crate::execution::jobs::abandon_pending_retry_if_head_matches(
            run_db.clone(),
            &job_id,
            &retry_turn_id,
        ) {
            log::warn!(
                "Failed to release unscheduled Codex retry turn {}: {}",
                retry_turn_id,
                cleanup_error
            );
        }
    }
    true
}

/// Hold a turn whose model stayed at capacity through every in-session retry,
/// rather than failing it: at capacity is a want of supply, which waiting
/// cures. The turn yields as a provider limit with no reset time, because
/// Codex declares none and none is inferred, so the hold re-checks on its
/// bounded clock and any change to the accounts asks sooner. The process stays
/// warm for the resume. Switching an open session to another offer is its own
/// lifecycle operation (cairn/4693), so the hold waits for this model.
///
/// Returns `false` when the hold could not be armed; the caller then fails the
/// turn as before.
fn hold_at_capacity(
    orch: &Orchestrator,
    run_db: &Arc<LocalDb>,
    run_id: &str,
    session_id: Option<&str>,
    target: &CapacityRetryTarget,
) -> bool {
    let refusal = cairn_models::supply::CapacityRefusal {
        subject: "the selected Codex model".to_string(),
        reason: "The selected Codex model remained at capacity after three automatic retries"
            .to_string(),
        retry_at: None,
        retry_account: None,
    };
    let observation = crate::orchestrator::wakes::ProviderLimitObservation {
        job_id: target.job_id.clone(),
        run_id: run_id.to_string(),
        turn_id: target.cairn_turn_id.clone(),
        backend: super::CODEX_BACKEND_NAME.to_string(),
        account_id: orch.process_state.get_account_id(run_id),
        retry_not_before: refusal.retry_at,
        boundary_source: "unknown".to_string(),
        observed_text: Some(refusal.reason.clone()),
        resume_gate: None,
        boundary_account_id: None,
        now: chrono::Utc::now().timestamp(),
    };
    let armed = crate::storage::run_db_blocking({
        let db = run_db.clone();
        move || async move {
            crate::orchestrator::wakes::yield_and_arm_provider_limit_hold(&db, observation).await
        }
    });
    let hold = match armed {
        Ok(hold) => hold,
        Err(error) => {
            log::warn!("Could not hold run {run_id} at Codex capacity: {error}");
            return false;
        }
    };
    crate::orchestrator::lifecycle::warm_provider_held_run(orch, run_id);
    crate::backends::codex::emit_codex_run_turn_completed(&orch.services.emitter, run_id);
    if hold.observed_count == 1 {
        let note = format!(
            "{}. The node holds and re-checks every {} minutes, or sooner if an account changes; continue it to retry now.",
            refusal.reason,
            crate::orchestrator::wakes::SUPPLY_RECHECK_SECS / 60
        );
        if let Err(error) = crate::messages::transcript::insert_system_message_sync(
            orch,
            run_id,
            session_id,
            Some(&target.cairn_turn_id),
            &note,
            serde_json::json!({
                "provider": "codex",
                "kind": "selected_model_capacity_hold",
                "holdId": hold.id,
            }),
        ) {
            log::warn!("Failed to record the Codex capacity hold for run {run_id}: {error}");
        }
    }
    true
}

/// End the turn the provider refused for capacity.
///
/// A capacity refusal says nothing about the process, so a top-level or recipe
/// job keeps its live run and native conversation and only the turn ends, as
/// `provider_failure`. A delegated child or workflow goes through `fail_run`,
/// which records `reason`: once its retries are spent that is what tells the
/// waiting parent, and while they remain the retry successor claimed next keeps
/// the child's job running. A run this host holds no process for has nothing to
/// keep and takes the same path.
fn settle_capacity_failed_turn(
    orch: &Orchestrator,
    run_db: &Arc<LocalDb>,
    run_id: &str,
    reason: &str,
) {
    let exhausted = reason == "turn_failed";
    let delegated = crate::backends::is_task_spawned_run(super::CODEX_BACKEND_NAME, run_db, run_id);
    if !(exhausted && delegated)
        && crate::orchestrator::lifecycle::settle_provider_turn_failure(orch, run_id)
    {
        crate::backends::codex::emit_codex_run_turn_completed(&orch.services.emitter, run_id);
        // With retries left, the retry claimed next drains whatever is queued.
        // Once they are spent nothing else will, so the turn end flushes it.
        if exhausted {
            if let Err(error) =
                crate::messages::delivery::flush_pending_directs_on_idle(orch, run_id)
            {
                log::warn!("capacity-exhausted queued delivery flush failed: {error}");
            }
        }
        return;
    }
    crate::orchestrator::lifecycle::fail_run(orch, run_id, reason);
}

struct CapacityRetryTarget {
    job_id: String,
    session_id: String,
    cairn_turn_id: String,
}

const CAPACITY_RETRY_DELAYS: [Duration; 3] = [
    Duration::from_secs(2),
    Duration::from_secs(8),
    Duration::from_secs(30),
];
impl CodexBackend {
    /// What a Codex session answers its app-server through: its reader's
    /// host and, on a session's own server, its router's
    /// ([`CodexRuntime::own_server`]).
    pub(super) fn session_runtime(
        orch: &Orchestrator,
        run_id: &str,
        session_id: Option<String>,
        run_db: Arc<LocalDb>,
        client: Arc<AppServerClient>,
        oauth_state: Option<Arc<Mutex<CodexAuthState>>>,
        cleanup: Option<Arc<super::pool::PooledCall>>,
    ) -> CodexRuntime {
        codex_runtime(
            super::super::transcript_host::SessionTranscriptHost {
                orch: orch.clone(),
                emitter: orch.services.emitter.clone(),
                run_id: run_id.to_string(),
                session_id,
                run_db,
            },
            Some(client),
            oauth_state,
            cleanup,
        )
    }

    /// A dedicated session's reader over `notifications`, run to the end on
    /// this thread.
    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn reader_thread_app_server(
        orch: &Orchestrator,
        emitter: &Arc<dyn crate::services::EventEmitter>,
        run_id: &str,
        session_id: Option<String>,
        notifications: crossbeam_channel::Receiver<Value>,
        client: Arc<AppServerClient>,
        current_turn_id: Arc<Mutex<Option<String>>>,
        oauth_state: Option<Arc<Mutex<CodexAuthState>>>,
        backend_key: String,
        run_db: Arc<LocalDb>,
        cleanup: Option<Arc<super::pool::PooledCall>>,
    ) {
        let ephemeral = cleanup.is_some();
        let host = Self::session_runtime(
            orch,
            run_id,
            session_id.clone(),
            run_db.clone(),
            client,
            oauth_state,
            cleanup,
        );
        CodexRuntime::reader_thread_app_server(
            &host,
            emitter,
            run_id,
            session_id,
            notifications,
            current_turn_id,
            backend_key,
            run_db,
            ephemeral,
        );
    }
}
pub(super) fn codex_runtime(
    session: super::super::transcript_host::SessionTranscriptHost,
    client: Option<Arc<AppServerClient>>,
    oauth_state: Option<Arc<Mutex<CodexAuthState>>>,
    cleanup: Option<Arc<super::pool::PooledCall>>,
) -> CodexRuntime {
    CodexRuntime {
        process_state: session.orch.process_state.clone(),
        boot_id: session.orch.boot_at.to_string(),
        host: Arc::new(CodexReaderHost {
            session,
            client,
            oauth_state,
            cleanup: Mutex::new(cleanup),
        }),
    }
}
const UNSUPPORTED_REQUEST: &str = "Unsupported request";

/// How Cairn answers one Codex server request. Every method in
/// `SUPPORTED_SERVER_REQUESTS` except the token refresh (which the session
/// runtime answers before a request reaches here) routes to a handler; anything
/// else is refused with `-32601`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ServerRequestRoute {
    Approval {
        kind: &'static str,
        command_execution: bool,
    },
    DeclineNativeFileChange,
    McpElicitation,
    Unsupported(&'static str),
}

fn route_server_request(method: &str) -> ServerRequestRoute {
    let approval = |kind, command_execution| ServerRequestRoute::Approval {
        kind,
        command_execution,
    };
    match method {
        "item/commandExecution/requestApproval" => approval("codex/command_execution", true),
        "item/fileChange/requestApproval" => ServerRequestRoute::DeclineNativeFileChange,
        "item/fileRead/requestApproval" => approval("codex/file_read", false),
        "item/mcpToolCall/requestApproval" => approval("codex/mcp_tool_call", false),
        "item/permissions/requestApproval" => approval("codex/permissions", false),
        "mcpServer/elicitation/request" => ServerRequestRoute::McpElicitation,
        "item/tool/call" => ServerRequestRoute::Unsupported("Dynamic tool calls are not supported"),
        "item/tool/requestUserInput" | "tool/requestUserInput" => {
            ServerRequestRoute::Unsupported("Interactive prompts are not supported")
        }
        _ => ServerRequestRoute::Unsupported(UNSUPPORTED_REQUEST),
    }
}

struct CodexReaderHost {
    session: super::super::transcript_host::SessionTranscriptHost,
    client: Option<Arc<AppServerClient>>,
    oauth_state: Option<Arc<Mutex<CodexAuthState>>>,
    cleanup: Mutex<Option<Arc<super::pool::PooledCall>>>,
}
impl CodexReaderHost {
    /// Cairn's notice for a token request it refused this session, while the
    /// refusal stands. A dedicated session answers its own token requests; a
    /// pooled call's are answered by its pool.
    fn standing_token_refusal(&self) -> Option<String> {
        if self.oauth_state.is_some() {
            return standing_refusal_notice(self.oauth_state.as_ref());
        }
        let cleanup = self.cleanup.lock().ok()?;
        cleanup.as_ref()?.standing_token_refusal()
    }
}
impl CodexHost for CodexReaderHost {
    fn transcripts(&self) -> &dyn cairn_session::transcripts::TranscriptHost {
        &self.session
    }
    fn error(&self, message: &str) {
        insert_error_event(
            &self.session.orch,
            &self.session.run_id,
            self.session.session_id.as_deref(),
            message,
        );
    }
    fn server_request(
        &self,
        method: Option<&str>,
        msg: &Value,
        id_value: &Value,
    ) -> Result<(), String> {
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| "Codex reader has no transport".to_string())?;
        let orch = &self.session.orch;
        let run_db = self.session.run_db.clone();
        let run_id = self.session.run_id.as_str();
        let Some(method) = method else {
            return Ok(());
        };
        let params = || msg.get("params").cloned().unwrap_or(Value::Null);
        match route_server_request(method) {
            ServerRequestRoute::Approval {
                kind,
                command_execution,
            } => handle_codex_approval_request(
                orch,
                &run_db,
                run_id,
                kind,
                params(),
                client.as_ref(),
                id_value,
                command_execution,
            ),
            ServerRequestRoute::DeclineNativeFileChange => {
                decline_codex_native_file_change(client.as_ref(), id_value)
            }
            ServerRequestRoute::McpElicitation => handle_codex_mcp_server_elicitation_request(
                orch,
                &run_db,
                run_id,
                params(),
                client.as_ref(),
                id_value,
            ),
            ServerRequestRoute::Unsupported(reason) => {
                if reason == UNSUPPORTED_REQUEST {
                    log::warn!("Unhandled Codex server request: {}", method);
                } else {
                    log::debug!("Codex sent {method}: declining");
                }
                client.respond_error(id_value, -32601, reason)
            }
        }
    }
    fn refresh_tokens(&self, id_value: &Value) -> Result<(), String> {
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| "Codex reader has no transport".to_string())?;
        let orch = &self.session.orch;
        client.reply(
            id_value,
            token_request_reply(orch, orch.subscriptions(), self.oauth_state.as_ref()),
        )
    }
    fn capture_output(&self, value: Value) {
        let orch = &self.session.orch;
        let run_id = self.session.run_id.as_str();
        let orch_capture = orch.clone();
        let run_id_capture = run_id.to_string();
        if let Err(e) =
            crate::backends::run_state::run_backend_db(super::CODEX_BACKEND_NAME, async move {
                crate::mcp::handlers::comments_artifacts::capture_call_structured_output(
                    &orch_capture,
                    &run_id_capture,
                    value,
                )
                .await
            })
        {
            log::warn!(
                "Failed to capture Codex structured output for run {}: {}",
                run_id,
                e
            );
        }
    }
    fn account_usage(&self, snapshot: cairn_models::types::usage::ProviderUsageSnapshot) {
        let oauth_state = &self.oauth_state;
        let orch = &self.session.orch;
        // Attribute the update to the subscription that spent
        // the tokens. The usage panel reads per (backend,
        // account), and routing needs a measured window per
        // account rather than one global number, so a session
        // running is itself a usage probe for its own account.
        let account_id = oauth_state
            .as_ref()
            .and_then(|state| state.lock().ok())
            .and_then(|guard| guard.cairn_account_id());
        if let Some(account_id) = account_id.as_deref() {
            // An exhausted window blocks the account only until
            // it rolls over, which is what its reset time says.
            let blocked_until = snapshot
                .windows
                .iter()
                .filter(|window| window.remaining_percent <= 0.0)
                .filter_map(|window| window.resets_at)
                .max()
                .map(crate::credentials::AccountBlock::Declared);
            if let Err(error) =
                orch.record_account_health(account_id, snapshot.windows.clone(), blocked_until)
            {
                log::warn!("Failed to record Codex account health: {error}");
            }
        }
        orch.store_provider_account_usage_snapshot(account_id, snapshot);
    }
    fn context_snapshot(&self, snapshot: ContextTokenState) {
        self.session.orch.store_context_token_snapshot(snapshot);
    }
    fn admit_goal_turn(&self, turn_id: &str) -> Result<Option<GoalTurnAdmission>, String> {
        admit_native_goal_turn(
            &self.session.orch,
            &self.session.run_db,
            &self.session.run_id,
            self.session.session_id.as_deref(),
            turn_id,
        )
        .map(|outcome| {
            outcome.map(|outcome| match outcome {
                crate::execution::jobs::CodexGoalTurnAdmission::Admitted(_) => {
                    GoalTurnAdmission::Admitted
                }
                crate::execution::jobs::CodexGoalTurnAdmission::Deferred {
                    predecessor_turn_id,
                } => GoalTurnAdmission::Deferred {
                    predecessor_turn_id,
                },
            })
        })
    }
    /// The terminal-artifact handoff: the agent wrote its output, so the goal
    /// must not continue past it. It lifts when the job next runs.
    fn interrupt(&self) -> Result<(), String> {
        crate::backends::stdin::send_interrupt(
            &self.session.orch.process_state,
            &self.session.run_id,
            crate::agent_process::process::PauseIntent::Stake(
                crate::agent_process::process::GoalPause::Interrupted,
            ),
        )
    }
    fn artifact_handoff(&self) {
        crate::orchestrator::lifecycle::transition_to_warm_state(
            &self.session.orch,
            &self.session.run_id,
            None,
        );
    }
    fn watchdog_arm_failed(&self) -> Result<(), String> {
        crate::orchestrator::lifecycle::kill_session_with_reason(
            &self.session.orch,
            &self.session.run_id,
            crate::orchestrator::lifecycle::WATCHDOG_ARM_FAILED_EXIT_REASON,
        )
    }
    fn recover_watchdog(
        &self,
        identity: &crate::runs::watchdog_ledger::WatchdogIdentity,
        now: i64,
    ) -> Result<(), String> {
        crate::orchestrator::lifecycle::recover_provider_watchdog(&self.session.orch, identity, now)
            .map(|_| ())
    }
    fn capacity_failure(&self) -> bool {
        handle_selected_model_capacity_failure(
            &self.session.orch,
            &self.session.run_db,
            &self.session.run_id,
            self.session.session_id.as_deref(),
        )
    }
    fn complete(&self, completion: SessionTurnCompletion, context: CodexTurnContext) {
        // Codex reports a turn that ran out of tokens in its own words (a
        // routing or auth error) and drops the reason Cairn gave it, so the
        // run is told that reason beside Codex's. A failure another path
        // already took over (a capacity retry) is not this turn's to explain.
        if matches!(completion.outcome, SessionTurnOutcome::Failed { .. })
            && context.failure_disposition != TurnFailureDisposition::AlreadyHandled
        {
            if let Some(notice) = self.standing_token_refusal() {
                self.error(&notice);
            }
        }
        apply_codex_turn_completion(
            &self.session.orch,
            &self.session.run_db,
            &self.session.emitter,
            &self.session.run_id,
            self.session.session_id.as_deref(),
            completion,
            context,
        );
    }

    fn reader_closed(&self) {
        if let Some(cleanup) = self.cleanup.lock().unwrap().take() {
            cleanup.release();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_models::wire::codex_app_server::{
        Notification, AUTH_TOKENS_REFRESH_METHOD, SUPPORTED_SERVER_REQUESTS,
    };

    #[test]
    fn every_supported_server_request_routes_to_a_real_handler() {
        for method in SUPPORTED_SERVER_REQUESTS {
            if *method == AUTH_TOKENS_REFRESH_METHOD {
                let msg = json!({"id": 7, "method": method, "params": {}});
                assert!(
                    matches!(
                        Notification::parse(&msg),
                        Notification::AuthTokensRefreshRequest { .. }
                    ),
                    "{method} must reach the session runtime's token handler"
                );
                continue;
            }
            assert!(
                !matches!(
                    route_server_request(method),
                    ServerRequestRoute::Unsupported(_)
                ),
                "{method} is listed as supported but is declined with -32601"
            );
        }
    }

    #[test]
    fn unlisted_server_requests_are_refused_as_unsupported() {
        for method in [
            "item/tool/call",
            "tool/requestUserInput",
            "brand/new/request",
        ] {
            assert!(!SUPPORTED_SERVER_REQUESTS.contains(&method));
            assert!(matches!(
                route_server_request(method),
                ServerRequestRoute::Unsupported(_)
            ));
        }
        assert_eq!(
            route_server_request("brand/new/request"),
            ServerRequestRoute::Unsupported(UNSUPPORTED_REQUEST)
        );
    }
    use crate::agent_process::process::RunHandle;
    use crate::db::DbState;
    use crate::orchestrator::OrchestratorBuilder;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::SearchIndex;
    use serde_json::json;

    fn scripted_test_orchestrator(db: Arc<LocalDb>) -> Orchestrator {
        let root = tempfile::tempdir().unwrap().keep();
        let state = Arc::new(DbState::new(
            db,
            Arc::new(SearchIndex::open_or_create(root.join("search.db")).unwrap()),
        ));
        OrchestratorBuilder::new(state, Arc::new(TestServicesBuilder::new().build()), root).build()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn terminal_notifications_flush_buffered_text_before_the_result_and_turn_transition() {
        for terminal in [
            json!({"method":"turn/completed","params":{"turn":{"id":"p1","status":"completed"}}}),
            json!({"method":"turn/aborted","params":{"turnId":"p1","reason":"interrupted"}}),
        ] {
            let db = Arc::new(crate::storage::migrated_test_db("completion-stream-order").await);
            db.execute_script(
                "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
                 INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES('p','w','P','p','/tmp/p',1,1);
                 INSERT INTO jobs(id,project_id,status,node_name,created_at,updated_at) VALUES('j','p','running','builder',1,1);
                 INSERT INTO sessions(id,job_id,backend,backend_id,status,sequence,created_at,updated_at) VALUES('s','j','codex','native-thread','open',1,1,1);
                 INSERT INTO runs(id,project_id,job_id,status,session_id,created_at,updated_at) VALUES('r','p','j','live','s',1,1);
                 INSERT INTO turns(id,session_id,run_id,job_id,sequence,state,start_reason,created_at,updated_at) VALUES('t','s','r','j',1,'running','initial',1,1);
                 UPDATE jobs SET current_turn_id='t',current_session_id='s' WHERE id='j';",
            ).await.unwrap();
            let orch = scripted_test_orchestrator(db.clone());
            let mut handle = RunHandle::new(
                Arc::new(Mutex::new(None)),
                Arc::new(Mutex::new(Some(
                    cairn_models::session::claude::ClaudeControl::boxed(Box::new(Vec::<u8>::new())),
                ))),
                Some("s".into()),
                Some("j".into()),
            );
            handle.begin_turn("t");
            orch.process_state
                .processes
                .lock()
                .unwrap()
                .register("r".into(), handle);
            orch.process_state.set_current_turn_id("r", Some("t"));
            let client = Arc::new(AppServerClient::for_test_scripted(
                vec![],
                Arc::new(Mutex::new(Vec::new())),
            ));
            let (tx, rx) = crossbeam_channel::unbounded();
            // No item/completed notification: the terminal boundary must flush
            // even an unfinished accumulator before completing the Cairn turn.
            tx.send(json!({"method":"item/agentMessage/delta","params":{"turnId":"p1","delta":"buffered tail"}})).unwrap();
            tx.send(terminal).unwrap();
            drop(tx);
            CodexBackend::reader_thread_app_server(
                &orch,
                &orch.services.emitter,
                "r",
                Some("s".into()),
                rx,
                client,
                Arc::new(Mutex::new(Some("p1".into()))),
                None,
                "codex".into(),
                db.clone(),
                None,
            );
            let content = db
                .query_opt_text("SELECT data FROM events WHERE event_type='assistant'", ())
                .await
                .unwrap()
                .unwrap();
            assert!(content.contains("buffered tail"));
            assert_eq!(
                db.query_opt_text(
                    "SELECT turn_id FROM events WHERE event_type='assistant'",
                    ()
                )
                .await
                .unwrap()
                .as_deref(),
                Some("t")
            );
            assert_eq!(
                db.query_opt_text("SELECT state FROM turns WHERE id='t'", ())
                    .await
                    .unwrap()
                    .as_deref(),
                Some("complete")
            );
            // This also catches a flush postponed until reader teardown, after
            // the host has already persisted its success result.
            assert_eq!(db.query_opt(
                "SELECT COUNT(*) FROM events assistant JOIN events result ON assistant.run_id=result.run_id WHERE assistant.event_type='assistant' AND result.event_type='result:success' AND assistant.sequence > result.sequence",
                (), |row| row.i64(0),
            ).await.unwrap(), Some(0));
        }
    }

    /// An orchestrator whose Codex refresh authority can no longer renew
    /// anything, and a session on Cairn account `acct-cairn` holding `at-1`.
    fn refusing_orchestrator(db: Arc<LocalDb>) -> (Orchestrator, Arc<Mutex<CodexAuthState>>) {
        let root = tempfile::tempdir().unwrap().keep();
        let state = Arc::new(DbState::new(
            db,
            Arc::new(SearchIndex::open_or_create(root.join("search.db")).unwrap()),
        ));
        let orch =
            OrchestratorBuilder::new(state, Arc::new(TestServicesBuilder::new().build()), root)
                .subscriptions(Arc::new(
                    cairn_models::subscription::testing::RenewingCodex::refusing(),
                ))
                .build();
        let login = json!({"tokens": {
            "id_token": "id-1",
            "access_token": "at-1",
            "account_id": "chatgpt-1",
        }});
        let auth = CodexAuthState::new_for_account(
            &orch,
            &login.to_string(),
            Some("acct-cairn".to_string()),
        )
        .unwrap();
        (orch, Arc::new(Mutex::new(auth)))
    }

    /// Codex drops the message a refused token request carries and fails the
    /// turn in its own words; the run must still hear Cairn's.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_turn_failing_after_a_refused_token_request_reports_the_refusal() {
        let db = Arc::new(crate::storage::migrated_test_db("codex-refused-token-request").await);
        db.execute_script(
            "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES('p','w','P','p','/tmp/p',1,1);
             INSERT INTO jobs(id,project_id,status,node_name,created_at,updated_at) VALUES('j','p','running','builder',1,1);
             INSERT INTO sessions(id,job_id,backend,backend_id,status,sequence,created_at,updated_at) VALUES('s','j','codex','native-thread','open',1,1,1);
             INSERT INTO runs(id,project_id,job_id,status,session_id,created_at,updated_at) VALUES('r','p','j','live','s',1,1);
             INSERT INTO turns(id,session_id,run_id,job_id,sequence,state,start_reason,created_at,updated_at) VALUES('t','s','r','j',1,'running','initial',1,1);
             UPDATE jobs SET current_turn_id='t',current_session_id='s' WHERE id='j';",
        )
        .await
        .unwrap();
        let (orch, auth) = refusing_orchestrator(db.clone());
        let mut handle = RunHandle::new(
            Arc::new(Mutex::new(None)),
            Arc::new(Mutex::new(Some(
                cairn_models::session::claude::ClaudeControl::boxed(Box::new(Vec::<u8>::new())),
            ))),
            Some("s".into()),
            Some("j".into()),
        );
        handle.begin_turn("t");
        orch.process_state
            .processes
            .lock()
            .unwrap()
            .register("r".into(), handle);
        orch.process_state.set_current_turn_id("r", Some("t"));
        let sent = Arc::new(Mutex::new(Vec::new()));
        let client = Arc::new(AppServerClient::for_test_scripted(vec![], sent.clone()));
        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(json!({"id": 41, "method": "account/chatgptAuthTokens/refresh", "params": {"reason": "unauthorized"}})).unwrap();
        tx.send(json!({"method": "error", "params": {
            "turnId": "p1",
            "error": {"message": "workspace routing discovery unauthorized (401)"},
            "willRetry": false,
        }}))
        .unwrap();
        drop(tx);

        CodexBackend::reader_thread_app_server(
            &orch,
            &orch.services.emitter,
            "r",
            Some("s".into()),
            rx,
            client,
            Arc::new(Mutex::new(Some("p1".into()))),
            Some(auth),
            "codex".into(),
            db.clone(),
            None,
        );

        // What Codex was sent is the refusal, unchanged.
        let reply = sent.lock().unwrap()[0].clone();
        assert_eq!(reply["id"], 41);
        assert_eq!(
            reply["error"]["code"],
            cairn_models::codex::token_requests::TOKEN_REQUEST_REFUSED
        );
        let refusal = reply["error"]["message"].as_str().unwrap().to_string();
        assert!(refusal.contains("acct-cairn"), "{refusal}");

        let errors = db
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT json_extract(data,'$.content') FROM events WHERE event_type='system:error' ORDER BY sequence",
                            (),
                        )
                        .await?;
                    let mut errors = Vec::new();
                    while let Some(row) = rows.next().await? {
                        errors.push(row.text(0)?);
                    }
                    Ok(errors)
                })
            })
            .await
            .unwrap();
        assert!(
            errors
                .iter()
                .any(|error| error.contains("workspace routing discovery unauthorized")),
            "Codex's own error is kept: {errors:?}"
        );
        assert!(
            errors.iter().any(|error| error.contains(&refusal)),
            "the run hears Cairn's refusal: {errors:?}"
        );
        assert!(
            errors.iter().all(|error| !error.contains("at-1")),
            "{errors:?}"
        );
    }

    /// A pooled call's token requests are answered by its pool, which keeps
    /// what it refused for the calls it serves.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_pool_remembers_the_token_request_it_refused() {
        let db = Arc::new(crate::storage::migrated_test_db("codex-pool-refused-token").await);
        let (orch, auth) = refusing_orchestrator(db);
        let host = super::super::pool::pool_host(&orch, Some(auth));
        let sent = Arc::new(Mutex::new(Vec::new()));
        let client = AppServerClient::for_test_scripted(vec![], sent.clone());
        assert_eq!(host.standing_token_refusal(), None);

        host.refresh_tokens(&client, &json!(7));

        let refusal = sent.lock().unwrap()[0]["error"]["message"]
            .as_str()
            .unwrap()
            .to_string();
        let notice = host.standing_token_refusal().expect("the refusal stands");
        assert!(notice.contains(&refusal), "{notice}");
    }

    /// The interleaving that crashed fresh launches while the transport published
    /// a launch turn before persisting it: the provider's first turn reaches the
    /// real reader while the published turn is not yet started. The identity
    /// checks must still refuse it, and the rejection must survive the process
    /// saying exactly why.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_provider_turn_against_an_unstarted_launch_turn_is_refused_and_says_why() {
        for (bound_run, expected, blameless) in [
            (
                "NULL",
                "Codex provider turn conflicts with active Cairn turn t: run disagrees (turn: none, launch: r)",
                ["session disagrees", "job disagrees", "neither running"],
            ),
            (
                "'r'",
                "Codex provider turn conflicts with active Cairn turn t: turn is pending, neither running nor terminal",
                ["session disagrees", "job disagrees", "run disagrees"],
            ),
        ] {
            let db = Arc::new(crate::storage::migrated_test_db("codex-unstarted-launch-turn").await);
            db.execute_script(format!(
                "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
                 INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES('p','w','P','p','/tmp/p',1,1);
                 INSERT INTO jobs(id,project_id,status,node_name,created_at,updated_at) VALUES('j','p','running','Review',1,1);
                 INSERT INTO sessions(id,job_id,backend,status,sequence,created_at,updated_at) VALUES('s','j','codex','open',1,1,1);
                 INSERT INTO runs(id,project_id,job_id,status,session_id,start_mode,created_at,updated_at) VALUES('r','p','j','live','s','fresh',1,1);
                 INSERT INTO turns(id,session_id,run_id,job_id,sequence,state,start_reason,created_at,updated_at) VALUES('t','s',{bound_run},'j',1,'pending','initial',1,1);
                 UPDATE jobs SET current_turn_id='t',current_session_id='s' WHERE id='j';"
            ))
            .await
            .unwrap();
            let orch = scripted_test_orchestrator(db.clone());
            orch.process_state
                .register_process("r".into(), RunHandle::test_handle(Some("s"), Some("j")))
                .unwrap();
            orch.process_state.set_current_turn_id("r", Some("t"));
            let (tx, rx) = crossbeam_channel::unbounded();
            tx.send(json!({"method":"turn/started","params":{"turn":{"id":"p1"}}}))
                .unwrap();
            drop(tx);

            CodexBackend::reader_thread_app_server(
                &orch,
                &orch.services.emitter,
                "r",
                Some("s".into()),
                rx,
                Arc::new(AppServerClient::for_test_scripted(
                    vec![],
                    Arc::new(Mutex::new(Vec::new())),
                )),
                Arc::new(Mutex::new(None)),
                None,
                "codex".into(),
                db.clone(),
                None,
            );

            let error = db
                .query_opt_text(
                    "SELECT json_extract(data,'$.content') FROM events WHERE event_type='system:error'",
                    (),
                )
                .await
                .unwrap()
                .expect("the rejection is stored as the run's error");
            assert!(error.contains(expected), "{error}");
            for field in blameless {
                assert!(!error.contains(field), "{error}");
            }
            assert_eq!(
                db.query_opt_text("SELECT exit_reason FROM runs WHERE id='r'", ())
                    .await
                    .unwrap()
                    .as_deref(),
                Some(crate::orchestrator::lifecycle::WATCHDOG_ARM_FAILED_EXIT_REASON)
            );
            assert_eq!(
                db.query_opt_i64(
                    "SELECT COUNT(*) FROM events WHERE event_type='system:codex_goal_turn_started'",
                    (),
                )
                .await
                .unwrap(),
                Some(0),
                "a refused turn binds nothing"
            );
        }
    }

    /// A Codex job mid-turn on a warm persistent session: job `j` (shaped by
    /// `job_sql`), session `s` on native thread `native-thread`, live run `r`
    /// serving Cairn turn `t`, which maps to provider turn `p1`.
    async fn persistent_codex_turn(name: &str, job_sql: &str) -> (Arc<LocalDb>, Orchestrator) {
        let db = Arc::new(crate::storage::migrated_test_db(name).await);
        db.execute_script(format!(
            "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES('p','w','P','p','/tmp/p',1,1);
             INSERT INTO jobs(id,project_id,status,node_name,created_at,updated_at) VALUES('pj','p','running','parent',1,1);
             INSERT INTO jobs(id,project_id,status,node_name,created_at,updated_at) VALUES('j','p','running','planner',1,1);
             {job_sql}
             INSERT INTO sessions(id,job_id,backend,backend_id,status,sequence,created_at,updated_at) VALUES('s','j','codex','native-thread','open',1,1,1);
             INSERT INTO runs(id,project_id,job_id,status,session_id,created_at,updated_at) VALUES('r','p','j','live','s',1,1);
             INSERT INTO turns(id,session_id,run_id,job_id,sequence,state,start_reason,created_at,started_at,updated_at) VALUES('t','s','r','j',1,'running','prompt_response',1,1,1);
             UPDATE jobs SET current_turn_id='t',current_session_id='s' WHERE id='j';"
        ))
        .await
        .unwrap();
        let orch = scripted_test_orchestrator(db.clone());
        let mut handle = RunHandle::new(
            Arc::new(Mutex::new(None)),
            Arc::new(Mutex::new(Some(
                cairn_models::session::claude::ClaudeControl::boxed(Box::new(Vec::<u8>::new())),
            ))),
            Some("s".into()),
            Some("j".into()),
        );
        handle.begin_turn("t");
        orch.process_state
            .processes
            .lock()
            .unwrap()
            .register("r".into(), handle);
        orch.process_state.set_current_turn_id("r", Some("t"));
        (db, orch)
    }

    /// Run the dedicated-session reader over `notifications` to the end.
    fn read_notifications(orch: &Orchestrator, db: &Arc<LocalDb>, notifications: Vec<Value>) {
        let (tx, rx) = crossbeam_channel::unbounded();
        for notification in notifications {
            tx.send(notification).unwrap();
        }
        drop(tx);
        CodexBackend::reader_thread_app_server(
            orch,
            &orch.services.emitter,
            "r",
            Some("s".into()),
            rx,
            Arc::new(AppServerClient::for_test_scripted(
                vec![],
                Arc::new(Mutex::new(Vec::new())),
            )),
            Arc::new(Mutex::new(Some("p1".into()))),
            None,
            "codex".into(),
            db.clone(),
            None,
        );
    }

    async fn text(db: &LocalDb, sql: &str) -> Option<String> {
        db.query_opt_text(sql, ()).await.unwrap()
    }

    fn provider_error(message: &str) -> Value {
        json!({"method": "error", "params": {
            "turnId": "p1",
            "error": {"message": message},
            "willRetry": false,
        }})
    }

    const CAPACITY: &str = "Selected model is at capacity. Please try a different model.";

    /// cairn/6189's specimen: a recipe planner's turn reads a tool result, then
    /// the provider refuses the next sampling for capacity, without retrying. The
    /// process is alive and was reused hours later, so the turn must not read as
    /// a crash, the run must stay live on its warm process, and one automatic
    /// retry must be reserved on the same session and native thread. Current main
    /// excluded recipe nodes from the capacity retry, crashed the run and
    /// interrupted the turn as `crash`, and nothing resumed it.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_recipe_planner_refused_for_capacity_keeps_its_process_and_retries_on_it() {
        let (db, orch) = persistent_codex_turn(
            "codex-capacity-recipe-planner",
            "UPDATE jobs SET recipe_node_id='planner' WHERE id='j';",
        )
        .await;
        read_notifications(
            &orch,
            &db,
            vec![
                json!({"method":"item/agentMessage/delta","params":{"turnId":"p1","delta":"Read the plan."}}),
                provider_error(CAPACITY),
            ],
        );

        assert_eq!(
            text(
                &db,
                "SELECT state || '/' || COALESCE(end_reason,'') FROM turns WHERE id='t'"
            )
            .await
            .as_deref(),
            Some("interrupted/provider_failure"),
            "a provider refusal is not a crash"
        );
        assert_eq!(
            text(&db, "SELECT status FROM runs WHERE id='r'")
                .await
                .as_deref(),
            Some("live"),
            "the run keeps its live process"
        );
        assert!(
            orch.process_state.is_warm("r"),
            "the process is parked warm"
        );
        assert_eq!(
            text(&db, "SELECT start_reason || '/' || state || '/' || session_id FROM turns WHERE predecessor_id='t'").await.as_deref(),
            Some("retry/pending/s"),
            "one retry is reserved on the same session"
        );
        assert_eq!(
            text(&db, "SELECT backend_id FROM sessions WHERE id='s'")
                .await
                .as_deref(),
            Some("native-thread")
        );
        assert_eq!(
            db.query_opt_i64(
                "SELECT COUNT(*) FROM events WHERE data LIKE '%selected_model_capacity_retry%'",
                (),
            )
            .await
            .unwrap(),
            Some(1),
            "the retry is announced once"
        );
    }

    /// A delegated agent gets the same retry on its own warm conversation,
    /// rather than failing its parent's wait on a transient refusal.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_delegated_agent_refused_for_capacity_retries_instead_of_failing() {
        let (db, orch) = persistent_codex_turn(
            "codex-capacity-delegated",
            "UPDATE jobs SET parent_job_id='pj' WHERE id='j';",
        )
        .await;
        read_notifications(&orch, &db, vec![provider_error(CAPACITY)]);

        assert_eq!(
            text(
                &db,
                "SELECT state || '/' || COALESCE(end_reason,'') FROM turns WHERE id='t'"
            )
            .await
            .as_deref(),
            Some("interrupted/provider_failure")
        );
        assert_ne!(
            text(&db, "SELECT status FROM jobs WHERE id='j'")
                .await
                .as_deref(),
            Some("failed"),
            "a retry is still owed, so the child has not failed"
        );
        assert_eq!(
            text(
                &db,
                "SELECT start_reason FROM turns WHERE predecessor_id='t'"
            )
            .await
            .as_deref(),
            Some("retry")
        );
    }

    /// Once its retries are spent, a delegated agent fails terminally, which is
    /// what tells the waiting parent.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_delegated_agent_whose_capacity_retries_are_spent_fails_for_its_parent() {
        let (db, orch) = persistent_codex_turn(
            "codex-capacity-delegated-spent",
            "UPDATE jobs SET parent_job_id='pj' WHERE id='j';",
        )
        .await;
        // `t` is already the third retry in a row.
        db.execute_script(
            "INSERT INTO turns(id,session_id,job_id,sequence,state,start_reason,created_at,updated_at) VALUES('t0','s','j',-2,'interrupted','initial',0,0);
             INSERT INTO turns(id,session_id,job_id,sequence,predecessor_id,state,start_reason,created_at,updated_at) VALUES('ta','s','j',-1,'t0','interrupted','retry',0,0);
             INSERT INTO turns(id,session_id,job_id,sequence,predecessor_id,state,start_reason,created_at,updated_at) VALUES('tb','s','j',0,'ta','interrupted','retry',0,0);
             UPDATE turns SET start_reason='retry', predecessor_id='tb' WHERE id='t';",
        )
        .await
        .unwrap();
        read_notifications(&orch, &db, vec![provider_error(CAPACITY)]);

        assert_eq!(
            text(&db, "SELECT state FROM turns WHERE id='t'")
                .await
                .as_deref(),
            Some("failed")
        );
        assert_eq!(
            db.query_opt_i64("SELECT COUNT(*) FROM turns WHERE predecessor_id='t'", ())
                .await
                .unwrap(),
            Some(0),
            "no fourth retry"
        );
        let errors = text(
            &db,
            "SELECT group_concat(json_extract(data,'$.content'), ' | ') FROM events WHERE event_type='system:error'",
        )
        .await
        .unwrap_or_default();
        assert!(errors.contains("remained at capacity"), "{errors}");
    }

    /// Any other fatal provider error on a top-level job ends the turn, not the
    /// process: the diagnostic is kept, and the job is left for the queued-content
    /// flush and the unowned-running sweep to continue.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_fatal_provider_error_ends_the_turn_not_the_process() {
        let (db, orch) = persistent_codex_turn("codex-fatal-top-level", "").await;
        read_notifications(
            &orch,
            &db,
            vec![provider_error("stream disconnected before completion")],
        );

        assert_eq!(
            text(
                &db,
                "SELECT state || '/' || COALESCE(end_reason,'') FROM turns WHERE id='t'"
            )
            .await
            .as_deref(),
            Some("interrupted/provider_failure")
        );
        assert_eq!(
            text(&db, "SELECT status FROM runs WHERE id='r'")
                .await
                .as_deref(),
            Some("live")
        );
        assert!(orch.process_state.is_warm("r"));
        let errors = text(
            &db,
            "SELECT group_concat(json_extract(data,'$.content'), ' | ') FROM events WHERE event_type='system:error'",
        )
        .await
        .unwrap_or_default();
        assert!(errors.contains("stream disconnected"), "{errors}");
        assert_eq!(
            text(
                &db,
                "SELECT COALESCE(exit_reason, '') FROM runs WHERE id='r'"
            )
            .await
            .as_deref(),
            Some(""),
            "no exit is recorded for a process that did not exit"
        );
    }

    /// The post-tool shape the issue first suspected: Codex ends a turn normally
    /// with no assistant item after a tool result. That is a completed turn on a
    /// live process, never a crash.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_turn_completed_right_after_a_tool_result_is_not_a_crash() {
        let (db, orch) = persistent_codex_turn(
            "codex-post-tool-complete",
            "UPDATE jobs SET recipe_node_id='planner' WHERE id='j';",
        )
        .await;
        read_notifications(
            &orch,
            &db,
            vec![
                json!({"method":"turn/completed","params":{"turn":{"id":"p1","status":"completed"}}}),
            ],
        );

        assert_eq!(
            text(
                &db,
                "SELECT state || '/' || COALESCE(end_reason,'') FROM turns WHERE id='t'"
            )
            .await
            .as_deref(),
            Some("complete/")
        );
        assert_eq!(
            text(&db, "SELECT status FROM runs WHERE id='r'")
                .await
                .as_deref(),
            Some("live")
        );
        assert!(orch.process_state.is_warm("r"));
    }

    #[tokio::test]
    async fn nodeless_run_does_not_enter_goal_turn_admission() {
        let db = Arc::new(crate::storage::migrated_test_db("nodeless-goal-admission").await);
        db.execute_script(
            "PRAGMA foreign_keys = OFF;
             INSERT INTO runs (id, status, created_at, updated_at)
               VALUES ('project-chat', 'live', 1, 1);
             PRAGMA foreign_keys = ON;",
        )
        .await
        .unwrap();

        assert_eq!(goal_job_id_for_run(&db, "project-chat").unwrap(), None);
        assert!(
            goal_job_id_for_run(&db, "missing-run").is_err(),
            "a missing run remains fail-closed"
        );
    }
}
