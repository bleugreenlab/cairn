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
    identity: &crate::agent_process::process::ProcessIdentity,
) -> Result<Option<GoalTurnAdmission>, String> {
    if identity.run_id() != run_id || !orch.process_state.is_process_identity_current(identity) {
        return Err("Codex reader no longer owns its process registration".to_string());
    }
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
    if !orch.process_state.is_process_identity_current(identity) {
        return Err("Codex reader lost its process registration before admission".to_string());
    }
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
        let state = crate::storage::run_db_blocking({
            let db = db.clone();
            let turn_id = turn_id.clone();
            move || async move {
                db.query_opt_text("SELECT state FROM turns WHERE id=?1", (turn_id,))
                    .await
                    .map_err(|error| error.to_string())
            }
        })?
        .ok_or_else(|| format!("admitted Codex turn {turn_id} disappeared before attachment"))?
        .parse::<crate::models::TurnState>()?;
        if state.is_terminal() {
            return Ok(Some(GoalTurnAdmission::Ignored));
        }
        if current.as_deref() != Some(turn_id.as_str())
            && orch
                .process_state
                .begin_turn_if_identity(identity, turn_id)
                .is_none()
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

    Ok(Some(match admission {
        crate::execution::jobs::CodexGoalTurnAdmission::Admitted(turn_id) => {
            let owner = orch
                .process_state
                .capture_turn_owner_for_identity(identity)
                .filter(|owner| owner.turn_id() == turn_id)
                .ok_or_else(|| "native Codex goal turn lost its admitted owner".to_string())?;
            GoalTurnAdmission::Admitted { owner }
        }
        crate::execution::jobs::CodexGoalTurnAdmission::Deferred {
            predecessor_turn_id,
        } => GoalTurnAdmission::Deferred {
            predecessor_turn_id,
        },
    }))
}

fn capacity_retry_target(
    db: &Arc<LocalDb>,
    owner: &crate::agent_process::process::ProcessTurnOwner,
) -> Result<Option<CapacityRetryTarget>, String> {
    let run_id = owner.run_id();
    let cairn_turn_id = owner.turn_id().to_string();
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
    owner: &crate::agent_process::process::ProcessTurnOwner,
) -> bool {
    if owner.run_id() != run_id || !orch.process_state.is_turn_owner_current(owner) {
        return false;
    }
    let target = match capacity_retry_target(run_db, owner) {
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
            return false;
        }
    };
    let next_attempt = completed_retries + 1;
    if next_attempt > CAPACITY_RETRY_DELAYS.len() as u32 {
        match hold_at_capacity(orch, run_db, run_id, session_id, &target, owner) {
            CapacityHoldOutcome::Armed => return true,
            CapacityHoldOutcome::StaleOwner => return false,
            CapacityHoldOutcome::Unarmed => {}
        }
        if !settle_capacity_failed_turn(orch, run_db, run_id, "turn_failed", owner) {
            return false;
        }
        insert_error_event(
            orch,
            run_id,
            session_id,
            "The selected Codex model remained at capacity after three automatic retries. You can continue this job manually to try again.",
        );
        return true;
    }

    if !settle_capacity_failed_turn(orch, run_db, run_id, "capacity_retry", owner) {
        return false;
    }
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

#[derive(Debug, PartialEq, Eq)]
enum CapacityHoldOutcome {
    StaleOwner,
    Unarmed,
    Armed,
}

/// Hold a turn whose model stayed at capacity through every in-session retry,
/// rather than failing it: at capacity is a want of supply, which waiting
/// cures. The turn yields as a provider limit with no reset time, because
/// Codex declares none and none is inferred, so the hold re-checks on its
/// bounded clock and any change to the accounts asks sooner. The process stays
/// warm for the resume. Switching an open session to another offer is its own
/// lifecycle operation (cairn/4693), so the hold waits for this model.
///
/// Only an unarmed hold permits exact-owner failure fallback. Once persisted,
/// the hold owns recovery even if its process occupancy could not be released.
fn hold_at_capacity(
    orch: &Orchestrator,
    run_db: &Arc<LocalDb>,
    run_id: &str,
    session_id: Option<&str>,
    target: &CapacityRetryTarget,
    owner: &crate::agent_process::process::ProcessTurnOwner,
) -> CapacityHoldOutcome {
    let launch_lock = orch.job_launch_lock(&target.job_id);
    let launch_guard = launch_lock
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if !orch.process_state.is_turn_owner_current(owner) {
        return CapacityHoldOutcome::StaleOwner;
    }
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
            return CapacityHoldOutcome::Unarmed;
        }
    };
    let warmed = orch.process_state.transition_to_warm_if_owned(owner);
    drop(launch_guard);
    if !warmed {
        // The durable hold already yielded this turn. Do not fail it, or any
        // replacement owner, just because its occupancy is no longer ours.
        log::warn!("Capacity hold for run {run_id} armed without releasing its accepted occupancy");
        return CapacityHoldOutcome::Armed;
    }
    if let Err(error) = crate::execution::advancement::recompute_job(orch, &target.job_id) {
        log::error!(
            "Failed to recompute capacity-held job {}: {error}",
            target.job_id
        );
    }
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
    CapacityHoldOutcome::Armed
}

/// End the turn the provider refused for capacity.
///
/// A capacity refusal says nothing about the process, so a top-level or recipe
/// job keeps its live run and native conversation and only the turn ends, as
/// `provider_failure`. An exhausted delegated child finalizes its exact owner,
/// recording `reason` so its waiting parent receives the failure. A stale owner
/// or failed settlement authorizes no fallback finalization.
fn settle_capacity_failed_turn(
    orch: &Orchestrator,
    run_db: &Arc<LocalDb>,
    run_id: &str,
    reason: &str,
    owner: &crate::agent_process::process::ProcessTurnOwner,
) -> bool {
    let exhausted = reason == "turn_failed";
    let delegated = crate::backends::is_task_spawned_run(super::CODEX_BACKEND_NAME, run_db, run_id);
    if exhausted && delegated {
        return crate::orchestrator::lifecycle::finalize_owned_completion(
            orch,
            owner.process_identity(),
            Some(owner),
            crate::models::RunStatus::Crashed,
            Some(reason),
            None,
            || {},
        );
    }
    if crate::orchestrator::lifecycle::release_owned_completion(
        orch,
        owner.process_identity(),
        Some(owner),
        Some((
            crate::models::TurnState::Interrupted,
            Some(crate::models::TurnEndReason::ProviderFailure),
        )),
    ) {
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
        return true;
    }
    false
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
        let Some(identity) = orch.process_state.capture_process_identity(run_id) else {
            host.host.reader_closed();
            return;
        };
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
            identity,
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
    fn admit_goal_turn(
        &self,
        turn_id: &str,
        identity: &crate::agent_process::process::ProcessIdentity,
    ) -> Result<Option<GoalTurnAdmission>, String> {
        admit_native_goal_turn(
            &self.session.orch,
            &self.session.run_db,
            &self.session.run_id,
            self.session.session_id.as_deref(),
            turn_id,
            identity,
        )
    }
    /// The terminal-artifact handoff: the agent wrote its output, so the goal
    /// must not continue past it. It lifts when the job next runs.
    fn interrupt(
        &self,
        identity: &crate::agent_process::process::ProcessIdentity,
        owner: Option<&crate::agent_process::process::ProcessTurnOwner>,
    ) -> Result<(), String> {
        let job_id = goal_job_id_for_run(&self.session.run_db, &self.session.run_id)?;
        let launch_lock = job_id
            .as_deref()
            .map(|id| self.session.orch.job_launch_lock(id));
        let _guard = launch_lock
            .as_ref()
            .map(|lock| lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner()));
        let state = &self.session.orch.process_state;
        if identity.run_id() != self.session.run_id
            || !state.is_process_identity_current(identity)
            || owner.map_or_else(
                || !state.is_untracked_busy_for_identity(identity),
                |owner| owner.process_identity() != identity || !state.is_turn_owner_current(owner),
            )
        {
            return Err("Codex terminal boundary no longer owns its process turn".to_string());
        }
        crate::backends::stdin::send_interrupt(
            &self.session.orch.process_state,
            &self.session.run_id,
            crate::agent_process::process::PauseIntent::Stake(
                crate::agent_process::process::GoalPause::Interrupted,
            ),
            cairn_models::interface::session::InterruptCause::ArtifactHandoff,
        )
    }
    fn take_interrupt_cause(
        &self,
        provider_turn_id: &str,
    ) -> Option<cairn_models::interface::session::InterruptCause> {
        self.client
            .as_ref()
            .and_then(|client| client.take_interrupt_cause(provider_turn_id))
    }
    fn artifact_handoff(
        &self,
        identity: &crate::agent_process::process::ProcessIdentity,
        owner: Option<&crate::agent_process::process::ProcessTurnOwner>,
    ) -> bool {
        crate::orchestrator::lifecycle::transition_owned_completion_to_warm(
            &self.session.orch,
            identity,
            owner,
            Some((
                crate::models::TurnState::Complete,
                Some(crate::models::TurnEndReason::ArtifactHandoff),
            )),
        )
    }
    fn watchdog_arm_failed(&self, evidence: &str) -> Result<(), String> {
        // The exit reason stays the stable category; the evidence is the
        // prose a waiting parent reads, kept before the kill finalizes.
        crate::orchestrator::lifecycle::record_run_cause_if_active(
            &self.session.orch,
            &self.session.run_id,
            evidence,
        );
        crate::orchestrator::lifecycle::kill_session_with_reason(
            &self.session.orch,
            &self.session.run_id,
            crate::orchestrator::lifecycle::WATCHDOG_ARM_FAILED_EXIT_REASON,
            // A lease write that failed says nothing about the job's own work.
            crate::orchestrator::lifecycle::TerminalPolicy::Keep,
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
    fn capacity_failure(
        &self,
        owner: Option<&crate::agent_process::process::ProcessTurnOwner>,
    ) -> bool {
        let Some(owner) = owner else {
            return false;
        };
        handle_selected_model_capacity_failure(
            &self.session.orch,
            &self.session.run_db,
            &self.session.run_id,
            self.session.session_id.as_deref(),
            owner,
        )
    }
    fn complete(&self, completion: SessionTurnCompletion, mut context: CodexTurnContext) {
        // Cairn's account-scoped refusal explains an opaque provider auth error.
        // Retired bindings may account usage, but cannot give another turn a cause.
        let failed = !context.accounting_only
            && matches!(completion.outcome, SessionTurnOutcome::Failed { .. })
            && context.failure_disposition != TurnFailureDisposition::AlreadyHandled
            && context.owner.as_ref().map_or_else(
                || {
                    self.session
                        .orch
                        .process_state
                        .is_process_identity_current(&context.process_identity)
                },
                |owner| self.session.orch.process_state.is_turn_owner_current(owner),
            );
        context.fatal_cause = if failed {
            let refusal = self.standing_token_refusal();
            if let Some(notice) = refusal.as_deref() {
                self.error(notice);
            }
            refusal.or(context.fatal_cause)
        } else {
            None
        };
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
        for (terminal, settled) in [
            (
                json!({"method":"turn/completed","params":{"turn":{"id":"p1","status":"completed"}}}),
                "complete",
            ),
            // An abort Cairn did not ask for cuts the turn off.
            (
                json!({"method":"turn/aborted","params":{"turnId":"p1","reason":"interrupted"}}),
                "interrupted",
            ),
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
                Some(settled)
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
             INSERT INTO jobs(id,project_id,status,node_name,created_at,updated_at) VALUES('jp','p','running','builder',1,1);
             INSERT INTO jobs(id,project_id,status,node_name,parent_job_id,created_at,updated_at) VALUES('j','p','running','review','jp',1,1);
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

        // The task fails, and the cause its parent is told is Cairn's
        // account-scoped refusal, not Codex's opaque "unauthorized".
        let notice = format!(
            "Cairn refused this Codex session's request for fresh ChatGPT tokens: {refusal}"
        );
        let cause = db
            .query_opt_text("SELECT error_message FROM runs WHERE id='r'", ())
            .await
            .unwrap()
            .expect("the failed task run keeps its cause");
        assert_eq!(
            Some(cause.as_str()),
            crate::security::sanitize::failure_cause(&notice).as_deref()
        );
        assert!(!cause.contains("workspace routing discovery"), "{cause}");
        assert!(!cause.contains("at-1"), "{cause}");
        assert_eq!(
            db.query_opt_text("SELECT state FROM turns WHERE id='t'", ())
                .await
                .unwrap()
                .as_deref(),
            Some("failed")
        );
        assert_eq!(
            db.query_opt_text("SELECT exit_reason FROM runs WHERE id='r'", ())
                .await
                .unwrap()
                .as_deref(),
            Some("turn_failed")
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
            // The refusal is the run's recorded cause, so the parent of a
            // refused task is told it rather than a generic error.
            let cause = db
                .query_opt_text("SELECT error_message FROM runs WHERE id='r'", ())
                .await
                .unwrap()
                .expect("the refused run keeps its cause");
            assert!(cause.contains(expected), "{cause}");
            assert!(
                cause.starts_with("Codex native goal turn p1 could not enter Cairn's turn lifecycle"),
                "{cause}"
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

    /// The transport must deliver a response while admission is parked behind
    /// launch ownership; notification handling must not hold the sender's lock.
    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread")]
    async fn notification_before_attachment_does_not_block_the_launch_response() {
        use cairn_platform::services::process::{ChildProcess, SpawnConfig};
        use std::io::{BufRead, BufReader, Write};
        use std::os::unix::net::UnixStream;

        struct PipeChild(Option<UnixStream>, Option<UnixStream>);
        impl ChildProcess for PipeChild {
            fn id(&self) -> u32 {
                0
            }
            fn take_stdout(&mut self) -> Option<Box<dyn BufRead + Send>> {
                self.0
                    .take()
                    .map(|pipe| Box::new(BufReader::new(pipe)) as _)
            }
            fn take_stderr(&mut self) -> Option<Box<dyn BufRead + Send>> {
                None
            }
            fn take_stdin(&mut self) -> Option<Box<dyn Write + Send>> {
                self.1.take().map(|pipe| Box::new(pipe) as _)
            }
            fn try_wait(&mut self) -> std::io::Result<Option<std::process::ExitStatus>> {
                Ok(None)
            }
            fn kill(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let (db, orch) = persistent_codex_turn("codex-launch-response-order", "").await;
        db.execute_script(
            "UPDATE turns SET state='pending',started_at=NULL WHERE id='t';
             INSERT INTO events(id,run_id,turn_id,sequence,event_type,data,timestamp,created_at)
               VALUES('brief-t','r','t',1,'attention:briefing','{}',1,1);",
        )
        .await
        .unwrap();
        orch.process_state.transition_to_warm("r");
        let launch_lock = orch.job_launch_lock("j");
        let launch_guard = launch_lock.lock().unwrap();
        let (client_pipe, mut server_pipe) = UnixStream::pair().unwrap();
        let client_write = client_pipe.try_clone().unwrap();
        let client = Arc::new(
            AppServerClient::launch(
                |_| Ok(Box::new(PipeChild(Some(client_pipe), Some(client_write)))),
                SpawnConfig::new("test-codex"),
            )
            .unwrap(),
        );
        let notifications = client.notifications();
        let (contended_tx, contended_rx) = crossbeam_channel::bounded(1);
        let (admitted_tx, admitted_rx) = crossbeam_channel::bounded(1);
        let reader_orch = orch.clone();
        let reader_db = db.clone();
        let reader_client = client.clone();
        let provider_turn = Arc::new(Mutex::new(None));
        let reader_provider_turn = provider_turn.clone();
        let reader = std::thread::spawn(move || {
            let notification = notifications
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap();
            assert_eq!(notification["method"], "turn/started");
            assert!(reader_orch.job_launch_lock("j").try_lock().is_err());
            contended_tx.send(()).unwrap();
            let (tx, rx) = crossbeam_channel::unbounded();
            tx.send(notification).unwrap();
            drop(tx);
            CodexBackend::reader_thread_app_server(
                &reader_orch,
                &reader_orch.services.emitter,
                "r",
                Some("s".into()),
                rx,
                reader_client,
                reader_provider_turn,
                None,
                "codex".into(),
                reader_db,
                None,
            );
            admitted_tx.send(()).unwrap();
        });
        let server = std::thread::spawn(move || {
            let mut request = String::new();
            BufReader::new(server_pipe.try_clone().unwrap())
                .read_line(&mut request)
                .unwrap();
            let request: Value = serde_json::from_str(&request).unwrap();
            assert_eq!(request["method"], "turn/start");
            writeln!(
                server_pipe,
                "{}",
                json!({
                    "method":"turn/started", "params":{"turn":{"id":"provider-launch"}}
                })
            )
            .unwrap();
            contended_rx
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap();
            writeln!(
                server_pipe,
                "{}",
                json!({
                    "id":request["id"], "result":{"turn":{"id":"provider-launch"}}
                })
            )
            .unwrap();
        });
        // This response crosses the real stdout demultiplexer while the
        // notification consumer is unable to acquire the job launch lock.
        let response = client
            .send_request_within(
                "turn/start",
                json!({"threadId":"native-thread"}),
                std::time::Instant::now() + std::time::Duration::from_secs(5),
            )
            .unwrap();
        assert_eq!(response["turn"]["id"], "provider-launch");
        assert!(admitted_rx.try_recv().is_err());
        crate::execution::jobs::start_turn(&orch, "t", "r").unwrap();
        let identity = orch.process_state.capture_process_identity("r").unwrap();
        orch.process_state
            .begin_turn_if_identity(&identity, "t")
            .unwrap();
        drop(launch_guard);
        admitted_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        assert_eq!(
            provider_turn.lock().unwrap().as_deref(),
            Some("provider-launch")
        );
        reader.join().unwrap();
        server.join().unwrap();
        assert_eq!(
            orch.process_state.get_current_turn_id("r").as_deref(),
            Some("t")
        );
        assert_eq!(
            db.query_opt_i64("SELECT COUNT(*) FROM turns WHERE job_id='j'", ())
                .await
                .unwrap(),
            Some(1)
        );
        assert_eq!(
            db.query_opt_i64(
                "SELECT COUNT(*) FROM events WHERE event_type='attention:briefing'",
                ()
            )
            .await
            .unwrap(),
            Some(1)
        );
        assert_eq!(
            db.query_opt_i64(
                "SELECT COUNT(*) FROM events WHERE event_type='system:codex_goal_turn_started'",
                ()
            )
            .await
            .unwrap(),
            Some(1)
        );
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

    #[tokio::test(flavor = "multi_thread")]
    async fn exhausted_capacity_distinguishes_unarmed_fallback_from_armed_hold() {
        for arm_hold in [false, true] {
            let (db, orch) = persistent_codex_turn(
                if arm_hold {
                    "codex-capacity-armed"
                } else {
                    "codex-capacity-unarmed"
                },
                "",
            )
            .await;
            db.execute_script(
                "INSERT INTO turns(id,session_id,job_id,sequence,state,start_reason,created_at,updated_at) VALUES('t0','s','j',-2,'interrupted','initial',0,0);
                 INSERT INTO turns(id,session_id,job_id,sequence,predecessor_id,state,start_reason,created_at,updated_at) VALUES('ta','s','j',-1,'t0','interrupted','retry',0,0);
                 INSERT INTO turns(id,session_id,job_id,sequence,predecessor_id,state,start_reason,created_at,updated_at) VALUES('tb','s','j',0,'ta','interrupted','retry',0,0);
                 UPDATE turns SET start_reason='retry', predecessor_id='tb' WHERE id='t';",
            )
            .await
            .unwrap();
            if !arm_hold {
                // Fail the hold write, not the turn settlement. Its transaction
                // must roll back the yield before exact-owner failure fallback.
                db.execute_script(
                    "CREATE TRIGGER refuse_capacity_hold BEFORE INSERT ON provider_limit_holds
                     BEGIN SELECT RAISE(ABORT, 'capacity hold unavailable'); END;",
                )
                .await
                .unwrap();
            }
            let identity = orch.process_state.capture_process_identity("r").unwrap();
            let owner = orch
                .process_state
                .capture_turn_owner_for_identity(&identity)
                .unwrap();
            assert!(handle_selected_model_capacity_failure(
                &orch,
                &db,
                "r",
                Some("s"),
                &owner
            ));
            assert_eq!(
                turn_row(&db).await.as_deref(),
                Some(if arm_hold {
                    "yielded/"
                } else {
                    "interrupted/provider_failure"
                })
            );
            assert_eq!(
                db.query_opt_i64(
                    "SELECT COUNT(*) FROM provider_limit_holds WHERE state='active'",
                    ()
                )
                .await
                .unwrap(),
                Some(i64::from(arm_hold))
            );
            assert_eq!(
                db.query_opt_i64(
                    "SELECT COUNT(*) FROM events WHERE event_type='system:error' AND data LIKE '%remained at capacity after three automatic retries%'",
                    (),
                )
                .await.unwrap(),
                Some(i64::from(!arm_hold)),
                "only a successfully settled unarmed fallback reports exhaustion"
            );
            assert_eq!(
                text(&db, "SELECT status FROM runs WHERE id='r'")
                    .await
                    .as_deref(),
                Some("live")
            );
            assert_eq!(
                db.query_opt_i64("SELECT COUNT(*) FROM turns WHERE predecessor_id='t'", ())
                    .await
                    .unwrap(),
                Some(0),
                "neither outcome starts a fourth retry"
            );
            assert!(orch.process_state.begin_turn("r", "successor"));
            assert!(!handle_selected_model_capacity_failure(
                &orch,
                &db,
                "r",
                Some("s"),
                &owner
            ));
            assert_eq!(
                orch.process_state.get_current_turn_id("r").as_deref(),
                Some("successor")
            );
            assert_eq!(
                db.query_opt_i64(
                    "SELECT COUNT(*) FROM events WHERE event_type='system:error' AND data LIKE '%remained at capacity after three automatic retries%'",
                    (),
                )
                .await.unwrap(),
                Some(i64::from(!arm_hold)),
                "stale replay cannot diagnose or fail the successor"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stale_capacity_owner_cannot_settle_or_hold_a_successor() {
        let (db, orch) = persistent_codex_turn("codex-stale-capacity-owner", "").await;
        let identity = orch.process_state.capture_process_identity("r").unwrap();
        let owner = orch
            .process_state
            .capture_turn_owner_for_identity(&identity)
            .unwrap();
        let target = capacity_retry_target(&db, &owner).unwrap().unwrap();
        assert!(orch.process_state.begin_turn("r", "successor"));
        assert!(!handle_selected_model_capacity_failure(
            &orch,
            &db,
            "r",
            Some("s"),
            &owner
        ));
        assert!(!settle_capacity_failed_turn(
            &orch,
            &db,
            "r",
            "capacity_retry",
            &owner
        ));
        assert!(!settle_capacity_failed_turn(
            &orch,
            &db,
            "r",
            "turn_failed",
            &owner
        ));
        assert_eq!(
            hold_at_capacity(&orch, &db, "r", Some("s"), &target, &owner),
            CapacityHoldOutcome::StaleOwner
        );
        assert_eq!(
            orch.process_state.get_current_turn_id("r").as_deref(),
            Some("successor")
        );
        assert_eq!(
            text(&db, "SELECT state FROM turns WHERE id='t'")
                .await
                .as_deref(),
            Some("running")
        );
        assert_eq!(
            text(&db, "SELECT status FROM runs WHERE id='r'")
                .await
                .as_deref(),
            Some("live")
        );
        assert_eq!(
            text(
                &db,
                "SELECT CAST(COUNT(*) AS TEXT) FROM provider_limit_holds"
            )
            .await
            .as_deref(),
            Some("0")
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn replayed_terminal_provider_marker_cannot_reattach_over_successor() {
        let (db, orch) = persistent_codex_turn("codex-terminal-marker-replay", "").await;
        let identity = orch.process_state.capture_process_identity("r").unwrap();
        assert!(matches!(
            admit_native_goal_turn(&orch, &db, "r", Some("s"), "p1", &identity).unwrap(),
            Some(GoalTurnAdmission::Admitted { .. })
        ));
        db.execute_script("UPDATE turns SET state='complete' WHERE id='t';")
            .await
            .unwrap();
        assert!(orch.process_state.begin_turn("r", "successor"));
        assert!(matches!(
            admit_native_goal_turn(&orch, &db, "r", Some("s"), "p1", &identity).unwrap(),
            Some(GoalTurnAdmission::Ignored)
        ));
        assert_eq!(
            orch.process_state.get_current_turn_id("r").as_deref(),
            Some("successor")
        );
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

    /// [`read_notifications`] through `client`, so a test can record an
    /// interrupt's cause on it first and see what the reader asked of it.
    fn read_through(
        orch: &Orchestrator,
        db: &Arc<LocalDb>,
        client: Arc<AppServerClient>,
        notifications: Vec<Value>,
    ) {
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
            client,
            Arc::new(Mutex::new(Some("p1".into()))),
            None,
            "codex".into(),
            db.clone(),
            None,
        );
    }

    /// A client that has already sent one `turn/interrupt` for `p1` with
    /// `cause`, and records anything the reader sends after.
    fn client_that_interrupted(
        cause: cairn_models::interface::session::InterruptCause,
    ) -> (Arc<AppServerClient>, Arc<Mutex<Vec<Value>>>) {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let client = Arc::new(AppServerClient::for_test_scripted(
            vec![Ok(Value::Null)],
            requests.clone(),
        ));
        client
            .interrupt_turn("native-thread", "p1", cause, None)
            .unwrap();
        requests.lock().unwrap().clear();
        (client, requests)
    }

    fn aborted(reason: &str) -> Value {
        json!({"method": "turn/aborted", "params": {"turnId": "p1", "reason": reason}})
    }

    async fn turn_row(db: &LocalDb) -> Option<String> {
        text(
            db,
            "SELECT state || '/' || COALESCE(end_reason,'') FROM turns WHERE id='t'",
        )
        .await
    }

    async fn interrupted_record(db: &LocalDb) -> Option<String> {
        text(
            db,
            "SELECT COALESCE(json_extract(data,'$.raw.interruptCause'),'none') || '/' || json_extract(data,'$.raw.providerReason') || '/' || json_extract(data,'$.raw.providerTurnId')
               FROM events WHERE event_type='result:interrupted'",
        )
        .await
    }

    /// cairn/4634 acceptance 1 and 3: a read completes, the model reasons and
    /// answers. Nothing interrupts the turn and it completes as itself.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_completed_read_followed_by_reasoning_continues_to_completion() {
        let (db, orch) = persistent_codex_turn("codex-read-then-reason", "").await;
        let requests = Arc::new(Mutex::new(Vec::new()));
        let client = Arc::new(AppServerClient::for_test_scripted(vec![], requests.clone()));
        read_through(
            &orch,
            &db,
            client,
            vec![
                json!({"method":"item/started","params":{"item":{"type":"mcpToolCall","id":"read-1","server":"cairn","tool":"read","arguments":{}}}}),
                json!({"method":"item/completed","params":{"turnId":"p1","item":{"type":"mcpToolCall","id":"read-1","server":"cairn","tool":"read","status":"completed","result":{"content":[{"type":"text","text":"diff"}]}}}}),
                json!({"method":"item/started","params":{"item":{"type":"reasoning","id":"r-1"}}}),
                json!({"method":"item/completed","params":{"turnId":"p1","item":{"type":"reasoning","id":"r-1"}}}),
                json!({"method":"item/completed","params":{"turnId":"p1","item":{"type":"agentMessage","id":"m-1","text":"Next I will edit."}}}),
                json!({"method":"turn/completed","params":{"turn":{"id":"p1","status":"completed"}}}),
            ],
        );
        assert!(
            !requests
                .lock()
                .unwrap()
                .iter()
                .any(|request| request["method"] == "turn/interrupt"),
            "a completed read never interrupts the turn"
        );
        assert_eq!(turn_row(&db).await.as_deref(), Some("complete/"));
        assert_eq!(interrupted_record(&db).await, None);
    }

    /// An abort Cairn never asked for is the provider's own: the turn is cut
    /// off, never laundered into `complete`, and the ledger says so.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_abort_with_no_recorded_cause_is_a_provider_interrupt() {
        let (db, orch) = persistent_codex_turn("codex-unattributed-abort", "").await;
        read_notifications(&orch, &db, vec![aborted("interrupted")]);
        assert_eq!(
            turn_row(&db).await.as_deref(),
            Some("interrupted/provider_interrupt")
        );
        assert_eq!(
            interrupted_record(&db).await.as_deref(),
            Some("none/interrupted/p1")
        );
        assert_eq!(
            text(&db, "SELECT status FROM runs WHERE id='r'")
                .await
                .as_deref(),
            Some("live"),
            "the session stays open for whatever continues the job"
        );
    }

    /// The watchdog's post-tool interrupt, attributed through the client.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_watchdog_post_tool_interrupt_reads_as_the_watchdogs() {
        let (db, orch) = persistent_codex_turn("codex-post-tool-abort", "").await;
        let (client, _) = client_that_interrupted(
            cairn_models::interface::session::InterruptCause::WatchdogPostTool,
        );
        read_through(&orch, &db, client.clone(), vec![aborted("interrupted")]);
        assert_eq!(
            turn_row(&db).await.as_deref(),
            Some("interrupted/watchdog_recovery")
        );
        assert_eq!(
            interrupted_record(&db).await.as_deref(),
            Some("watchdog_post_tool/interrupted/p1")
        );
        assert_eq!(
            client.take_interrupt_cause("p1"),
            None,
            "consumed by the abort"
        );
    }

    type RetirementReaderHook = Box<dyn FnMut(&str) + Send>;
    type RetirementDbChangeHook = Box<dyn FnOnce() + Send>;

    #[derive(Clone, Default)]
    struct RetirementReaderEmitter {
        events: Arc<Mutex<Vec<String>>>,
        on_marker: Arc<Mutex<Option<RetirementReaderHook>>>,
        on_db_change: Arc<Mutex<Option<RetirementDbChangeHook>>>,
    }

    impl crate::services::EventEmitter for RetirementReaderEmitter {
        fn emit(&self, event: &str, payload: Value) -> Result<(), String> {
            self.events.lock().unwrap().push(event.to_string());
            if event == "db-change"
                && payload["table"] == "events"
                && payload["action"] == "insert"
                && payload["runId"] == "r"
            {
                let callback = self.on_db_change.lock().unwrap().take();
                if let Some(callback) = callback {
                    callback();
                }
            }
            if event == "tool-output-delta" && payload["toolUseId"] == "retirement-marker" {
                let callback = self.on_marker.lock().unwrap().take();
                if let Some(mut callback) = callback {
                    callback(payload["delta"].as_str().unwrap());
                    *self.on_marker.lock().unwrap() = Some(callback);
                }
            }
            Ok(())
        }

        fn emit_empty(&self, event: &str) -> Result<(), String> {
            self.emit(event, Value::Null)
        }
    }

    /// Actual park/Stop release precedes the terminal report. Its accepted
    /// binding still owns accounting, not the successor's stream or lifecycle.
    #[tokio::test(flavor = "multi_thread")]
    async fn park_and_stop_terminals_account_once_without_successor_effects() {
        for (
            stop,
            successor,
            interrupt_fails,
            completed,
            settlement_fails,
            stale_stop,
            idle_registration,
        ) in [
            (false, true, false, false, false, false, 0),
            (false, false, true, true, false, false, 0),
            (true, false, false, false, false, false, 0),
            (true, false, false, true, false, false, 0),
            (true, false, false, false, true, false, 0),
            (true, true, false, false, false, true, 0),
            (true, false, false, false, false, false, 1),
            (true, false, false, false, false, false, 2),
        ] {
            let (db, _) = persistent_codex_turn("codex-retired-accounting", "").await;
            let emitter = RetirementReaderEmitter::default();
            let root = tempfile::tempdir().unwrap().keep();
            let state = Arc::new(DbState::new(
                db.clone(),
                Arc::new(SearchIndex::open_or_create(root.join("search.db")).unwrap()),
            ));
            let orch = OrchestratorBuilder::new(
                state,
                Arc::new(
                    TestServicesBuilder::new()
                        .with_emitter(emitter.clone())
                        .build(),
                ),
                root,
            )
            .build();
            let goal = |status: &str| {
                json!({"result":{"goal":{
                    "threadId":"native-thread","objective":"finish work","status":status,
                    "tokenBudget":null,"tokensUsed":0,"timeUsedSeconds":0,"createdAt":1,"updatedAt":2
                }}})
            };
            let requests = Arc::new(Mutex::new(Vec::new()));
            let responses = if interrupt_fails {
                vec![Err("pause transport failed".to_string())]
            } else {
                vec![Ok(goal("active")), Ok(goal("paused")), Ok(Value::Null)]
            };
            let client = Arc::new(AppServerClient::for_test_scripted(
                responses,
                requests.clone(),
            ));
            let provider = Arc::new(Mutex::new(Some("p1".to_string())));
            let control = cairn_models::session::codex::CodexControl::new(
                client.clone(),
                "native-thread".into(),
                "/tmp/p".into(),
                None,
                None,
                None,
                provider.clone(),
            );
            let mut handle = RunHandle::new(
                Arc::new(Mutex::new(None)),
                Arc::new(Mutex::new(Some(Box::new(control)))),
                Some("s".into()),
                Some("j".into()),
            );
            handle.backend = Some("codex".into());
            if idle_registration == 0 {
                handle.begin_turn("t");
            }
            orch.process_state
                .processes
                .lock()
                .unwrap()
                .register("r".into(), handle);
            if idle_registration != 0 {
                let status = if idle_registration == 1 {
                    "starting"
                } else {
                    "live"
                };
                if idle_registration == 2 {
                    assert!(orch.process_state.transition_to_warm("r"));
                }
                db.execute("UPDATE runs SET status=?1 WHERE id='r'", (status,))
                    .await
                    .unwrap();
                db.execute("UPDATE turns SET state='pending' WHERE id='t'", ())
                    .await
                    .unwrap();
                *provider.lock().unwrap() = None;
                let identity = orch.process_state.capture_process_identity("r").unwrap();
                crate::orchestrator::lifecycle::operator_stop_run(&orch, "r").unwrap();
                assert_eq!(
                    text(&db, "SELECT status FROM runs WHERE id='r'")
                        .await
                        .as_deref(),
                    Some(status)
                );
                assert!(orch.process_state.is_process_identity_current(&identity));
                assert!(!emitter
                    .events
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|event| event == "run-completed"));
                continue;
            }
            let owner = orch.process_state.capture_turn_owner("r", "t").unwrap();
            let mut broadcasts = orch.run_completions.subscribe();
            let action = orch.clone();
            let action_db = db.clone();
            let action_emitter = emitter.clone();
            *emitter.on_marker.lock().unwrap() = Some(Box::new(move |phase| {
                if phase == "retire" {
                    if stop {
                        if settlement_fails {
                            crate::storage::run_db_blocking({
                                let db = action_db.clone();
                                move || async move {
                                    db.execute_script("CREATE TRIGGER reject_stop BEFORE UPDATE OF state ON turns WHEN NEW.id='t' AND NEW.state='interrupted' BEGIN SELECT RAISE(ABORT,'injected stop settlement failure'); END;")
                                        .await.map_err(|error| error.to_string())
                                }
                            }).unwrap();
                        }
                        if stale_stop {
                            assert!(crate::transcripts::stream_store::insert_event(
                                action_db.clone(),
                                crate::transcripts::stream_store::EventInsert {
                                    id: "pending-call".into(),
                                    run_id: "r".into(),
                                    session_id: Some("s".into()),
                                    timestamp: 1,
                                    selected_model: None,
                                    event_type: "assistant".into(),
                                    data: json!({"eventType":"assistant","isError":false,"sessionId":"s","toolUses":[{"id":"pending-run","name":"run","input":{"commands":[]}}]}).to_string(),
                                    parent_tool_use_id: None,
                                    created_at: 1,
                                    input_tokens: None,
                                    cache_read_tokens: None,
                                    cache_create_tokens: None,
                                    output_tokens: None,
                                    thinking_tokens: None,
                                    turn_id: Some("t".into()),
                                    cost_usd: None,
                                },
                            ).unwrap());
                            let raced = action.clone();
                            let raced_db = action_db.clone();
                            let predecessor = owner.clone();
                            *action_emitter.on_db_change.lock().unwrap() = Some(Box::new(
                                move || {
                                    assert!(raced.job_launch_lock("j").try_lock().is_ok());
                                    assert!(raced
                                        .process_state
                                        .is_turn_owner_current(&predecessor));
                                    let answered = crate::storage::run_db_blocking({
                                        let db = raced_db.clone();
                                        move || async move {
                                            db.query_opt_i64(
                                                "SELECT COUNT(*) FROM events WHERE run_id='r' AND turn_id='t' AND event_type='tool_result'
                                                 AND json_extract(data,'$.toolUseId')='pending-run'
                                                 AND json_extract(data,'$.raw.synthetic')=1
                                                 AND json_extract(data,'$.raw.reason')='user_stop'",
                                                (),
                                            ).await.map_err(|error| error.to_string())
                                        }
                                    }).unwrap();
                                    assert_eq!(
                                        answered,
                                        Some(1),
                                        "inject only after Stop answers P's pending tool"
                                    );
                                    crate::storage::run_db_blocking(move || async move {
                                    raced_db.execute_script(
                                        "UPDATE turns SET state='complete' WHERE id='t';
                                         INSERT INTO turns(id,session_id,run_id,job_id,sequence,state,start_reason,created_at,started_at,updated_at)
                                         VALUES('successor','s','r','j',2,'running','attention',2,2,2);
                                         UPDATE jobs SET current_turn_id='successor' WHERE id='j';"
                                    ).await.map_err(|error| error.to_string())
                                }).unwrap();
                                    assert!(raced
                                        .process_state
                                        .retire_turn_accounting_if_owned(&predecessor));
                                    assert!(raced
                                        .process_state
                                        .transition_to_warm_if_owned(&predecessor));
                                    raced.process_state.begin_turn("r", "successor");
                                },
                            ));
                        }
                        let stopped =
                            crate::orchestrator::lifecycle::operator_stop_run(&action, "r");
                        if stale_stop {
                            assert!(
                                action_emitter.on_db_change.lock().unwrap().is_none(),
                                "the synthetic-result boundary must have fired"
                            );
                            assert_eq!(
                                action.process_state.get_current_turn_id("r").as_deref(),
                                Some("successor")
                            );
                        }
                        if settlement_fails || stale_stop {
                            let error = stopped.unwrap_err();
                            assert!(error.contains(if stale_stop {
                                "changed ownership"
                            } else {
                                "durably settled"
                            }));
                        } else {
                            stopped.unwrap();
                        }
                        if settlement_fails {
                            assert!(!owner.accounting_only());
                            assert_eq!(
                                action.process_state.get_current_turn_id("r").as_deref(),
                                Some("t")
                            );
                            crate::storage::run_db_blocking({
                                let db = action_db.clone();
                                move || async move {
                                    db.execute_script("DROP TRIGGER reject_stop;")
                                        .await
                                        .map_err(|error| error.to_string())
                                }
                            })
                            .unwrap();
                        }
                    } else {
                        crate::storage::run_db_blocking({
                            let db = action_db.clone();
                            move || async move {
                                db.execute_script(
                                    "UPDATE turns SET state='yielded' WHERE id='t';
                                     INSERT INTO agent_waits(id,job_id,run_id,session_id,predecessor_turn_id,tool_use_id,condition_json,state,created_at)
                                     VALUES('wait','j','r','s','t','tool','{\"kind\":\"duration\"}','pending',1);"
                                ).await.map_err(|error| error.to_string())
                            }
                        }).unwrap();
                        action.process_state.yield_for_host("r", "t");
                        for _ in 0..2 {
                            crate::orchestrator::lifecycle::park_owned_turn(
                                &action,
                                "r",
                                Some(&owner),
                            )
                            .unwrap();
                        }
                    }
                    assert_eq!(owner.accounting_only(), !settlement_fails);
                    assert_eq!(
                        action.process_state.get_current_turn_id("r").as_deref(),
                        if stale_stop {
                            Some("successor")
                        } else if settlement_fails {
                            Some("t")
                        } else {
                            None
                        }
                    );
                } else if phase == "attach" {
                    crate::storage::run_db_blocking({
                        let db = action_db.clone();
                        move || async move {
                            db.execute_script(
                                "UPDATE agent_waits SET state='resolved' WHERE id='wait';
                                 UPDATE turns SET state='complete' WHERE id='t';
                                 INSERT INTO turns(id,session_id,run_id,job_id,sequence,state,start_reason,created_at,started_at,updated_at)
                                 VALUES('successor','s','r','j',2,'running','wait_resolved',2,2,2);
                                 UPDATE jobs SET current_turn_id='successor' WHERE id='j';"
                            ).await.map_err(|error| error.to_string())
                        }
                    }).unwrap();
                    action.process_state.begin_turn("r", "successor");
                } else if phase == "verify" {
                    assert_eq!(
                        action.process_state.get_current_turn_id("r").as_deref(),
                        if successor { Some("successor") } else { None }
                    );
                    assert!(action.job_launch_lock("j").try_lock().is_ok());
                    let lease = crate::storage::run_db_blocking({
                        let db = action_db.clone();
                        move || async move {
                            db.query_opt_text("SELECT state FROM codex_watchdog_leases WHERE provider_turn_id='p1'", ())
                                .await.map_err(|error| error.to_string())
                        }
                    }).unwrap();
                    assert_eq!(lease.as_deref(), Some("disarmed"));
                    if successor {
                        let successor_lease = crate::storage::run_db_blocking({
                            let db = action_db.clone();
                            move || async move {
                                db.query_opt_text("SELECT state FROM codex_watchdog_leases WHERE provider_turn_id='p2'", ())
                                    .await.map_err(|error| error.to_string())
                            }
                        }).unwrap();
                        assert_eq!(successor_lease.as_deref(), Some("active"));
                    }
                    if successor {
                        let buffered = crate::storage::run_db_blocking({
                            let db = action_db.clone();
                            move || async move {
                                db.query_opt_i64(
                                    "SELECT COUNT(*) FROM events WHERE event_type='assistant' AND json_extract(data,'$.content')='successor buffer'",
                                    (),
                                )
                                .await
                                .map_err(|error| error.to_string())
                            }
                        })
                        .unwrap();
                        assert_eq!(buffered, Some(0), "P terminal must not flush S");
                    }
                }
            }));
            let usage = |id: &str, last: u32, total: u32| {
                json!({
                    "method":"thread/tokenUsage/updated","params":{"turnId":id,"tokenUsage":{
                        "last":{"inputTokens":last,"outputTokens":1,"totalTokens":last+1},
                        "total":{"inputTokens":total,"outputTokens":total/10}
                    }}
                })
            };
            let marker = |phase: &str| {
                json!({"method":"item/commandExecution/outputDelta",
                "params":{"itemId":"retirement-marker","delta":phase}})
            };
            let mut notifications = vec![
                json!({"method":"turn/started","params":{"turn":{"id":"p1"}}}),
                usage("p1", 100, 100),
                marker("retire"),
            ];
            if successor {
                notifications.extend([
                    marker(if stale_stop { "already-attached" } else { "attach" }),
                    json!({"method":"turn/started","params":{"turn":{"id":"p2"}}}),
                    usage("p2", 10, 130),
                    json!({"method":"item/agentMessage/delta","params":{"turnId":"p2","delta":"successor buffer"}}),
                ]);
            }
            let terminal = if completed {
                json!({"method":"turn/completed","params":{"turn":{"id":"p1","status":"completed"}}})
            } else {
                aborted("interrupted")
            };
            notifications.extend([
                usage("p1", 20, 120), terminal.clone(), terminal,
                aborted("interrupted"),
                json!({"method":"error","params":{"turnId":"p1","message":"capacity exceeded","willRetry":false}}),
                usage("p1", 999, 999), marker("verify"),
            ]);
            let (tx, rx) = crossbeam_channel::unbounded();
            for notification in notifications {
                tx.send(notification).unwrap();
            }
            drop(tx);
            let reader_emitter: Arc<dyn crate::services::EventEmitter> = Arc::new(emitter.clone());
            CodexBackend::reader_thread_app_server(
                &orch,
                &reader_emitter,
                "r",
                Some("s".into()),
                rx,
                client,
                provider,
                None,
                "codex".into(),
                db.clone(),
                None,
            );
            assert_eq!(db.query_opt_i64(
                "SELECT COUNT(*) FROM events WHERE turn_id='t' AND event_type LIKE 'result:%' AND input_tokens=120", ()
            ).await.unwrap(), Some(1));
            assert_eq!(
                db.query_opt_i64(
                    "SELECT COUNT(*) FROM events WHERE event_type LIKE 'result:%'",
                    ()
                )
                .await
                .unwrap(),
                Some(1),
                "replay cannot account again or charge S"
            );
            assert_eq!(
                turn_row(&db).await.as_deref(),
                Some(if stale_stop {
                    "complete/"
                } else if stop {
                    "interrupted/user_stop"
                } else if successor {
                    "complete/"
                } else {
                    "yielded/"
                })
            );
            assert_eq!(
                text(&db, "SELECT status FROM runs WHERE id='r'")
                    .await
                    .as_deref(),
                Some("live")
            );
            assert_eq!(
                orch.get_context_token_state("s").await.unwrap().used_tokens,
                if successor {
                    11
                } else if settlement_fails {
                    21
                } else {
                    101
                }
            );
            assert_eq!(
                db.query_opt_i64("SELECT COUNT(*) FROM provider_limit_holds", ())
                    .await
                    .unwrap(),
                Some(0)
            );
            assert_eq!(
                emitter
                    .events
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|event| *event == "run-turn-completed")
                    .count(),
                if stale_stop { 0 } else { 1 },
                "actual park/Stop publishes its idle edge once"
            );
            assert!(
                broadcasts.try_recv().is_err(),
                "retired work must not broadcast success or wake its parent"
            );
            if stop {
                assert_eq!(
                    text(&db, "SELECT goal_intent FROM jobs WHERE id='j'")
                        .await
                        .as_deref(),
                    Some("off")
                );
                if !stale_stop {
                    assert_eq!(
                        text(&db, "SELECT status FROM jobs WHERE id='j'")
                            .await
                            .as_deref(),
                        Some("idle")
                    );
                    if !completed && !settlement_fails {
                        db.execute_script(
                            "INSERT INTO turns(id,session_id,run_id,job_id,sequence,state,start_reason,end_reason,created_at,updated_at)
                             VALUES('refused','s',NULL,'j',2,'cancelled','attention','launch_refused',2,2);"
                        ).await.unwrap();
                        crate::execution::advancement::recompute_job(&orch, "j").unwrap();
                        assert_eq!(
                            text(&db, "SELECT status FROM jobs WHERE id='j'")
                                .await
                                .as_deref(),
                            Some("idle"),
                            "an unserved follow-up cannot revive the stopped retained run"
                        );
                        db.execute_script(
                            "INSERT INTO runs(id,project_id,job_id,status,session_id,created_at,updated_at)
                             VALUES('resume','p','j','starting','s',3,3);"
                        ).await.unwrap();
                        crate::execution::advancement::recompute_job(&orch, "j").unwrap();
                        assert_eq!(
                            text(&db, "SELECT status FROM jobs WHERE id='j'")
                                .await
                                .as_deref(),
                            Some("running"),
                            "a newly admitted run still witnesses the resume gap"
                        );
                        db.execute("UPDATE runs SET status='exited' WHERE id='resume'", ())
                            .await
                            .unwrap();
                        crate::execution::advancement::recompute_job(&orch, "j").unwrap();
                        assert_eq!(
                            text(&db, "SELECT status FROM jobs WHERE id='j'")
                                .await
                                .as_deref(),
                            Some("idle")
                        );
                        db.execute_script(
                            "INSERT INTO turns(id,session_id,run_id,job_id,sequence,state,start_reason,created_at,updated_at)
                             VALUES('review','s','r','j',3,'running','memory_review',4,4);"
                        ).await.unwrap();
                        crate::execution::advancement::recompute_job(&orch, "j").unwrap();
                        assert_eq!(
                            text(&db, "SELECT status FROM jobs WHERE id='j'")
                                .await
                                .as_deref(),
                            Some("running"),
                            "a genuinely live memory review remains work"
                        );
                        db.execute("UPDATE turns SET state='cancelled' WHERE id='review'", ())
                            .await
                            .unwrap();
                        crate::execution::advancement::recompute_job(&orch, "j").unwrap();
                        assert_eq!(
                            text(&db, "SELECT status FROM jobs WHERE id='j'")
                                .await
                                .as_deref(),
                            Some("idle"),
                            "a cancelled memory review cannot revive the stopped work turn"
                        );
                    }
                } else {
                    crate::execution::advancement::recompute_job(&orch, "j").unwrap();
                    assert_eq!(
                        text(&db, "SELECT status FROM jobs WHERE id='j'")
                            .await
                            .as_deref(),
                        Some("running"),
                        "the live successor overrides its stopped predecessor"
                    );
                }
            }
            assert!(requests
                .lock()
                .unwrap()
                .iter()
                .all(|request| request["method"] != "turn/start"
                    && request["params"]["status"] != "active"));
        }
    }

    /// A teardown's abort leaves the turn to the teardown's finalization, so
    /// the outcome cannot depend on whether the abort beat the process exit.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_teardown_abort_leaves_the_turn_to_the_teardown() {
        let (db, orch) = persistent_codex_turn("codex-teardown-abort", "").await;
        let (client, _) =
            client_that_interrupted(cairn_models::interface::session::InterruptCause::Teardown {
                exit_reason: "provider_silence_recovery".into(),
            });
        read_through(&orch, &db, client, vec![aborted("interrupted")]);
        assert_eq!(turn_row(&db).await.as_deref(), Some("running/"));
        assert_eq!(
            interrupted_record(&db).await.as_deref(),
            Some("teardown/interrupted/p1")
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn handoff_terminal_accounts_predecessor_once_without_touching_native_successor() {
        for (successor, finish_successor) in [(false, false), (true, false), (true, true)] {
            let (db, orch) = persistent_codex_turn("codex-late-handoff-accounting", "").await;
            assert!(orch.process_state.arm_terminal_tool("r"));
            let usage = |id: &str, last: u32, total: u32| {
                json!({
                    "method": "thread/tokenUsage/updated",
                    "params": {"turnId": id, "tokenUsage": {
                        "last": {"inputTokens":last, "outputTokens":1, "totalTokens":last+1},
                        "total": {"inputTokens":total, "outputTokens":total/10}
                    }}
                })
            };
            let mut notifications = vec![
                usage("p1", 100, 100),
                json!({"method":"item/completed","params":{"turnId":"p1","item":{
                    "type":"mcpToolCall","id":"return-1","server":"cairn","tool":"write",
                    "status":"completed","result":{"content":[]}
                }}}),
            ];
            if successor {
                notifications.extend([
                    json!({"method":"turn/started","params":{"turn":{"id":"p2"}}}),
                    usage("p2", 10, 130),
                    json!({"method":"item/agentMessage/delta","params":{"turnId":"p2","delta":"successor buffer"}}),
                ]);
            }
            // This older P total arrives after S's newer total. Its baseline
            // belongs to P; it must neither charge S nor rewind S's baseline.
            notifications.push(usage("p1", 20, 120));
            notifications.extend([
                aborted("interrupted"),
                aborted("interrupted"),
                json!({"method":"turn/completed","params":{"turn":{"id":"p1","status":"completed"}}}),
                json!({"method":"error","params":{"turnId":"p1","message":"capacity exceeded","willRetry":false}}),
                usage("p1", 999, 999),
            ]);
            if successor && !finish_successor {
                // S's stream spans P's late frames. Had any of them finalized
                // it, this tail would open a second assistant event.
                notifications.push(json!({"method":"item/agentMessage/delta","params":{"turnId":"p2","delta":" successor tail"}}));
            }
            if finish_successor {
                notifications.extend([
                    usage("p2", 10, 140),
                    json!({"method":"turn/completed","params":{"turn":{"id":"p2","status":"completed"}}}),
                ]);
            }
            read_notifications(&orch, &db, notifications);
            assert_eq!(
                turn_row(&db).await.as_deref(),
                Some("complete/artifact_handoff")
            );
            assert_eq!(db.query_opt_i64(
                "SELECT COUNT(*) FROM events WHERE turn_id='t' AND event_type LIKE 'result:%' AND input_tokens=120",
                (),
            ).await.unwrap(), Some(1));
            assert_eq!(
                db.query_opt_i64(
                    "SELECT COUNT(*) FROM events WHERE event_type LIKE 'result:%'",
                    (),
                )
                .await
                .unwrap(),
                Some(if finish_successor { 2 } else { 1 }),
                "replay must not insert another result or charge S"
            );
            assert_eq!(
                text(&db, "SELECT status FROM runs WHERE id='r'")
                    .await
                    .as_deref(),
                Some("live")
            );
            assert_eq!(
                db.query_opt_i64("SELECT COUNT(*) FROM provider_limit_holds", ())
                    .await
                    .unwrap(),
                Some(0)
            );
            let gauge = orch.get_context_token_state("s").await.unwrap();
            assert_eq!(gauge.used_tokens, if successor { 11 } else { 101 });
            if finish_successor {
                assert_eq!(db.query_opt_i64(
                    "SELECT input_tokens FROM events WHERE event_type='result:success' AND turn_id!='t'", (),
                ).await.unwrap(), Some(20), "S gets only its two 10-token responses");
                assert_eq!(orch.process_state.get_current_turn_id("r"), None);
            } else if successor {
                let owner = orch
                    .process_state
                    .capture_process_identity("r")
                    .and_then(|id| orch.process_state.capture_turn_owner_for_identity(&id))
                    .unwrap();
                assert_ne!(owner.turn_id(), "t");
                assert_eq!(text(&db, "SELECT state FROM turns WHERE id=(SELECT current_turn_id FROM jobs WHERE id='j')")
                    .await.as_deref(), Some("running"));
                // The stream closing under live S finalizes S's partial message
                // once; late P frames must not have split it beforehand.
                assert_eq!(db.query_opt_i64(
                    "SELECT COUNT(*) FROM events WHERE event_type='assistant' AND json_extract(data,'$.content') LIKE '%successor%'", (),
                ).await.unwrap(), Some(1), "late P must not finalize S's stream");
                assert_eq!(db.query_opt_i64(
                    "SELECT COUNT(*) FROM events WHERE event_type='assistant' AND json_extract(data,'$.content') LIKE 'successor buffer%successor tail'", (),
                ).await.unwrap(), Some(1), "S's one message carries both halves");
            } else {
                assert_eq!(orch.process_state.get_current_turn_id("r"), None);
            }
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn terminal_boundary_settles_before_native_successor_and_cannot_replay() {
        let (db, orch) = persistent_codex_turn("codex-terminal-boundary-owner", "").await;
        let identity = orch.process_state.capture_process_identity("r").unwrap();
        let owner = orch
            .process_state
            .capture_turn_owner_for_identity(&identity)
            .unwrap();
        let runtime = CodexBackend::session_runtime(
            &orch,
            "r",
            Some("s".into()),
            db.clone(),
            Arc::new(AppServerClient::for_test_scripted(
                vec![],
                Arc::new(Mutex::new(Vec::new())),
            )),
            None,
            None,
        );
        assert!(runtime.host.artifact_handoff(&identity, Some(&owner)));
        assert_eq!(
            turn_row(&db).await.as_deref(),
            Some("complete/artifact_handoff")
        );
        let Some(GoalTurnAdmission::Admitted { owner: successor }) =
            admit_native_goal_turn(&orch, &db, "r", Some("s"), "p2", &identity).unwrap()
        else {
            panic!("native successor must be admitted after the handoff");
        };
        assert_ne!(successor.turn_id(), owner.turn_id());
        assert!(!runtime.host.artifact_handoff(&identity, Some(&owner)));
        assert!(orch.process_state.is_turn_owner_current(&successor));
        assert_eq!(
            text(
                &db,
                &format!("SELECT state FROM turns WHERE id='{}'", successor.turn_id())
            )
            .await
            .as_deref(),
            Some("running")
        );
    }

    /// A terminal tool's handoff still completes as a handoff.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_terminal_tool_turn_still_completes_as_a_handoff() {
        let (db, orch) = persistent_codex_turn("codex-handoff-abort", "").await;
        orch.process_state
            .processes
            .lock()
            .unwrap()
            .get("r")
            .unwrap()
            .terminal_tool_called
            .store(true, std::sync::atomic::Ordering::Release);
        read_notifications(&orch, &db, vec![aborted("interrupted")]);
        assert_eq!(
            turn_row(&db).await.as_deref(),
            Some("complete/artifact_handoff")
        );
    }

    /// cairn/4634 test 5: the watchdog's interrupt raced a natural completion.
    /// The turn completes as itself, and the cause recorded for it is dropped
    /// with it rather than left to misattribute a later turn.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_turn_that_completes_while_its_interrupt_is_in_flight_stays_complete() {
        let (db, orch) = persistent_codex_turn("codex-interrupt-completion-race", "").await;
        let (client, _) = client_that_interrupted(
            cairn_models::interface::session::InterruptCause::WatchdogPostTool,
        );
        read_through(
            &orch,
            &db,
            client.clone(),
            vec![
                json!({"method":"turn/completed","params":{"turn":{"id":"p1","status":"completed"}}}),
            ],
        );
        assert_eq!(turn_row(&db).await.as_deref(), Some("complete/"));
        assert_eq!(client.take_interrupt_cause("p1"), None);
        assert_eq!(interrupted_record(&db).await, None);
    }

    fn provider_error(message: &str) -> Value {
        json!({"method": "error", "params": {
            "turnId": "p1",
            "error": {"message": message},
            "willRetry": false,
        }})
    }

    const CAPACITY: &str = "Selected model is at capacity. Please try a different model.";

    /// A run reused warm serves a newer provider turn (`p1`) when the terminal
    /// callbacks of an earlier one (`p0`), which Cairn already settled, arrive
    /// late. They name `p0`, so they neither end the delegated task's current
    /// turn nor record `p0`'s cause against the run; the same fatal error
    /// naming `p1` still fails it with its cause.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_late_terminal_callback_for_a_superseded_turn_leaves_the_run_and_its_cause_alone() {
        let stale = |turn: &str| {
            vec![
                json!({"method": "error", "params": {
                    "turnId": turn,
                    "error": {"message": "refresh_token_reused"},
                    "willRetry": false,
                }}),
                json!({"method": "turn/completed", "params": {"turn": {"id": turn, "status": "failed"}}}),
                json!({"method": "turn/aborted", "params": {"turnId": turn, "reason": "interrupted"}}),
            ]
        };
        let (db, orch) = persistent_codex_turn(
            "codex-superseded-terminal",
            "UPDATE jobs SET parent_job_id='pj' WHERE id='j';",
        )
        .await;
        read_notifications(&orch, &db, stale("p0"));
        assert_eq!(
            text(
                &db,
                "SELECT status || '/' || COALESCE(error_message,'none') FROM runs WHERE id='r'"
            )
            .await
            .as_deref(),
            Some("live/none"),
            "a superseded turn's callbacks neither end the run nor give it a cause"
        );
        assert_eq!(turn_row(&db).await.as_deref(), Some("running/"));

        let (db, orch) = persistent_codex_turn(
            "codex-current-terminal",
            "UPDATE jobs SET parent_job_id='pj' WHERE id='j';",
        )
        .await;
        read_notifications(&orch, &db, stale("p1"));
        assert_eq!(
            text(
                &db,
                "SELECT status || '/' || COALESCE(error_message,'none') FROM runs WHERE id='r'"
            )
            .await
            .as_deref(),
            Some("crashed/Codex error: refresh_token_reused"),
            "the current turn's fatal error fails the task with its cause"
        );
    }

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

    /// Once its retries are spent, a delegated agent holds rather than failing
    /// its parent (cairn/6069): at capacity is a want of supply that waiting
    /// cures. Codex declares no reset and none is inferred, so the hold
    /// re-checks on the bounded clock, and its parent stays parked on it,
    /// counting it as pending rather than gone, until that re-check (or a
    /// change to the accounts) releases it onto the same session.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_delegated_agent_whose_capacity_retries_are_spent_holds_and_its_parent_waits() {
        use crate::messages::delivery::{describe_hold, Hold};
        use crate::orchestrator::wakes::{
            load_active_provider_limit_holds, resolve_provider_limit_hold, SUPPLY_RECHECK_SECS,
        };

        let (db, orch) = persistent_codex_turn(
            "codex-capacity-delegated-spent",
            "UPDATE jobs SET parent_job_id='pj' WHERE id='j';",
        )
        .await;
        // `t` is already the third retry in a row, and the parent is parked on
        // the child's packet.
        db.execute_script(
            "INSERT INTO turns(id,session_id,job_id,sequence,state,start_reason,created_at,updated_at) VALUES('t0','s','j',-2,'interrupted','initial',0,0);
             INSERT INTO turns(id,session_id,job_id,sequence,predecessor_id,state,start_reason,created_at,updated_at) VALUES('ta','s','j',-1,'t0','interrupted','retry',0,0);
             INSERT INTO turns(id,session_id,job_id,sequence,predecessor_id,state,start_reason,created_at,updated_at) VALUES('tb','s','j',0,'ta','interrupted','retry',0,0);
             UPDATE turns SET start_reason='retry', predecessor_id='tb' WHERE id='t';
             INSERT INTO issues(id,project_id,number,title,status,progress,attention,created_at,updated_at) VALUES('i','p',1,'I','active','active','none',1,1);
             INSERT INTO executions(id,recipe_id,issue_id,project_id,status,started_at,seq) VALUES('e','recipe','i','p','running',1,1);
             INSERT INTO sessions(id,job_id,backend,status,sequence,created_at,updated_at) VALUES('ps','pj','codex','open',1,1,1);
             INSERT INTO turns(id,session_id,job_id,sequence,state,yield_reason,start_reason,created_at,updated_at) VALUES('pt','ps','pj',1,'yielded','dependency_wait','initial',1,1);
             UPDATE jobs SET current_turn_id='pt',current_session_id='ps' WHERE id='pj';
             INSERT INTO delegated_packets(execution_id,id,seq,parent_job_id,parent_turn_id,result_artifact_job_id,status,spec,created_at) VALUES('e','packet','1','pj','pt','j','running','{}',1);",
        )
        .await
        .unwrap();
        read_notifications(&orch, &db, vec![provider_error(CAPACITY)]);

        assert_eq!(
            text(
                &db,
                "SELECT state || '/' || COALESCE(yield_reason,'') FROM turns WHERE id='t'"
            )
            .await
            .as_deref(),
            Some("yielded/provider_limit"),
            "spent retries hold the turn, not fail it"
        );
        assert_eq!(
            db.query_opt_i64("SELECT COUNT(*) FROM turns WHERE predecessor_id='t'", ())
                .await
                .unwrap(),
            Some(0),
            "no fourth retry"
        );
        assert_ne!(
            text(&db, "SELECT status FROM jobs WHERE id='j'")
                .await
                .as_deref(),
            Some("failed")
        );
        assert_eq!(
            text(
                &db,
                "SELECT status FROM delegated_packets WHERE id='packet'"
            )
            .await
            .as_deref(),
            Some("running"),
            "the packet is not resolved, so the parent is not told the child is gone"
        );
        assert!(orch.process_state.is_warm("r"), "the process stays warm");

        let holds = load_active_provider_limit_holds(&db).await.unwrap();
        let [hold] = holds.as_slice() else {
            panic!("one active hold owns the yielded head: {holds:?}");
        };
        assert_eq!(
            (hold.job_id.as_str(), hold.turn_id.as_deref()),
            ("j", Some("t"))
        );
        assert_eq!(hold.retry_not_before, None, "no reset is inferred");
        let due = hold.updated_at + SUPPLY_RECHECK_SECS;
        assert_eq!(hold.next_due_at(), Some(due), "a bounded re-check");
        assert_eq!(
            db.query_opt_i64(
                "SELECT COUNT(*) FROM events WHERE data LIKE '%selected_model_capacity_hold%'",
                (),
            )
            .await
            .unwrap(),
            Some(1),
            "the hold says what it waits for and when it asks again"
        );

        assert_eq!(
            describe_hold(&db, "j").await,
            Some(Hold::ProviderLimit {
                retry_not_before: None,
                gate: None,
            }),
            "the child reads as held"
        );
        assert_eq!(
            describe_hold(&db, "pj").await,
            Some(Hold::Tasks { pending: 1 }),
            "the parent reads as waiting on it"
        );

        assert!(
            !resolve_provider_limit_hold(&orch, hold, due - 1)
                .await
                .unwrap(),
            "nothing releases it before the re-check"
        );
        assert!(
            resolve_provider_limit_hold(&orch, hold, due).await.unwrap(),
            "the re-check releases it"
        );
        assert_eq!(
            text(
                &db,
                "SELECT t.start_reason || '/' || t.session_id FROM jobs j JOIN turns t ON t.id=j.current_turn_id WHERE j.id='j'"
            )
            .await
            .as_deref(),
            Some("wait_resolved/s"),
            "it resumes on the same session"
        );
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

    /// A Codex turn refused by the provider holds only its prompt and the cause.
    /// The cause is stamped with the failing turn, so the digest, the turn read,
    /// the Markdown raw stream and a transcript grep all show it; before, only
    /// `chat/raw?format=json` did, and the turn read as having nothing to say.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_refused_turns_cause_reads_in_the_digest_the_turn_and_markdown_raw() {
        use crate::mcp::types::McpCallbackRequest;
        use crate::resources::produce_cairn_resource;

        const NODE: &str = "cairn://p/p/1/1/planner";
        const REFUSAL: &str =
            "The 'gpt-5.5' model is not supported when using Codex with a ChatGPT account.";
        let (db, orch) = persistent_codex_turn(
            "codex-refused-model-transcript",
            "INSERT INTO issues(id,project_id,number,title,status,progress,attention,created_at,updated_at) VALUES('i','p',1,'I','active','active','none',1,1);
             INSERT INTO executions(id,recipe_id,issue_id,project_id,status,started_at,seq) VALUES('e','recipe','i','p','running',1,1);
             UPDATE jobs SET execution_id='e', issue_id='i', uri_segment='planner' WHERE id='j';",
        )
        .await;
        db.execute(
            "INSERT INTO events(id,run_id,session_id,sequence,timestamp,event_type,data,created_at,turn_id) VALUES('ev-prompt','r','s',1,1,'user',?1,1,'t')",
            (json!({"content": "Plan the change."}).to_string(),),
        )
        .await
        .unwrap();
        read_notifications(&orch, &db, vec![provider_error(REFUSAL)]);

        assert_eq!(
            text(
                &db,
                "SELECT turn_id FROM events WHERE event_type='system:error'"
            )
            .await
            .as_deref(),
            Some("t"),
            "the cause belongs to the turn it failed"
        );
        let request = McpCallbackRequest {
            answer_bound_ms: None,
            thread_id: None,
            cwd: String::new(),
            run_id: None,
            tool: "read".to_string(),
            payload: json!({}),
            tool_use_id: None,
            origin: None,
        };
        for surface in ["chat", "chat/turn/1", "chat/raw"] {
            let read = produce_cairn_resource(&orch, &request, &format!("{NODE}/{surface}")).await;
            assert!(
                read.content.contains(REFUSAL),
                "{surface}: {}",
                read.content
            );
        }
        for surface in ["chat", "chat/turn/1"] {
            let found =
                produce_cairn_resource(&orch, &request, &format!("{NODE}/{surface}?grep=ChatGPT"))
                    .await;
            assert_eq!(found.match_count, Some(1), "{surface}: {}", found.content);
            assert!(
                found.content.contains("  error:\n"),
                "{surface} labels the field: {}",
                found.content
            );
        }
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
