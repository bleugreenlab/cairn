use super::permissions::{
    decline_codex_native_file_change, handle_codex_approval_request,
    handle_codex_mcp_server_elicitation_request,
};
use super::CodexBackend;
use crate::agent_process::completion::{SessionTurnCompletion, SessionTurnOutcome};
use crate::agent_process::stream::TranscriptEvent;
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
                                AND j.parent_job_id IS NULL
                                AND j.recipe_node_id IS NULL
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
        insert_error_event(
            orch,
            run_id,
            session_id,
            "The selected Codex model remained at capacity after three automatic retries. You can continue this job manually to try again.",
        );
        crate::orchestrator::lifecycle::fail_run(orch, run_id, "turn_failed");
        return true;
    }

    crate::orchestrator::lifecycle::fail_run(orch, run_id, "capacity_retry");
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
        backend_key: String,
        run_db: Arc<LocalDb>,
        ephemeral_cleanup: Option<super::pool::PooledCall>,
    ) {
        let ephemeral = ephemeral_cleanup.is_some();
        let host = codex_runtime(
            super::super::transcript_host::SessionTranscriptHost {
                orch: orch.clone(),
                emitter: emitter.clone(),
                run_id: run_id.to_string(),
                session_id: session_id.clone(),
                run_db: run_db.clone(),
            },
            Some(client),
            oauth_state,
            ephemeral_cleanup,
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
    cleanup: Option<super::pool::PooledCall>,
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
#[cfg(test)]
pub(crate) fn observe_and_interrupt_for_test(
    orch: &crate::orchestrator::Orchestrator,
    run_id: &str,
    session_id: &str,
    run_db: Arc<LocalDb>,
    cairn_turn_id: &str,
    provider_turn_id: &str,
    text: &str,
) -> Result<bool, String> {
    let host = CodexReaderHost {
        session: super::super::transcript_host::SessionTranscriptHost {
            orch: orch.clone(),
            emitter: orch.services.emitter.clone(),
            run_id: run_id.to_string(),
            session_id: Some(session_id.to_string()),
            run_db,
        },
        client: None,
        oauth_state: None,
        cleanup: Mutex::new(None),
    };
    if host
        .observe_final_output(cairn_turn_id, provider_turn_id, text)
        .is_some()
    {
        host.interrupt()?;
        Ok(true)
    } else {
        Ok(false)
    }
}

struct CodexReaderHost {
    session: super::super::transcript_host::SessionTranscriptHost,
    client: Option<Arc<AppServerClient>>,
    oauth_state: Option<Arc<Mutex<CodexAuthState>>>,
    cleanup: Mutex<Option<super::pool::PooledCall>>,
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
        match method {
            Some("item/commandExecution/requestApproval") => handle_codex_approval_request(
                orch,
                &run_db,
                run_id,
                "codex/command_execution",
                msg.get("params").cloned().unwrap_or(Value::Null),
                client.as_ref(),
                id_value,
                true,
            ),
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
            Some("mcpServer/elicitation/request") => handle_codex_mcp_server_elicitation_request(
                orch,
                &run_db,
                run_id,
                msg.get("params").cloned().unwrap_or(Value::Null),
                client.as_ref(),
                id_value,
            ),
            Some("item/tool/call") => {
                log::debug!("Codex requested dynamic tool call — declining");
                client.respond_error(id_value, -32601, "Dynamic tool calls are not supported")
            }
            Some("item/tool/requestUserInput") | Some("tool/requestUserInput") => {
                log::debug!("Codex requested interactive input — declining");
                client.respond_error(id_value, -32601, "Interactive prompts are not supported")
            }
            Some(other) => {
                log::warn!("Unhandled Codex server request: {}", other);
                client.respond_error(id_value, -32601, "Unsupported request")
            }
            None => Ok(()),
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
    fn observe_final_output(
        &self,
        cairn_turn_id: &str,
        provider_turn_id: &str,
        text: &str,
    ) -> Option<TranscriptEvent> {
        let run_db = self.session.run_db.clone();
        let run_id = self.session.run_id.as_str();
        let session_id = self.session.session_id.as_deref();
        let session_id = session_id?;
        match crate::execution::jobs::observe_codex_final_output(
            run_db.clone(),
            run_id,
            session_id,
            cairn_turn_id,
            provider_turn_id,
            text,
        ) {
            Ok(Some(breaker)) => {
                let evidence = if breaker.trigger == "three_consecutive_message_only_goal_turns" {
                    format!(
                                                "Codex safety pause: {} consecutive goal turns produced only a final message with no tool use (latest: {}). Resume explicitly to continue.",
                                                breaker.repeat_count, breaker.preview
                                            )
                } else {
                    format!(
                                                "Codex safety pause: the same finalized response repeated {} times ({}). Resume explicitly to continue.",
                                                breaker.repeat_count, breaker.preview
                                            )
                };
                let notice = TranscriptEvent {
                    event_type: "system:notice".to_string(),
                    session_id: Some(session_id.to_string()),
                    parent_tool_use_id: None,
                    content: Some(evidence),
                    thinking: None,
                    tool_name: None,
                    tool_input: None,
                    tool_uses: None,
                    tool_use_id: None,
                    tool_result: None,
                    is_error: false,
                    thinking_ms: None,
                    queued_message_id: None,
                    raw: Some(serde_json::json!({
                        "kind": "codex_repeat_circuit_breaker",
                        "resumable": true,
                        "normalizedContentDigest": breaker.digest,
                    })),
                };
                Some(notice)
            }
            Ok(None) => None,
            Err(error) => {
                log::error!(
                    "Failed to observe finalized Codex output for {provider_turn_id}: {error}"
                );
                None
            }
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
                crate::execution::jobs::CodexGoalTurnAdmission::SafetyPaused => {
                    GoalTurnAdmission::SafetyPaused
                }
            })
        })
    }
    fn defer_goal_turn(&self, turn_id: &str) -> Result<(), String> {
        crate::backends::stdin::defer_codex_goal_turn(
            &self.session.orch.process_state,
            &self.session.run_id,
            turn_id,
            // This callback rejects safety-paused turns; ordering-only deferrals
            // are handled separately by admit_native_goal_turn.
            crate::agent_process::process::PauseIntent::Stake(
                crate::agent_process::process::GoalPause::SafetyBrake,
            ),
        )
    }
    fn interrupt(&self) -> Result<(), String> {
        crate::backends::stdin::send_interrupt(
            &self.session.orch.process_state,
            &self.session.run_id,
            crate::agent_process::process::PauseIntent::Stake(
                crate::agent_process::process::GoalPause::SafetyBrake,
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
            cleanup.deregister();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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

    async fn event_count(db: &LocalDb, event_type: &str) -> i64 {
        let event_type = event_type.to_string();
        db.read(|conn| {
            let event_type = event_type.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT COUNT(*) FROM events WHERE event_type = ?1",
                        (event_type.as_str(),),
                    )
                    .await?;
                rows.next().await?.unwrap().i64(0)
            })
        })
        .await
        .unwrap()
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

    #[tokio::test(flavor = "multi_thread")]
    async fn third_message_only_goal_turn_denies_raced_fourth_and_explicitly_rearms() {
        let db = Arc::new(crate::storage::migrated_test_db("scripted-repeat-boundary").await);
        db.execute_script(
            "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at)
               VALUES('p','w','P','p','/tmp/p',1,1);
             INSERT INTO issues(id,project_id,number,title,status,attention,created_at,updated_at)
               VALUES('i','p',1,'Loop','active','none',1,1);
             INSERT INTO jobs(id,issue_id,project_id,status,node_name,current_session_id,created_at,updated_at)
               VALUES('j','i','p','running','builder','s',1,1);
             INSERT INTO sessions(id,job_id,backend,backend_id,status,sequence,created_at,updated_at)
               VALUES('s','j','codex','native-thread','open',1,1,1);
             INSERT INTO runs(id,project_id,issue_id,job_id,status,session_id,created_at,updated_at)
               VALUES('r','p','i','j','live','s',1,1);
             INSERT INTO turns(id,session_id,run_id,job_id,sequence,state,start_reason,created_at,updated_at)
               VALUES('t0','s','r','j',1,'running','initial',1,1);
             UPDATE jobs SET current_turn_id='t0' WHERE id='j';",
        )
        .await
        .unwrap();

        let goal = |status: &str| {
            json!({"result":{"goal":{
                "threadId":"native-thread",
                "objective":"finish assigned issue",
                "status":status,
                "tokenBudget":null,
                "tokensUsed":0,
                "timeUsedSeconds":0,
                "createdAt":1,
                "updatedAt":2
            }}})
        };
        let requests = Arc::new(Mutex::new(Vec::new()));
        let client = Arc::new(AppServerClient::for_test_scripted(
            vec![
                // Each pause checks terminal status before writing.
                Ok(goal("paused")),
                Ok(goal("paused")),
                Ok(goal("paused")),
                Ok(goal("paused")),
                Ok(goal("paused")),
                Ok(goal("paused")),
                Ok(Value::Null),
                Ok(goal("paused")),
                Ok(goal("paused")),
                Ok(Value::Null),
                Ok(goal("paused")),
                Ok(goal("active")),
                Ok(json!({"result":{"turn":{"id":"p5"}}})),
            ],
            requests.clone(),
        ));
        let provider_turn = Arc::new(Mutex::new(Some("p1".to_string())));
        let stdin = cairn_models::session::codex::CodexControl::new(
            client.clone(),
            "native-thread".to_string(),
            "/tmp/p".to_string(),
            None,
            None,
            None,
            provider_turn.clone(),
        );
        let orch = scripted_test_orchestrator(db.clone());
        let mut handle = RunHandle::new(
            Arc::new(Mutex::new(None)),
            Arc::new(Mutex::new(Some(Box::new(stdin)))),
            Some("s".to_string()),
            Some("j".to_string()),
        );
        handle.backend = Some("codex".to_string());
        handle.begin_turn("t0");
        orch.process_state
            .processes
            .lock()
            .unwrap()
            .register("r".to_string(), handle);
        orch.process_state.set_current_turn_id("r", Some("t0"));

        let (tx, rx) = crossbeam_channel::unbounded();
        let specimen = [
            "Blocked pending recovery of the recipe's existing independent-review node",
            "The review-orchestration blocker remains unchanged",
            "No recovery event has arrived...",
        ];
        for (provider_turn_id, text) in ["p1", "p2", "p3"].into_iter().zip(specimen) {
            tx.send(json!({
                "method":"turn/started",
                "params":{"turn":{"id":provider_turn_id,"status":"inProgress"}}
            }))
            .unwrap();
            tx.send(json!({
                "method":"rawResponseItem/completed",
                "params":{
                    "turnId":provider_turn_id,
                    "item":{"type":"message","role":"assistant","content":[
                        {"type":"output_text","text":text}
                    ]}
                }
            }))
            .unwrap();
            tx.send(json!({
                "method":"turn/completed",
                "params":{"turn":{"id":provider_turn_id,"status":"completed"}}
            }))
            .unwrap();
        }
        tx.send(json!({
            "method":"turn/started",
            "params":{"turn":{"id":"p4","status":"inProgress"}}
        }))
        .unwrap();
        drop(tx);

        let emitter = orch.services.emitter.clone();
        CodexBackend::reader_thread_app_server(
            &orch,
            &emitter,
            "r",
            Some("s".to_string()),
            rx,
            client,
            provider_turn,
            None,
            "codex".to_string(),
            db.clone(),
            None,
        );

        assert_eq!(
            event_count(&db, "system:codex_repeat_circuit_breaker").await,
            1
        );
        assert_eq!(event_count(&db, "system:notice").await, 1);
        assert_eq!(
            db.query_opt_text(
                "SELECT data FROM events WHERE event_type='system:error' ORDER BY sequence LIMIT 1",
                (),
            )
            .await
            .unwrap(),
            None,
            "the scripted lifecycle must not emit an Error/crash"
        );
        assert_eq!(
            db.query_opt_text("SELECT status FROM runs WHERE id='r'", ())
                .await
                .unwrap()
                .as_deref(),
            Some("live")
        );
        assert!(
            db.query_opt_text(
                "SELECT turn.id
                   FROM turns turn
                   JOIN events breaker ON breaker.turn_id = turn.id
                  WHERE breaker.event_type='system:codex_repeat_circuit_breaker'
                    AND turn.state='yielded'
                    AND turn.yield_reason='safety_pause'",
                (),
            )
            .await
            .unwrap()
            .is_some(),
            "the breaker must yield the third provider turn as a safety pause"
        );
        {
            let sent = requests.lock().unwrap();
            assert_eq!(
                sent.iter()
                    .map(|request| request["method"].as_str().unwrap())
                    .collect::<Vec<_>>(),
                vec![
                    "thread/goal/get",
                    "thread/goal/set",
                    "thread/goal/get",
                    "thread/goal/set",
                    "thread/goal/get",
                    "thread/goal/set",
                    "turn/interrupt",
                    "thread/goal/get",
                    "thread/goal/set",
                    "turn/interrupt",
                ]
            );
            assert_eq!(sent[1]["params"]["status"], "paused");
            assert_eq!(sent[3]["params"]["status"], "paused");
            assert_eq!(sent[5]["params"]["status"], "paused");
            assert_eq!(sent[6]["params"]["turnId"], "p3");
            assert_eq!(sent[8]["params"]["status"], "paused");
            assert_eq!(sent[9]["params"]["turnId"], "p4");
            assert!(sent.iter().all(|request| request["method"] != "turn/start"));
        }

        assert!(crate::execution::jobs::rearm_codex_repeat_breaker(db.clone(), "j", "s").unwrap());
        crate::backends::stdin::rearm_goal_safety_brake(&orch.process_state, "r").unwrap();
        assert!(matches!(
            crate::backends::stdin::reactivate_goal(
                &orch.process_state,
                "r",
                Some(&crate::execution::jobs::GoalReactivation::Operator {
                    kind: "explicit_resume",
                    id: None,
                }),
                crate::execution::jobs::JobRunnable::Unknown,
            )
            .unwrap(),
            crate::backends::stdin::GoalReactivationResult::Lifted { .. }
        ));
        crate::backends::stdin::send_user_message(
            &orch.process_state,
            "r",
            &crate::agent_process::stdin::MessageContent::text("resume explicitly"),
            "s",
            None,
        )
        .expect("explicit attention resumes the same warm provider thread");
        {
            let sent = requests.lock().unwrap();
            assert_eq!(sent[10]["method"], "thread/goal/get");
            assert_eq!(sent[11]["method"], "thread/goal/set");
            assert_eq!(sent[11]["params"]["status"], "active");
            assert_eq!(sent[12]["method"], "turn/start");
            assert_eq!(sent[12]["params"]["threadId"], "native-thread");
        }
        assert_eq!(
            db.query_opt_text("SELECT backend_id FROM sessions WHERE id='s'", ())
                .await
                .unwrap()
                .as_deref(),
            Some("native-thread")
        );
        assert!(
            crate::execution::jobs::observe_codex_final_output(
                db.clone(),
                "r",
                "s",
                "t0",
                "p5",
                "Complete.",
            )
            .unwrap()
            .is_none(),
            "the resumed thread starts a fresh repetition window"
        );
        let repeat_count = db
            .query_opt(
                "SELECT json_extract(data,'$.repeatCount')
                   FROM events
                  WHERE event_type='system:codex_final_output_observed'
                  ORDER BY sequence DESC LIMIT 1",
                (),
                |row| row.i64(0),
            )
            .await
            .unwrap();
        assert_eq!(repeat_count, Some(1));
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
