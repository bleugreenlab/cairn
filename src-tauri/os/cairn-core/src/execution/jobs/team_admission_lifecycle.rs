use crate::agent_process::process::{AgentProcessState, ProcessAdmissionClaim};
use crate::orchestrator::Orchestrator;
use crate::runtime_admission::{
    AdmissionTerminalAction, FinishAdmissionRequest, RuntimeAdmissionClient, TeamAdmissionClient,
};
use crate::storage::{LocalDb, TeamOrdinaryAuthority};
use std::sync::Arc;
use std::time::Duration;

pub(super) fn team_client(orch: &Orchestrator) -> Result<Arc<dyn RuntimeAdmissionClient>, String> {
    let token = std::env::var("CAIRN_RUNNER_TOKEN")
        .map_err(|_| "CAIRN_RUNNER_TOKEN is unavailable".to_string())?;
    TeamAdmissionClient::with_api(orch.runtime_admission_api_config(), token)
        .map(|client| Arc::new(client) as Arc<dyn RuntimeAdmissionClient>)
        .map_err(|error| error.to_string())
}

pub(super) fn durable_lease_belongs_to_host(
    authority: &TeamOrdinaryAuthority,
    remote: Option<&crate::runtime_admission::AdmissionRequest>,
    current_host: &str,
) -> bool {
    authority.lease_host_id == current_host
        && remote.is_none_or(|remote| {
            remote.lease_generation == authority.lease_generation
                && remote.lease_host_id.as_deref() == Some(current_host)
                && matches!(remote.state.as_str(), "leased" | "starting" | "active")
        })
}

async fn heartbeat_once_if_current(
    process_state: &AgentProcessState,
    client: &dyn RuntimeAdmissionClient,
    run_id: &str,
    request_id: &str,
    generation: i64,
) -> Result<bool, String> {
    if !process_state.admission_claim_is_current(run_id, request_id, generation) {
        return Ok(false);
    }
    client
        .heartbeat(
            request_id,
            &crate::runtime_admission::HeartbeatAdmissionRequest {
                generation,
                lease_ms: 300_000,
            },
        )
        .await
        .map(|_| true)
        .map_err(|error| error.to_string())
}

fn action_from_str(value: &str) -> Option<AdmissionTerminalAction> {
    match value {
        "released" => Some(AdmissionTerminalAction::Released),
        "cancelled" => Some(AdmissionTerminalAction::Cancelled),
        "failed" => Some(AdmissionTerminalAction::Failed),
        _ => None,
    }
}

async fn finish_pending_once(
    db: &LocalDb,
    client: &dyn RuntimeAdmissionClient,
    pending: &TeamOrdinaryAuthority,
) -> Result<bool, String> {
    let action = pending
        .pending_terminal_action
        .as_deref()
        .and_then(action_from_str)
        .ok_or_else(|| "pending team admission has no valid terminal action".to_string())?;
    client
        .finish(
            &pending.request_id,
            &FinishAdmissionRequest {
                generation: pending.lease_generation,
                action,
                reason: None,
                code: None,
                detail: pending.pending_terminal_detail.clone(),
            },
        )
        .await
        .map_err(|error| error.to_string())?;
    crate::storage::clear_team_ordinary_terminal(db, &pending.request_id, pending.lease_generation)
        .await
        .map_err(|error| error.to_string())
}

fn spawn_terminal_retry(
    db: Arc<LocalDb>,
    client: Arc<dyn RuntimeAdmissionClient>,
    pending: TeamOrdinaryAuthority,
) {
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        return;
    };
    handle.spawn(async move {
        let mut delay = Duration::from_secs(1);
        loop {
            match finish_pending_once(&db, client.as_ref(), &pending).await {
                Ok(_) => return,
                Err(error) => {
                    log::warn!(
                        "team ordinary admission {} generation {} finish remains pending: {error}",
                        pending.request_id,
                        pending.lease_generation
                    );
                }
            }
            tokio::time::sleep(delay).await;
            delay = delay.saturating_mul(2).min(Duration::from_secs(60));
        }
    });
}

/// Persist the terminal obligation before the in-memory process claim is allowed
/// to disappear, then retry the exact remote generation independently of teardown.
pub(crate) fn journal_and_retry_terminal(
    orch: &Orchestrator,
    claim: &ProcessAdmissionClaim,
    action: AdmissionTerminalAction,
    detail: Option<&str>,
) -> Result<(), String> {
    let action = match action {
        AdmissionTerminalAction::Released => "released",
        AdmissionTerminalAction::Cancelled => "cancelled",
        AdmissionTerminalAction::Failed => "failed",
    };
    let db = orch.db.local.clone();
    super::run_db({
        let db = db.clone();
        let request_id = claim.request_id.clone();
        let detail = detail.map(str::to_string);
        let generation = claim.generation;
        async move {
            crate::storage::queue_team_ordinary_terminal(
                &db,
                &request_id,
                generation,
                action,
                detail.as_deref(),
                chrono::Utc::now().timestamp(),
            )
            .await
            .map_err(|error| error.to_string())
            .and_then(|changed| {
                changed
                    .then_some(())
                    .ok_or_else(|| "team admission generation was replaced before release".into())
            })
        }
    })?;
    if let Ok(client) = team_client(orch) {
        spawn_terminal_retry(
            db,
            client,
            TeamOrdinaryAuthority {
                request_id: claim.request_id.clone(),
                lease_owner: claim.owner.clone(),
                lease_generation: claim.generation,
                lease_host_id: claim.owner.clone(),
                pending_terminal_action: Some(action.into()),
                pending_terminal_detail: detail.map(str::to_string),
                updated_at: chrono::Utc::now().timestamp(),
            },
        );
    }
    Ok(())
}

const SETTLED_BEFORE_ADMISSION: &str = "run settled before admission";
const TEAM_CANCEL_TIMEOUT: Duration = Duration::from_secs(5);

/// What withdrawing a settled run's queued team request achieved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TeamWithdrawal {
    /// Postgres no longer queues the request, and the local mirror is retired.
    Withdrawn,
    /// Postgres holds the request outside the queue (leased, or requeued in a
    /// race), so its own lease lifecycle ends it. The mirror stays queued and
    /// the next sweep looks again.
    Pending,
}

/// Withdraw the queued team request of a run that already settled.
///
/// The local mirror row is the durable obligation: it is retired only after
/// Postgres acknowledges the cancellation or reports the request terminal. Any
/// failure leaves it `queued`, so the process sweep and startup recovery retry
/// until the authority answers. Retiring it first would strand a queued request
/// in Postgres that blocks younger work behind it, with nothing left locally to
/// retry from.
pub(super) async fn withdraw_settled_team_request_with(
    db: &LocalDb,
    client: &dyn RuntimeAdmissionClient,
    request_id: &str,
) -> Result<TeamWithdrawal, String> {
    use crate::runtime_admission::AdmissionClientError;
    let cancelled = tokio::time::timeout(
        TEAM_CANCEL_TIMEOUT,
        client.cancel(request_id, Some(SETTLED_BEFORE_ADMISSION)),
    )
    .await
    .map_err(|_| format!("team cancel of {request_id} timed out"))?;
    let gone = match cancelled {
        Ok(_) => true,
        // Not queued (or not found). Ask where it is instead of assuming.
        Err(AdmissionClientError::Rejected { status, .. }) if status.as_u16() == 409 => {
            let status = tokio::time::timeout(TEAM_CANCEL_TIMEOUT, client.status(request_id))
                .await
                .map_err(|_| format!("team status of {request_id} timed out"))?;
            match status {
                Ok(status) => matches!(
                    status.request.state.as_str(),
                    "released" | "cancelled" | "failed"
                ),
                Err(AdmissionClientError::Rejected { status, .. }) if status.as_u16() == 404 => {
                    true
                }
                Err(error) => return Err(format!("team status of {request_id} failed: {error}")),
            }
        }
        Err(error) => return Err(format!("team cancel of {request_id} failed: {error}")),
    };
    if !gone {
        return Ok(TeamWithdrawal::Pending);
    }
    // From any phase before registration: a lease that was journaled but never
    // started is as finished as a queued request once Postgres has let it go.
    crate::storage::retire_team_mirror(
        db,
        request_id,
        SETTLED_BEFORE_ADMISSION,
        chrono::Utc::now().timestamp(),
    )
    .await
    .map_err(|error| error.to_string())?;
    Ok(TeamWithdrawal::Withdrawn)
}

pub(super) fn withdraw_settled_team_request(
    orch: &Orchestrator,
    request_id: &str,
) -> Result<TeamWithdrawal, String> {
    let client = team_client(orch)?;
    let db = orch.db.local.clone();
    let request_id = request_id.to_string();
    super::run_db(async move {
        withdraw_settled_team_request_with(&db, client.as_ref(), &request_id).await
    })
}

pub(super) async fn retry_pending_terminals_on_startup(orch: &Orchestrator) {
    let pending = match crate::storage::list_pending_team_ordinary_terminals(&orch.db.local).await {
        Ok(pending) => pending,
        Err(error) => {
            log::warn!("could not recover pending team ordinary releases: {error}");
            return;
        }
    };
    let Ok(client) = team_client(orch) else {
        return;
    };
    for item in pending {
        spawn_terminal_retry(orch.db.local.clone(), client.clone(), item);
    }
}

pub(super) fn start_heartbeat(
    orch: &Orchestrator,
    run_id: String,
    request_id: String,
    generation: i64,
) {
    let Ok(client) = team_client(orch) else {
        return;
    };
    let process_state = orch.process_state.clone();
    spawn_heartbeat(process_state, client, run_id, request_id, generation);
}

fn spawn_heartbeat(
    process_state: Arc<AgentProcessState>,
    client: Arc<dyn RuntimeAdmissionClient>,
    run_id: String,
    request_id: String,
    generation: i64,
) {
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        return;
    };
    handle.spawn(async move {
        let mut delay = Duration::from_secs(60);
        loop {
            tokio::time::sleep(delay).await;
            match heartbeat_once_if_current(
                &process_state,
                client.as_ref(),
                &run_id,
                &request_id,
                generation,
            )
            .await
            {
                Ok(false) => return,
                Ok(true) => delay = Duration::from_secs(60),
                Err(error) => {
                    log::warn!(
                        "team ordinary admission {request_id} generation {generation} heartbeat failed; retrying while process remains current: {error}"
                    );
                    delay = Duration::from_secs(5);
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_admission::{
        AdmissionClientError, AdmissionPriority, AdmissionRequest, AdmissionStatus,
        EnqueueAdmissionRequest, HeartbeatAdmissionRequest, LeaseAdmissionRequest,
    };
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Mutex;

    #[derive(Default)]
    struct FakeClient {
        unavailable: AtomicBool,
        heartbeats: Mutex<Vec<i64>>,
        finishes: Mutex<Vec<i64>>,
        /// Postgres's view of the request, for cancel and status.
        remote_state: Mutex<String>,
    }

    fn remote_request(generation: i64, host: &str) -> AdmissionRequest {
        AdmissionRequest {
            id: "request-1".into(),
            launch_key: "ordinary:run-1".into(),
            workspace_id: "team-1".into(),
            backend_lane: "codex".into(),
            runtime_class: "dedicated_process".into(),
            resource_claim: serde_json::json!({"resident_process_units":1}),
            priority: AdmissionPriority::Ordinary,
            state: "leased".into(),
            lease_generation: generation,
            lease_owner: Some("runner-1".into()),
            lease_runner_id: Some("runner-1".into()),
            lease_host_id: Some(host.into()),
            lease_expires_at: None,
            admission_policy_revision: None,
            admission_capacity: None,
        }
    }

    #[async_trait::async_trait]
    impl RuntimeAdmissionClient for FakeClient {
        async fn enqueue(
            &self,
            _: &EnqueueAdmissionRequest,
        ) -> Result<AdmissionRequest, AdmissionClientError> {
            unreachable!()
        }
        async fn lease(
            &self,
            _: &LeaseAdmissionRequest,
        ) -> Result<Option<AdmissionRequest>, AdmissionClientError> {
            unreachable!()
        }
        async fn heartbeat(
            &self,
            _: &str,
            request: &HeartbeatAdmissionRequest,
        ) -> Result<AdmissionRequest, AdmissionClientError> {
            self.heartbeats.lock().unwrap().push(request.generation);
            if self.unavailable.load(Ordering::SeqCst) {
                Err(AdmissionClientError::Unavailable("outage".into()))
            } else {
                Ok(remote_request(request.generation, "host-1"))
            }
        }
        async fn finish(
            &self,
            _: &str,
            request: &FinishAdmissionRequest,
        ) -> Result<AdmissionRequest, AdmissionClientError> {
            self.finishes.lock().unwrap().push(request.generation);
            if self.unavailable.load(Ordering::SeqCst) {
                Err(AdmissionClientError::Unavailable(
                    "ambiguous finish outage".into(),
                ))
            } else {
                Ok(remote_request(request.generation, "host-1"))
            }
        }
        async fn cancel(
            &self,
            _: &str,
            _: Option<&str>,
        ) -> Result<AdmissionRequest, AdmissionClientError> {
            if self.unavailable.load(Ordering::SeqCst) {
                return Err(AdmissionClientError::Unavailable("outage".into()));
            }
            let mut state = self.remote_state.lock().unwrap();
            if *state != "queued" {
                return Err(AdmissionClientError::Rejected {
                    status: reqwest::StatusCode::CONFLICT,
                    message: "Request is not queued or not found".into(),
                });
            }
            *state = "cancelled".into();
            let mut request = remote_request(0, "host-1");
            request.state = state.clone();
            Ok(request)
        }
        async fn status(&self, _: &str) -> Result<AdmissionStatus, AdmissionClientError> {
            let mut request = remote_request(1, "host-1");
            request.state = self.remote_state.lock().unwrap().clone();
            Ok(AdmissionStatus {
                request,
                reason: None,
                position: None,
                behind_count: None,
                enqueue_age_ms: 0,
                effective_claim: serde_json::Value::Null,
                effective_capacity: None,
                runtime_host: None,
                host_constraint: None,
            })
        }
        async fn reap(&self) -> Result<Vec<AdmissionRequest>, AdmissionClientError> {
            unreachable!()
        }
    }

    fn claim(generation: i64) -> ProcessAdmissionClaim {
        ProcessAdmissionClaim {
            request_id: "request-1".into(),
            workspace_key: "team:team-1".into(),
            launch_key: "ordinary:run-1".into(),
            owner: "runner-1".into(),
            generation,
            team_workspace_id: Some("team-1".into()),
        }
    }

    async fn queued_team_mirror(name: &str) -> LocalDb {
        let db = crate::storage::migrated_test_db(name).await;
        db.execute_script(
            "INSERT INTO runtime_admission_requests(request_id,workspace_key,resource_lane,launch_key,claim_json,priority,enqueue_seq,state,lease_generation,created_at,updated_at)
               VALUES('request-1','team:team-1','codex','ordinary:run-1','{\"residentProcessUnits\":1,\"logicalStreamUnits\":1}',0,1,'queued',0,1,1);",
        )
        .await
        .unwrap();
        db
    }

    async fn local_state(db: &LocalDb) -> String {
        crate::storage::get_admission_request(db, "request-1")
            .await
            .unwrap()
            .unwrap()
            .state
    }

    /// A cancellation that never reached Postgres must leave the obligation in
    /// place: the mirror stays queued for the next sweep or restart to retry,
    /// and only an acknowledged cancel retires it, freeing the remote queue for
    /// the younger requests behind it.
    #[tokio::test]
    async fn a_failed_team_cancel_keeps_the_mirror_queued_until_postgres_acknowledges() {
        let db = queued_team_mirror("team-withdraw-retry.db").await;
        let client = FakeClient::default();
        *client.remote_state.lock().unwrap() = "queued".into();
        client.unavailable.store(true, Ordering::SeqCst);

        assert!(
            withdraw_settled_team_request_with(&db, &client, "request-1")
                .await
                .is_err()
        );
        assert_eq!(
            local_state(&db).await,
            "queued",
            "the obligation survives the outage"
        );
        assert_eq!(*client.remote_state.lock().unwrap(), "queued");

        // The next sweep, or startup recovery after a restart, retries it.
        client.unavailable.store(false, Ordering::SeqCst);
        assert_eq!(
            withdraw_settled_team_request_with(&db, &client, "request-1").await,
            Ok(TeamWithdrawal::Withdrawn)
        );
        assert_eq!(local_state(&db).await, "cancelled");
        assert_eq!(
            *client.remote_state.lock().unwrap(),
            "cancelled",
            "Postgres no longer queues it, so younger requests can win"
        );
    }

    /// A request Postgres has leased is ended by its own lease lifecycle, so the
    /// mirror waits rather than being retired; once Postgres reports it terminal,
    /// the mirror is retired too.
    #[tokio::test]
    async fn a_team_request_leased_elsewhere_is_left_to_its_lease() {
        let db = queued_team_mirror("team-withdraw-leased.db").await;
        let client = FakeClient::default();
        *client.remote_state.lock().unwrap() = "leased".into();
        assert_eq!(
            withdraw_settled_team_request_with(&db, &client, "request-1").await,
            Ok(TeamWithdrawal::Pending)
        );
        assert_eq!(local_state(&db).await, "queued");

        *client.remote_state.lock().unwrap() = "released".into();
        assert_eq!(
            withdraw_settled_team_request_with(&db, &client, "request-1").await,
            Ok(TeamWithdrawal::Withdrawn)
        );
        assert_eq!(local_state(&db).await, "cancelled");
    }

    #[tokio::test]
    async fn heartbeat_outage_retries_only_while_exact_generation_is_current() {
        let state = AgentProcessState::default();
        state.prepare_admission_claim("run-1".into(), claim(7));
        let client = FakeClient::default();
        client.unavailable.store(true, Ordering::SeqCst);
        assert!(
            heartbeat_once_if_current(&state, &client, "run-1", "request-1", 7)
                .await
                .is_err()
        );
        client.unavailable.store(false, Ordering::SeqCst);
        assert!(
            heartbeat_once_if_current(&state, &client, "run-1", "request-1", 7)
                .await
                .unwrap()
        );
        state.prepare_admission_claim("run-1".into(), claim(8));
        assert!(
            !heartbeat_once_if_current(&state, &client, "run-1", "request-1", 7)
                .await
                .unwrap()
        );
        assert_eq!(*client.heartbeats.lock().unwrap(), vec![7, 7]);
    }

    #[test]
    fn restart_reuses_only_same_generation_on_same_host() {
        let authority = TeamOrdinaryAuthority {
            request_id: "request-1".into(),
            lease_owner: "runner-1".into(),
            lease_generation: 7,
            lease_host_id: "host-1".into(),
            pending_terminal_action: None,
            pending_terminal_detail: None,
            updated_at: 1,
        };
        assert!(durable_lease_belongs_to_host(&authority, None, "host-1"));
        assert!(durable_lease_belongs_to_host(
            &authority,
            Some(&remote_request(7, "host-1")),
            "host-1"
        ));
        assert!(!durable_lease_belongs_to_host(
            &authority,
            Some(&remote_request(8, "host-1")),
            "host-1"
        ));
        assert!(!durable_lease_belongs_to_host(
            &authority,
            Some(&remote_request(7, "lost-host")),
            "host-1"
        ));
        assert!(!durable_lease_belongs_to_host(
            &authority,
            None,
            "replacement-host"
        ));
    }

    #[tokio::test]
    async fn ambiguous_finish_remains_durable_and_retries_exact_generation() {
        let db = crate::storage::migrated_test_db("team-terminal-retry.db").await;
        db.execute_script(
            "INSERT INTO runtime_admission_requests(request_id,workspace_key,resource_lane,launch_key,claim_json,priority,enqueue_seq,state,created_at,updated_at) VALUES('request-1','team:team-1','codex','ordinary:run-1','{}',0,1,'queued',1,1);\n\
             INSERT INTO prepared_ordinary_launches(request_id,job_id,run_id,session_id,turn_id,execution_id,launch_event_id,prompt,reason,session_start_json,model,agent_config_json,runtime_capability_json,created_at) VALUES('request-1','job-1','run-1','session-1','turn-1','','event-1','hello','initial','{}','','null','{}',1);"
        ).await.unwrap();
        crate::storage::persist_team_ordinary_lease(
            &db,
            TeamOrdinaryAuthority {
                request_id: "request-1".into(),
                lease_owner: "runner-1".into(),
                lease_generation: 7,
                lease_host_id: "host-1".into(),
                pending_terminal_action: None,
                pending_terminal_detail: None,
                updated_at: 1,
            },
        )
        .await
        .unwrap();
        crate::storage::queue_team_ordinary_terminal(&db, "request-1", 7, "released", None, 2)
            .await
            .unwrap();
        let pending = crate::storage::list_pending_team_ordinary_terminals(&db)
            .await
            .unwrap()
            .pop()
            .unwrap();
        let client = FakeClient::default();
        client.unavailable.store(true, Ordering::SeqCst);
        assert!(finish_pending_once(&db, &client, &pending).await.is_err());
        assert_eq!(
            crate::storage::list_pending_team_ordinary_terminals(&db)
                .await
                .unwrap()
                .len(),
            1
        );
        client.unavailable.store(false, Ordering::SeqCst);
        assert!(finish_pending_once(&db, &client, &pending).await.unwrap());
        assert!(crate::storage::list_pending_team_ordinary_terminals(&db)
            .await
            .unwrap()
            .is_empty());
        assert_eq!(*client.finishes.lock().unwrap(), vec![7, 7]);
    }
}
