use super::{run_db, start_ordinary_session, PreparedJob};
use crate::models::{AgentConfig, Model, OutputSchemaInfo};
use crate::orchestrator::attention_push::TurnDelivery;
use crate::orchestrator::Orchestrator;
use crate::runtime_admission::RuntimeAdmissionClient;
use crate::storage::RowExt;
use std::sync::Arc;
use std::time::Duration;

use super::private_lane::WORKSPACE;
const OWNER: &str = "ordinary-session-promoter";

/// The lease owner this boot's promoter stamps on private rows. The boot id
/// lets startup recovery tell this boot's live leases from orphans of an
/// earlier one, as the watchdog ledger's `runner_boot_id` does.
fn private_lease_owner(orch: &Orchestrator) -> String {
    format!("{OWNER}@{}", orch.boot_at)
}
const PRIVATE_LEASE_SECONDS: i64 = 300;
const PRIVATE_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(60);
const PRIVATE_HEARTBEAT_RETRY_INTERVAL: Duration = Duration::from_secs(5);
const TEAM_STARTUP_RECOVERY_BUDGET: Duration = Duration::from_secs(5);

fn next_private_heartbeat_delay(result: &Result<bool, String>) -> Option<Duration> {
    match result {
        Ok(false) => None,
        Ok(true) => Some(PRIVATE_HEARTBEAT_INTERVAL),
        Err(_) => Some(PRIVATE_HEARTBEAT_RETRY_INTERVAL),
    }
}

async fn heartbeat_private_once_if_current(
    process_state: &crate::agent_process::process::AgentProcessState,
    db: &crate::storage::LocalDb,
    run_id: &str,
    request_id: &str,
    owner: &str,
    generation: i64,
    now: i64,
) -> Result<bool, String> {
    if !process_state.admission_claim_is_current(run_id, request_id, generation) {
        return Ok(false);
    }
    crate::storage::heartbeat_admission(
        db,
        request_id,
        owner,
        generation,
        now,
        now + PRIVATE_LEASE_SECONDS,
    )
    .await
    .map_err(|error| error.to_string())
}

fn spawn_private_heartbeat(
    process_state: Arc<crate::agent_process::process::AgentProcessState>,
    db: Arc<crate::storage::LocalDb>,
    run_id: String,
    request_id: String,
    owner: String,
    generation: i64,
) {
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        return;
    };
    handle.spawn(async move {
        let mut delay = PRIVATE_HEARTBEAT_INTERVAL;
        loop {
            tokio::time::sleep(delay).await;
            let result = heartbeat_private_once_if_current(
                &process_state,
                &db,
                &run_id,
                &request_id,
                &owner,
                generation,
                chrono::Utc::now().timestamp(),
            )
            .await;
            if let Err(error) = &result {
                    log::warn!(
                        "private ordinary admission {request_id} generation {generation} heartbeat failed; retrying while process remains current: {error}"
                    );
            }
            let Some(next_delay) = next_private_heartbeat_delay(&result) else {
                return;
            };
            delay = next_delay;
        }
    });
}

fn apply_prepared_identity_metadata(
    identity: &mut crate::identity::UserIdentity,
    metadata: crate::storage::PreparedUserIdentity,
) {
    identity.user_id = metadata.user_id;
    identity.email = metadata.email;
    identity.name = metadata.name;
}

enum OrdinaryLease {
    Private(crate::storage::AdmissionRequest),
    Team {
        request: crate::runtime_admission::AdmissionRequest,
        workspace_id: String,
    },
}

struct OrdinaryLaunchState {
    run_status: String,
    turn_awaits_launch: bool,
    project_id: String,
    session_status: String,
    current_session_id: Option<String>,
}

impl OrdinaryLaunchState {
    fn refusal_reason(&self, session_id: &str) -> Option<&'static str> {
        if self.session_status != "open" || self.current_session_id.as_deref() != Some(session_id) {
            Some("session closed before launch")
        } else if self.run_status != "starting" || !self.turn_awaits_launch {
            Some("run or turn is no longer launchable")
        } else {
            None
        }
    }
}

fn load_ordinary_launch_state(
    db: Arc<crate::storage::LocalDb>,
    run_id: &str,
    turn_id: &str,
) -> Result<Option<OrdinaryLaunchState>, String> {
    let run = run_id.to_string();
    let turn = turn_id.to_string();
    run_db(async move {
        db.query_opt(
            format!(
                "SELECT r.status,{},j.project_id,s.status,j.current_session_id FROM runs r JOIN jobs j ON j.id=r.job_id JOIN sessions s ON s.id=r.session_id JOIN turns t ON t.id=?2 WHERE r.id=?1",
                super::TURN_AWAITS_LAUNCH_SQL
            ),
            (run, turn),
            |row| Ok(OrdinaryLaunchState {
                run_status: row.text(0)?,
                turn_awaits_launch: row.i64(1)? != 0,
                project_id: row.text(2)?,
                session_status: row.text(3)?,
                current_session_id: row.opt_text(4)?,
            }),
        )
        .await
        .map_err(|e| e.to_string())
    })
}

fn resume_warm_race(
    orch: &Orchestrator,
    lease: &OrdinaryLease,
    snapshot: &crate::storage::PreparedOrdinaryLaunch,
    warm_run_id: &str,
    launch_guard: std::sync::MutexGuard<'_, ()>,
) -> Result<bool, String> {
    let owning = run_db({
        let dbs = orch.db.clone();
        let run_id = snapshot.run_id.clone();
        async move {
            crate::execution::routing::owning_db_for_run(&dbs, &run_id)
                .await
                .map_err(|e| e.to_string())
        }
    })?;
    let state = load_ordinary_launch_state(owning.clone(), &snapshot.run_id, &snapshot.turn_id)?
        .ok_or_else(|| "prepared ordinary warm continuation is no longer durable".to_string())?;
    if let Some(reason) = state.refusal_reason(&snapshot.session_id) {
        return withdraw_unlaunched(orch, lease, snapshot, launch_guard, reason);
    }
    let project_id = state.project_id;

    // The process that won the race already owns capacity. Release this unused
    // generation before delivery and never install it as a pending process claim.
    finish(
        orch,
        lease,
        "cancelled",
        Some("warm process won queued cold launch race"),
    )?;
    // Delivery binds the turn to the already-live warm run before it sends, so
    // that run's `started_at` proves nothing about this prompt. A failed handoff
    // is known here and nowhere else, so the wake is returned here.
    if let Err(error) = super::lifecycle::deliver_prepared_warm_continuation(
        orch,
        &owning,
        warm_run_id,
        snapshot,
        &project_id,
    ) {
        return_undelivered_attention(
            orch,
            &snapshot.run_id,
            &snapshot.turn_id,
            TurnDelivery::NeverSent,
        );
        return Err(error);
    }
    // The unused cold run never had a process, so it ends `exited`, as a
    // withdrawn launch does; its own exit reason says the warm process took it.
    let db = owning;
    let cold_run_id = snapshot.run_id.clone();
    run_db(async move {
        db.write(move |conn| {
            let cold_run_id = cold_run_id.clone();
            Box::pin(async move {
                let now = chrono::Utc::now().timestamp();
                conn.execute(
                    "UPDATE runs SET status='exited',exit_reason='warm_race_reused',exited_at=?2,updated_at=?2 WHERE id=?1 AND status='starting'",
                    (cold_run_id.clone(), now),
                )
                .await?;
                crate::storage::withdraw_queued_ordinary_admission(
                    conn,
                    &cold_run_id,
                    "warm process won queued cold launch race",
                    now,
                )
                .await?;
                Ok(())
            })
        })
        .await
        .map_err(|e| e.to_string())
    })?;
    orch.db.local.provider_admission_changed();
    Ok(false)
}

impl OrdinaryLease {
    fn request_id(&self) -> &str {
        match self {
            Self::Private(v) => &v.request_id,
            Self::Team { request, .. } => &request.id,
        }
    }
    fn launch_key(&self) -> &str {
        match self {
            Self::Private(v) => &v.launch_key,
            Self::Team { request, .. } => &request.launch_key,
        }
    }
    fn generation(&self) -> i64 {
        match self {
            Self::Private(v) => v.lease_generation,
            Self::Team { request, .. } => request.lease_generation,
        }
    }
    fn workspace_key(&self) -> String {
        match self {
            Self::Private(_) => WORKSPACE.into(),
            Self::Team { workspace_id, .. } => format!("team:{workspace_id}"),
        }
    }
    fn team_workspace_id(&self) -> Option<String> {
        match self {
            Self::Private(_) => None,
            Self::Team { workspace_id, .. } => Some(workspace_id.clone()),
        }
    }
    fn owner(&self) -> Result<String, String> {
        match self {
            Self::Private(request) => request
                .lease_owner
                .clone()
                .ok_or_else(|| "private ordinary lease has no owner".into()),
            Self::Team { request, .. } => request
                .lease_owner
                .clone()
                .ok_or_else(|| "team ordinary lease has no authoritative owner".into()),
        }
    }
}

fn enqueue_request_from_snapshot(
    workspace_id: String,
    launch: &crate::storage::PreparedOrdinaryLaunch,
) -> crate::runtime_admission::EnqueueAdmissionRequest {
    let runtime_class = match launch.runtime_capability.class {
        crate::storage::PreparedRuntimeClass::DedicatedProcess => "dedicated_process",
        crate::storage::PreparedRuntimeClass::MultiplexedServer => "multiplexed_server",
        crate::storage::PreparedRuntimeClass::StatelessHttp => "stateless_http",
    };
    crate::runtime_admission::EnqueueAdmissionRequest {
        id: launch.request_id.clone(),
        launch_key: format!("ordinary:{}", launch.run_id),
        workspace_id,
        execution_id: (!launch.execution_id.is_empty()).then(|| launch.execution_id.clone()),
        job_id: Some(launch.job_id.clone()),
        run_id: Some(launch.run_id.clone()),
        session_id: Some(launch.session_id.clone()),
        backend_lane: launch.runtime_capability.lane.clone(),
        runtime_class: runtime_class.into(),
        resource_claim: serde_json::to_value(&launch.runtime_capability.claim)
            .expect("persisted runtime claim is serializable"),
        priority: team_priority(launch.origin),
    }
}

fn team_priority(
    origin: crate::storage::LaunchOrigin,
) -> crate::runtime_admission::AdmissionPriority {
    if origin.is_urgent() {
        crate::runtime_admission::AdmissionPriority::Urgent
    } else {
        crate::runtime_admission::AdmissionPriority::Ordinary
    }
}

async fn recover_team_launch(
    orch: &Orchestrator,
    recoverable: crate::storage::RecoverableTeamOrdinaryLaunch,
) {
    let request =
        enqueue_request_from_snapshot(recoverable.workspace_id.clone(), &recoverable.launch);
    let current_host = std::env::var("CAIRN_RUNNER_ID").unwrap_or_else(|_| "local-runner".into());
    let status = match super::team_admission_lifecycle::team_client(orch) {
        Ok(client) => client
            .status(&request.id)
            .await
            .ok()
            .map(|status| status.request),
        Err(_) => None,
    };

    let durable_lease = recoverable.authority.as_ref().filter(|authority| {
        super::team_admission_lifecycle::durable_lease_belongs_to_host(
            authority,
            status.as_ref(),
            &current_host,
        )
    });
    if let Some(authority) = durable_lease {
        let remote = status.unwrap_or_else(|| crate::runtime_admission::AdmissionRequest {
            id: request.id.clone(),
            launch_key: request.launch_key.clone(),
            workspace_id: request.workspace_id.clone(),
            backend_lane: request.backend_lane.clone(),
            runtime_class: request.runtime_class.clone(),
            resource_claim: request.resource_claim.clone(),
            priority: request.priority,
            state: "leased".into(),
            lease_generation: authority.lease_generation,
            lease_owner: Some(authority.lease_owner.clone()),
            lease_runner_id: Some(authority.lease_owner.clone()),
            lease_host_id: Some(authority.lease_host_id.clone()),
            lease_expires_at: None,
            admission_policy_revision: None,
            admission_capacity: None,
        });
        let lease = OrdinaryLease::Team {
            request: remote,
            workspace_id: recoverable.workspace_id,
        };
        let continue_leased = {
            let orch = orch.clone();
            let request_id = request.id.clone();
            move || {
                if let Err(error) = start_leased(&orch, lease, recoverable.launch) {
                    log::warn!(
                        "team ordinary admission recovery could not continue leased request {request_id}: {error}"
                    );
                }
            }
        };
        // This runs inside startup recovery, which must not wait on its own
        // readiness: the held lease continues in the background once it opens.
        // Nothing heartbeats the lease while it waits. Team promotion leases
        // for five minutes, far longer than recovery runs; a lease the API
        // reclaimed meanwhile fails its transition in `start_leased`, which
        // terminalizes the start rather than leaving the job `running`.
        if orch.startup_readiness.is_ready() {
            continue_leased();
        } else {
            let readiness = orch.startup_readiness.clone();
            orch.spawn_blocking_detached(move || {
                readiness.wait_ready_blocking();
                continue_leased();
            });
        }
        return;
    }

    // No usable generation belongs to this host. Reconciliation stays remote:
    // idempotent enqueue/status/lease may leave the request queued, but it never
    // consults or consumes this runner's private capacity ledger.
    dispatch_team_promotion(orch, request);
}

fn backend_for(prepared: &PreparedJob) -> std::sync::Arc<dyn crate::backends::AgentBackend> {
    crate::backends::backend_for_name(Some(&backend_key_for(prepared))).into()
}

/// The provider key `backend_for` resolves: the same key the usage pool and
/// account surfaces are addressed by.
fn backend_key_for(prepared: &PreparedJob) -> String {
    crate::backends::effective_backend_name(
        prepared
            .agent_config
            .as_ref()
            .and_then(|config| config.selection.as_ref()),
        prepared.job_model.as_ref().map(Model::as_str),
    )
    .unwrap_or_else(|| crate::backends::CLAUDE_FAMILY_BACKEND.to_string())
}

/// Reclaim process-owned leases lost with the prior host and restart promotion
/// from the durable queue. Snapshot reconstruction performs the same fencing as
/// a live enqueue, so recovery cannot bypass launch-state checks.
///
/// Requeued rows whose run is already terminal are withdrawn before promotion:
/// a run whose row was `active` had a process that died with the predecessor,
/// and the boot reconcile settled it, so its row must not spend a lease.
///
/// Leases this boot already took are exempt. Watchdog recovery runs earlier in
/// startup and enqueues successors that the promoter leases and starts at
/// once, and transport readiness can admit operator launches before this step
/// too; requeueing those would orphan a live process from its row and fail
/// its start with "lease was replaced".
pub(crate) async fn recover_on_startup(orch: &Orchestrator) {
    let now = chrono::Utc::now().timestamp();
    let current_owner = private_lease_owner(orch);
    if let Err(error) = crate::storage::requeue_orphaned_admissions(
        &orch.db.local,
        WORKSPACE,
        Some(&current_owner),
        now,
    )
    .await
    {
        log::warn!("ordinary admission recovery could not reclaim leases: {error}");
    }
    let recovered_orch = orch.clone();
    if let Err(error) = tokio::task::spawn_blocking(move || {
        super::private_lane::withdraw_orphaned_ordinary_admissions(&recovered_orch);
        super::private_lane::promote_private_lanes_blocking(&recovered_orch);
    })
    .await
    {
        log::warn!("ordinary admission recovery worker failed: {error}");
    }
    if tokio::time::timeout(TEAM_STARTUP_RECOVERY_BUDGET, recover_team_on_startup(orch))
        .await
        .is_err()
    {
        log::warn!(
            "team ordinary admission recovery exceeded its startup budget; durable requests remain available for later reconciliation"
        );
    }
}

async fn recover_team_on_startup(orch: &Orchestrator) {
    super::team_admission_lifecycle::retry_pending_terminals_on_startup(orch).await;
    let team_launches =
        match crate::storage::list_recoverable_team_ordinary_launches(&orch.db.local).await {
            Ok(launches) => launches,
            Err(error) => {
                log::warn!("ordinary admission recovery could not list team snapshots: {error}");
                return;
            }
        };
    for recoverable in team_launches {
        if recoverable
            .authority
            .as_ref()
            .is_some_and(|authority| authority.pending_terminal_action.is_some())
        {
            continue;
        }
        recover_team_launch(orch, recoverable).await;
    }
}

pub(super) fn enqueue_or_start(
    orch: &Orchestrator,
    prepared: &mut PreparedJob,
    identity_override: Option<crate::identity::UserIdentity>,
) -> Result<bool, String> {
    let backend = backend_for(prepared);
    let capability = backend.runtime_launch_capability(&crate::backends::RuntimeLaunch {
        is_ephemeral_call: false,
        server_instance_key: None,
    })?;
    if capability.class == crate::backends::RuntimeClass::StatelessHttp {
        if !orch.startup_readiness.is_ready() {
            defer_stateless_start(orch, prepared, identity_override);
            return Ok(false);
        }
        return start_stateless_now(orch, prepared, identity_override);
    }

    let team_id = match cairn_common::ids::parse_route_scope(&prepared.project_id)
        .map_err(|error| format!("invalid ordinary launch project scope: {error}"))?
    {
        cairn_common::ids::RouteScope::Local => None,
        cairn_common::ids::RouteScope::Team(team_id) => Some(team_id),
    };

    let request_id = format!("ordinary-admission:{}", prepared.run_id);
    let now = chrono::Utc::now().timestamp();
    let mut runtime_capability: crate::storage::PreparedRuntimeCapability = serde_json::from_value(
        serde_json::to_value(&capability)
            .map_err(|error| format!("serialize ordinary runtime capability: {error}"))?,
    )
    .map_err(|error| format!("convert ordinary runtime capability: {error}"))?;
    runtime_capability.provider = Some(backend_key_for(prepared));
    let session_start = match &prepared.session_start {
        crate::backends::SessionStart::New { session_id } => {
            crate::storage::PreparedSessionStart::New {
                session_id: session_id.clone(),
            }
        }
        crate::backends::SessionStart::Resume {
            session_id,
            backend_id,
        } => crate::storage::PreparedSessionStart::Resume {
            session_id: session_id.clone(),
            backend_id: backend_id.clone(),
        },
        crate::backends::SessionStart::Fork {
            session_id,
            source_backend_id,
        } => crate::storage::PreparedSessionStart::Fork {
            session_id: session_id.clone(),
            source_backend_id: source_backend_id.clone(),
        },
    };
    let routed_provider =
        crate::credentials::RoutedProvider::for_backend(&backend.name().to_ascii_lowercase());
    if identity_override.is_none() {
        if let Some(provider) = routed_provider {
            // Account selection is part of preparation. Besides returning the
            // in-memory credential, this writes the non-secret account id onto
            // the session so queued and restarted promotion cannot silently
            // select a different account.
            crate::orchestrator::session::resolve_routed_session_identity(
                orch,
                provider,
                &prepared.session_id,
                Some(&prepared.project_id),
                prepared.job_model.as_ref().map(Model::as_str),
                crate::orchestrator::session::snapshot_account_override(orch, &prepared.run_id)?
                    .as_deref(),
            )?;
        }
    }
    let provider_account_id = run_db({
        let db = orch.db.local.clone();
        let session_id = prepared.session_id.clone();
        async move {
            crate::sessions::queries::get(&db, &session_id)
                .await
                .map(|session| session.account_id)
                .map_err(|error| error.to_string())
        }
    })?;
    let snapshot = crate::storage::PreparedOrdinaryLaunch {
        request_id: request_id.clone(),
        job_id: prepared.job_id.clone(),
        run_id: prepared.run_id.clone(),
        session_id: prepared.session_id.clone(),
        turn_id: prepared.turn_id.clone(),
        execution_id: prepared.execution_id.clone().unwrap_or_default(),
        prompt: prepared.prompt.clone(),
        reason: prepared.start_reason.clone(),
        session_start,
        model: prepared
            .job_model
            .as_ref()
            .map(|m| m.as_str().to_string())
            .unwrap_or_default(),
        agent_config: serde_json::to_value(&prepared.agent_config)
            .map_err(|error| format!("serialize ordinary agent config: {error}"))?,
        output_schema: prepared
            .artifact_schema_info
            .as_ref()
            .map(serde_json::to_value)
            .transpose()
            .map_err(|error| format!("serialize ordinary output schema: {error}"))?,
        runtime_capability,
        provider_account_id,
        identity_override: identity_override.as_ref().map(|identity| {
            crate::storage::PreparedUserIdentity {
                user_id: identity.user_id.clone(),
                email: identity.email.clone(),
                name: identity.name.clone(),
            }
        }),
        origin: prepared.origin,
        created_at: now,
    };
    // The prepared turn belongs to its run from here on, so every run-scoped
    // settlement (the reaper, the boot reconcile, finalize, stop) terminalizes
    // it with the run instead of stranding a pending head that reads mid-turn.
    // Starting the turn later rebinds it to whichever run serves it.
    run_db({
        let dbs = orch.db.clone();
        let run_id = prepared.run_id.clone();
        let turn_id = prepared.turn_id.clone();
        async move {
            let owning = crate::execution::routing::owning_db_for_run(&dbs, &run_id)
                .await
                .map_err(|error| error.to_string())?;
            owning
                .execute(
                    "UPDATE turns SET run_id=?1 WHERE id=?2 AND state='pending' AND run_id IS NULL",
                    (run_id, turn_id),
                )
                .await
                .map(|_| ())
                .map_err(|error| format!("bind prepared turn to its run: {error}"))
        }
    })?;
    run_db({
        let db = orch.db.local.clone();
        let durable_request_id = request_id.clone();
        let lane = capability.lane.0.clone();
        let claim = capability.claim.clone();
        let run_id = prepared.run_id.clone();
        let origin = prepared.origin;
        let queue_workspace = team_id
            .as_ref()
            .map(|id| format!("team:{id}"))
            .unwrap_or_else(|| WORKSPACE.into());
        async move {
            crate::storage::enqueue_prepared_ordinary_launch(
                &db,
                crate::storage::NewAdmissionRequest {
                    request_id: durable_request_id,
                    workspace_key: queue_workspace,
                    resource_lane: lane,
                    launch_key: format!("ordinary:{run_id}"),
                    resource_claim: crate::storage::RuntimeResourceClaim {
                        resident_process_units: claim.resident_process_units,
                        server_instance_key: claim.server_instance_key,
                        logical_stream_units: claim.logical_stream_units,
                        estimated_memory_bytes: claim.estimated_memory_bytes,
                    },
                    // An operator waiting on this launch goes ahead of background
                    // work; the urgent burst bound keeps ordinary work moving.
                    urgent: origin.is_urgent(),
                    now,
                },
                snapshot,
            )
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
        }
    })?;
    // The in-memory launch claim covers preparation and is released only now
    // that the durable admission row exists, so the stale-run reaper always
    // sees one or the other. From here the row is the claim: the reaper treats
    // a `queued`/`leased`/`starting` row as a launch in flight, and nothing is
    // held in memory while queued, so a queued request never blocks a
    // competing resume indefinitely.
    drop(prepared.launch_claim.take());
    if let Some(team_id) = team_id {
        let runtime_class = match capability.class {
            crate::backends::RuntimeClass::DedicatedProcess => "dedicated_process",
            crate::backends::RuntimeClass::MultiplexedServer => "multiplexed_server",
            crate::backends::RuntimeClass::StatelessHttp => unreachable!(),
        };
        dispatch_team_promotion(
            orch,
            crate::runtime_admission::EnqueueAdmissionRequest {
                id: request_id,
                launch_key: format!("ordinary:{}", prepared.run_id),
                workspace_id: team_id,
                execution_id: prepared.execution_id.clone(),
                job_id: Some(prepared.job_id.clone()),
                run_id: Some(prepared.run_id.clone()),
                session_id: Some(prepared.session_id.clone()),
                backend_lane: capability.lane.0.clone(),
                runtime_class: runtime_class.into(),
                resource_claim: serde_json::to_value(&capability.claim)
                    .map_err(|e| e.to_string())?,
                priority: team_priority(prepared.origin),
            },
        );
    } else {
        promote_private_lane(orch, &capability.lane.0);
    }
    Ok(false)
}

fn start_stateless_now(
    orch: &Orchestrator,
    prepared: &mut PreparedJob,
    identity_override: Option<crate::identity::UserIdentity>,
) -> Result<bool, String> {
    // Cold continuation drops its launch guard before this handoff; initial
    // starts and child tasks carry no guard. Deferred starts enter here only
    // after readiness, so this lock fences every stateless provider handoff.
    let launch_lock = orch.job_launch_lock(&prepared.job_id);
    let launch_guard = launch_lock
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if prepared.launch_claim.is_none() {
        prepared.launch_claim = Some(orch.claim_job_launch(&prepared.job_id, &prepared.run_id));
    }
    let owning = run_db({
        let dbs = orch.db.clone();
        let run_id = prepared.run_id.clone();
        async move {
            crate::execution::routing::owning_db_for_run(&dbs, &run_id)
                .await
                .map_err(|e| e.to_string())
        }
    })?;
    // Stateless launches have no enqueue write to bind an initial pending turn.
    // Bind it before settlement, while the in-memory claim still covers the run.
    run_db({
        let db = owning.clone();
        let run_id = prepared.run_id.clone();
        let turn_id = prepared.turn_id.clone();
        async move {
            db.execute(
                "UPDATE turns SET run_id=?1 WHERE id=?2 AND state='pending' AND run_id IS NULL",
                (run_id, turn_id),
            )
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
        }
    })?;
    let launch_state = load_ordinary_launch_state(owning, &prepared.run_id, &prepared.turn_id)?;
    let refusal = launch_state
        .as_ref()
        .map(|state| state.refusal_reason(&prepared.session_id))
        .unwrap_or(Some("run or turn is no longer launchable"));
    if let Some(reason) = refusal {
        return_undelivered_attention(
            orch,
            &prepared.run_id,
            &prepared.turn_id,
            TurnDelivery::Unknown,
        );
        let settled = settle_unlaunched_run(orch, &prepared.run_id, &prepared.turn_id)?;
        // Recomputing may launch a successor: neither the old launch claim nor
        // this non-reentrant mutex may survive into it.
        drop(prepared.launch_claim.take());
        drop(launch_guard);
        if settled {
            log::info!(
                "withdrew stateless launch of run {}: {reason}",
                prepared.run_id
            );
            if let Err(error) = crate::execution::advancement::recompute_job(orch, &prepared.job_id)
            {
                log::warn!(
                    "could not re-derive job {} after withdrawing its stateless launch: {error}",
                    prepared.job_id
                );
            }
        }
        return Ok(false);
    }
    super::start_ordinary_session_now(orch, prepared, identity_override)?;
    super::start_prepared_turn_and_publish(
        orch,
        &prepared.turn_id,
        &prepared.run_id,
        &prepared.run_id,
    )?;
    Ok(true)
}

/// Start a stateless launch once startup recovery is ready.
///
/// This class deliberately has no durable admission row, so nothing durable
/// can hold it. It waits in the background instead, keeping the in-memory
/// launch claim (which is what keeps the stale-run reaper off this class) until
/// its start returns. The caller reads `started=false` and hands the launch's
/// attention delivery over, exactly as for a queued launch, so every exit here
/// that ends without a process returns that delivery.
fn defer_stateless_start(
    orch: &Orchestrator,
    prepared: &mut PreparedJob,
    identity_override: Option<crate::identity::UserIdentity>,
) {
    let claim = prepared
        .launch_claim
        .take()
        .unwrap_or_else(|| orch.claim_job_launch(&prepared.job_id, &prepared.run_id));
    let mut deferred = PreparedJob {
        job_id: prepared.job_id.clone(),
        project_id: prepared.project_id.clone(),
        run_id: prepared.run_id.clone(),
        session_id: prepared.session_id.clone(),
        session_start: prepared.session_start.clone(),
        prompt: prepared.prompt.clone(),
        job_model: prepared.job_model.clone(),
        agent_config: prepared.agent_config.clone(),
        artifact_schema_info: prepared.artifact_schema_info.clone(),
        execution_id: prepared.execution_id.clone(),
        turn_id: prepared.turn_id.clone(),
        start_reason: prepared.start_reason.clone(),
        origin: prepared.origin,
        launch_claim: Some(claim),
    };
    let task_orch = orch.clone();
    orch.spawn_blocking_detached(move || {
        let orch = task_orch;
        orch.startup_readiness.wait_ready_blocking();
        let run_id = deferred.run_id.clone();
        let turn_id = deferred.turn_id.clone();
        // The locked handoff revalidates durable session, run and turn facts;
        // missing or unreadable facts never permit a provider prompt.
        if let Err(error) = start_stateless_now(&orch, &mut deferred, identity_override) {
            log::warn!("deferred stateless launch of run {run_id} failed to start: {error}");
            terminalize_failed_start(
                &orch,
                &run_id,
                &deferred.session_id,
                &deferred.job_id,
                &turn_id,
                &error,
            );
            orch.process_state.stop_and_remove(&run_id);
        }
        // Dropping `deferred` releases its launch claim, now that the process
        // registered or the start failed.
    });
}

fn dispatch_team_promotion(
    orch: &Orchestrator,
    request: crate::runtime_admission::EnqueueAdmissionRequest,
) {
    let scheduler = orch;
    let orch = orch.clone();
    let request_id = request.id.clone();
    scheduler.spawn_blocking_detached(move || {
        // The local mirror row is durable already; leasing waits for readiness
        // so the remote grant is never spent on a turn the gateway refuses.
        orch.startup_readiness.wait_ready_blocking();
        let mut delay = std::time::Duration::from_secs(1);
        loop {
            // The run lives in the team replica and this queue row in the local
            // database, so settling the run cannot withdraw the row in the same
            // write. Each retry asks instead. A settled run stops promotion and
            // turns the loop into withdrawal, which retries until Postgres lets
            // go of the request; the local mirror is retired only then.
            if let Some(run_id) = request.run_id.as_deref() {
                if super::private_lane::run_settled(&orch, run_id) == Some(true) {
                    match super::team_admission_lifecycle::withdraw_settled_team_request(
                        &orch,
                        &request_id,
                    ) {
                        Ok(_) => return,
                        Err(error) => log::warn!(
                            "team ordinary admission {request_id}: its run settled, and withdrawal remains pending: {error}"
                        ),
                    }
                    std::thread::sleep(delay);
                    delay = delay
                        .saturating_mul(2)
                        .min(std::time::Duration::from_secs(60));
                    continue;
                }
            }
            match enqueue_and_promote_team(&orch, request.clone()) {
                Ok(true) => return,
                Ok(false) => {}
                Err(error) => {
                    log::warn!("team ordinary admission {request_id} remains pending: {error}");
                }
            }
            std::thread::sleep(delay);
            delay = delay
                .saturating_mul(2)
                .min(std::time::Duration::from_secs(60));
        }
    });
}

fn enqueue_and_promote_team(
    orch: &Orchestrator,
    request: crate::runtime_admission::EnqueueAdmissionRequest,
) -> Result<bool, String> {
    let request_id = request.id.clone();
    let team_id = request.workspace_id.clone();
    let token = match crate::api::runner_credential() {
        Ok(credential) => credential.token().to_string(),
        Err(error) => {
            log::warn!("team ordinary admission {request_id} remains pending: {error}");
            return Ok(false);
        }
    };
    let client = crate::runtime_admission::TeamAdmissionClient::with_api(
        orch.runtime_admission_api_config(),
        token,
    )
    .map_err(|error| error.to_string())?;
    let lease = run_db(async move {
        if let Err(error) = client.enqueue(&request).await {
            log::warn!(
                "team ordinary admission {} remains pending: {error}",
                request.id
            );
            return Err(error.to_string());
        }
        client
            .lease(&crate::runtime_admission::LeaseAdmissionRequest {
                request_id: request.id,
                workspace_id: request.workspace_id,
                backend_lane: request.backend_lane,
                host_id: std::env::var("CAIRN_RUNNER_ID").unwrap_or_else(|_| "local-runner".into()),
                lease_ms: 300_000,
            })
            .await
            .map_err(|error| error.to_string())
    })?;
    // The authoritative claim operation only grants this runner's exact
    // reconstructable request when it is also the fair, capacity-fitting winner.
    if let Some(lease) = lease.filter(|lease| lease.id == request_id) {
        let lease_owner = lease
            .lease_owner
            .clone()
            .ok_or_else(|| "team ordinary lease has no authoritative owner".to_string())?;
        let lease_host_id = lease
            .lease_host_id
            .clone()
            .ok_or_else(|| "team ordinary lease has no authoritative host".to_string())?;
        run_db({
            let db = orch.db.local.clone();
            let request_id = lease.id.clone();
            let generation = lease.lease_generation;
            async move {
                crate::storage::persist_team_ordinary_lease(
                    &db,
                    crate::storage::TeamOrdinaryAuthority {
                        request_id,
                        lease_owner,
                        lease_generation: generation,
                        lease_host_id,
                        pending_terminal_action: None,
                        pending_terminal_detail: None,
                        updated_at: chrono::Utc::now().timestamp(),
                    },
                )
                .await
                .map_err(|error| error.to_string())
            }
        })?;
        let snapshot = run_db({
            let db = orch.db.local.clone();
            let id = lease.id.clone();
            async move {
                crate::storage::get_prepared_ordinary_launch(&db, &id)
                    .await
                    .map_err(|e| e.to_string())
            }
        })?
        .ok_or_else(|| "team ordinary lease has no local prepared snapshot".to_string())?;
        // A failed start has already finished its generation and failed its
        // job, so the request is terminal: retrying would only poll forever.
        if let Err(error) = start_leased(
            orch,
            OrdinaryLease::Team {
                request: lease,
                workspace_id: team_id,
            },
            snapshot,
        ) {
            log::warn!("team ordinary admission {request_id} failed to start: {error}");
        }
        return Ok(true);
    }
    Ok(false)
}

/// Schedule a promotion pass for `lane` in the background. Callers include
/// process-teardown callbacks on plain OS threads, so scheduling never depends
/// on the caller having a runtime, and a pass that fails is the sweep's to
/// retry: nothing the caller holds would change that outcome.
pub(crate) fn promote_private_lane(orch: &Orchestrator, lane: &str) {
    let promoter = orch.clone();
    let lane = lane.to_string();
    orch.spawn_blocking_detached(move || {
        if let Err(error) = promote_private_lane_blocking(&promoter, &lane) {
            log::warn!("ordinary admission lane {lane} stopped promoting: {error}");
        }
    });
}

/// Drain every queued launch the lane can admit right now. Each pass measures
/// the budget afresh (see `private_lane`), records the decision for the
/// surfaces that display it, and when the head is blocked with nothing mid-spawn
/// asks the warm GC for room on its behalf. Inside the boot horizon it also
/// leases against the boot ramp's warm-up window.
pub(super) fn promote_private_lane_blocking(orch: &Orchestrator, lane: &str) -> Result<(), String> {
    let owner = private_lease_owner(orch);
    loop {
        // Every caller wrote its admission row before reaching here, so a pass
        // that reads not-ready leaves the row for the drain that follows
        // readiness (`Orchestrator::mark_startup_recovery_ready`).
        if !orch.startup_readiness.is_ready() {
            orch.private_lanes.record(
                WORKSPACE,
                lane,
                super::private_lane::private_lane_budget(orch),
                Some(crate::storage::LaneShortfall::StartupRecovery),
            );
            return Ok(());
        }
        let db = orch.db.local.clone();
        let lane_owned = lane.to_string();
        let owner = owner.clone();
        // While the ramp is on, the window read, the lease it decides, and the
        // warming registration below are one critical section across passes;
        // the start itself runs outside it.
        let ramp_admission = super::private_lane::boot_ramp_active(orch)
            .then(|| orch.private_lanes.lock_ramp_admission());
        let spawn_window = super::private_lane::boot_ramp_window(orch);
        let budget = super::private_lane::private_lane_budget(orch);
        let now = chrono::Utc::now().timestamp();
        let decision = run_db(async move {
            crate::storage::lease_next_admission_within(
                &db,
                WORKSPACE,
                &lane_owned,
                &owner,
                now,
                now + PRIVATE_LEASE_SECONDS,
                budget.capacity(),
                budget.memory,
                spawn_window,
                crate::storage::MemoryPriority::Interactive,
            )
            .await
            .map_err(|error| error.to_string())
        })?;
        let lease = match decision {
            crate::storage::LeaseDecision::Leased(lease) => {
                orch.private_lanes.record(WORKSPACE, lane, budget, None);
                lease
            }
            crate::storage::LeaseDecision::Empty => {
                orch.private_lanes.record(WORKSPACE, lane, budget, None);
                drop(ramp_admission);
                // Ephemeral calls yield to queued sessions, so a drained queue
                // is when a call held back for one may admit.
                super::calls::promote_queued_call_lanes(orch);
                return Ok(());
            }
            crate::storage::LeaseDecision::Blocked {
                head_request_id,
                shortfall,
                in_flight_requests,
            } => {
                orch.private_lanes
                    .record(WORKSPACE, lane, budget, Some(shortfall.clone()));
                // A ramp hold waits for a warming spawn to speak. Evicting a warm
                // process frees memory, not a warm-up slot, and the release that
                // frees one promotes again.
                if matches!(shortfall, crate::storage::LaneShortfall::Ramp { .. }) {
                    super::private_lane::note_ramp_hold(orch, &head_request_id);
                    return Ok(());
                }
                // A spawn in flight is about to change what the probe reads and
                // will re-trigger promotion when it settles; evicting now would
                // decide against a measurement that is already stale.
                if in_flight_requests == 0 {
                    super::private_lane::make_room_for_head(orch, lane, &shortfall);
                }
                return Ok(());
            }
        };
        let snapshot = run_db({
            let db = orch.db.local.clone();
            let id = lease.request_id.clone();
            async move {
                crate::storage::get_prepared_ordinary_launch(&db, &id)
                    .await
                    .map_err(|e| e.to_string())
            }
        })?;
        let Some(snapshot) = snapshot else {
            finish(
                orch,
                &OrdinaryLease::Private(lease),
                "cancelled",
                Some("leased ordinary launch has no prepared snapshot"),
            )?;
            continue;
        };
        let request_id = lease.request_id.clone();
        let run_id = snapshot.run_id.clone();
        let warms = spawn_window.is_some() && lease.resource_claim.spawns_dedicated_process();
        if warms {
            orch.private_lanes
                .begin_warming(&run_id, lane, &request_id, lease.urgent);
        }
        drop(ramp_admission);
        let started = start_leased(orch, OrdinaryLease::Private(lease), snapshot);
        if warms {
            super::private_lane::settle_warming_start(orch, &run_id, &started);
        }
        if let Err(error) = started {
            // start_leased has already closed this generation and failed its
            // job; the next queued launch still deserves the freed slot.
            log::warn!("ordinary admission {request_id} failed to start: {error}");
        }
        // Continue until the capacity-aware repository declines another lease.
        // This drains all currently available slots rather than requiring one
        // unrelated enqueue/finalize callback per promotion.
    }
}

fn start_leased(
    orch: &Orchestrator,
    lease: OrdinaryLease,
    snapshot: crate::storage::PreparedOrdinaryLaunch,
) -> Result<bool, String> {
    let run_id = snapshot.run_id.clone();
    let session_id = snapshot.session_id.clone();
    let job_id = snapshot.job_id.clone();
    let turn_id = snapshot.turn_id.clone();
    let result = start_leased_inner(orch, &lease, snapshot);
    if let Err(error) = &result {
        terminalize_failed_start(orch, &run_id, &session_id, &job_id, &turn_id, error);
        // Once a generation has been leased, every restoration and start error
        // must close that exact generation, and capacity may only be returned
        // once no process holds it. A claim still pending never reached a
        // process. A claim that registration moved onto a live handle means a
        // dedicated process is resident: release-and-stop consumes that claim
        // exactly once and finishes its generation, so the lease must not also
        // be finished here.
        let _ = orch.process_state.cancel_pending_admission_claim(&run_id);
        if orch.process_state.admission_claim(&run_id).is_some() {
            if !crate::orchestrator::lifecycle::release_and_stop_process(orch, &run_id) {
                log::error!(
                    "promoted run {run_id} failed after registration and its claim release is pending retry"
                );
            }
        } else {
            orch.process_state.stop_and_remove(&run_id);
            if let Err(finish_error) = finish(orch, &lease, "failed", Some(error)) {
                return Err(format!(
                    "{error}; additionally failed to terminalize admission lease: {finish_error}"
                ));
            }
        }
    }
    result
}

/// Fail a launch whose start failed after its source was told admission owns
/// it. Enqueue (or deferral) already returned success to the launch source, so
/// nothing upstream will see this error: return the turn's attention delivery
/// and terminalize the run and job exactly as a direct start failure would, or
/// the job stays `running` forever.
fn terminalize_failed_start(
    orch: &Orchestrator,
    run_id: &str,
    session_id: &str,
    job_id: &str,
    turn_id: &str,
    error: &str,
) {
    return_undelivered_attention(orch, run_id, turn_id, TurnDelivery::Unknown);
    crate::orchestrator::session::insert_error_event(orch, run_id, Some(session_id), error);
    let failed = run_db({
        let dbs = orch.db.clone();
        let run_id = run_id.to_string();
        async move {
            crate::execution::routing::owning_db_for_run(&dbs, &run_id)
                .await
                .map_err(|e| e.to_string())
        }
    })
    .and_then(|owning| {
        let orch = orch.clone();
        let job_id = job_id.to_string();
        let error = error.to_string();
        let turn_id = turn_id.to_string();
        let run_id = run_id.to_string();
        run_db(async move {
            if crate::orchestrator::wakes::restore_provider_hold_after_admission_failure(
                &orch, &owning, &turn_id, chrono::Utc::now().timestamp(),
            ).await? {
                // The process launch failed, but its provider-held work did
                // not. Record the physical failure without a work verdict.
                owning.execute(
                    "UPDATE runs SET status='crashed', error_message=?1, exit_reason='session_start_failed', updated_at=?2 WHERE id=?3",
                    cairn_db::turso::params![error, chrono::Utc::now().timestamp(), run_id],
                ).await.map_err(|error| error.to_string())?;
            } else {
                super::mark_job_start_failed(&orch, &owning, &job_id, &error, false).await;
            }
            Ok::<(), String>(())
        })
    });
    if let Err(mark_error) = failed {
        log::error!("could not mark promoted job {job_id} start failed: {mark_error}");
    }
}

fn start_leased_inner(
    orch: &Orchestrator,
    lease: &OrdinaryLease,
    snapshot: crate::storage::PreparedOrdinaryLaunch,
) -> Result<bool, String> {
    let launch_lock = orch.job_launch_lock(&snapshot.job_id);
    let launch_guard = launch_lock
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(warm_run_id) = orch
        .process_state
        .find_warm_by_session(&snapshot.session_id)
    {
        return resume_warm_race(orch, lease, &snapshot, &warm_run_id, launch_guard);
    }
    // A same-session handle that is starting, serving a turn, or awaiting the
    // host is not reusable. In particular, never overwrite its occupancy and
    // write a second prompt to its stdin. That process serves the returned
    // attention at its next turn boundary.
    if orch
        .process_state
        .find_process_by_session(&snapshot.session_id)
        .is_some()
    {
        return withdraw_unlaunched(
            orch,
            lease,
            &snapshot,
            launch_guard,
            "same-session process is not warm and idle",
        );
    }
    // A claim on this same run is the enqueuer's own preparation claim, which it
    // holds until the admission row is durable and drops right after: a promoter
    // that leased the row in between is continuing that launch, not competing
    // with it. Only a claim on another run is a competing launch.
    if orch
        .claimed_launch_run(&snapshot.job_id)
        .is_some_and(|claimed| claimed != snapshot.run_id)
    {
        return withdraw_unlaunched(
            orch,
            lease,
            &snapshot,
            launch_guard,
            "session already has a process or competing launch",
        );
    }
    let owning = run_db({
        let dbs = orch.db.clone();
        let run_id = snapshot.run_id.clone();
        async move {
            crate::execution::routing::owning_db_for_run(&dbs, &run_id)
                .await
                .map_err(|e| e.to_string())
        }
    })?;
    let launch_state =
        load_ordinary_launch_state(owning.clone(), &snapshot.run_id, &snapshot.turn_id)?;
    let refusal = launch_state
        .as_ref()
        .map(|state| state.refusal_reason(&snapshot.session_id))
        .unwrap_or(Some("run or turn is no longer launchable"));
    if let Some(reason) = refusal {
        return withdraw_unlaunched(orch, lease, &snapshot, launch_guard, reason);
    }
    let project_id = launch_state
        .expect("launchable state was checked")
        .project_id;
    transition(orch, lease, "leased", "starting")?;
    let prompt = snapshot.prompt.clone();
    let session_start = match snapshot.session_start {
        crate::storage::PreparedSessionStart::New { session_id } => {
            crate::backends::SessionStart::New { session_id }
        }
        crate::storage::PreparedSessionStart::Resume {
            session_id,
            backend_id,
        } => crate::backends::SessionStart::Resume {
            session_id,
            backend_id,
        },
        crate::storage::PreparedSessionStart::Fork {
            session_id,
            source_backend_id,
        } => crate::backends::SessionStart::Fork {
            session_id,
            source_backend_id,
        },
    };
    let agent_config: Option<AgentConfig> = serde_json::from_value(snapshot.agent_config)
        .map_err(|e| format!("restore ordinary agent config: {e}"))?;
    let output: Option<OutputSchemaInfo> = snapshot
        .output_schema
        .map(serde_json::from_value)
        .transpose()
        .map_err(|e| format!("restore ordinary output schema: {e}"))?;
    let model = (!snapshot.model.is_empty()).then(|| Model::new(snapshot.model));
    let backend_name = crate::backends::effective_backend_name(
        agent_config
            .as_ref()
            .and_then(|config| config.selection.as_ref()),
        model.as_ref().map(Model::as_str),
    )
    .unwrap_or_else(|| "claude".into());
    let routed_provider =
        crate::credentials::RoutedProvider::for_backend(&backend_name.to_ascii_lowercase());
    let mut identity_override = match (snapshot.provider_account_id.as_deref(), routed_provider) {
        (Some(account_id), Some(provider)) => Some(
            orch.resolve_available_routed_account(
                provider,
                Some(&project_id),
                account_id,
                model.as_ref().map(Model::as_str),
            )
            .ok_or_else(|| {
                format!(
                    "prepared ordinary launch account {account_id} is no longer available for {backend_name}"
                )
            })?,
        ),
        (Some(account_id), None) => {
            return Err(format!(
                "prepared ordinary launch account {account_id} has no routed provider for {backend_name}"
            ));
        }
        (None, _) => None,
    };
    if let Some(metadata) = snapshot.identity_override {
        let identity = identity_override.get_or_insert_with(|| {
            orch.resolve_identity_for_project(Some(&project_id), None)
                .unwrap_or_else(|| crate::identity::UserIdentity {
                    user_id: metadata.user_id.clone(),
                    email: metadata.email.clone(),
                    name: metadata.name.clone(),
                    claude_auth: None,
                    codex_auth: None,
                    github_token: None,
                })
        });
        apply_prepared_identity_metadata(identity, metadata);
    }
    orch.process_state.prepare_admission_claim(
        snapshot.run_id.clone(),
        crate::agent_process::process::ProcessAdmissionClaim {
            request_id: lease.request_id().to_string(),
            workspace_key: lease.workspace_key(),
            launch_key: lease.launch_key().to_string(),
            owner: lease.owner()?,
            generation: lease.generation(),
            team_workspace_id: lease.team_workspace_id(),
        },
    );
    if let OrdinaryLease::Private(request) = &lease {
        spawn_private_heartbeat(
            orch.process_state.clone(),
            orch.db.local.clone(),
            snapshot.run_id.clone(),
            request.request_id.clone(),
            request
                .lease_owner
                .clone()
                .ok_or_else(|| "private ordinary lease has no owner".to_string())?,
            request.lease_generation,
        );
    }
    let claim = orch.claim_job_launch(&snapshot.job_id, &snapshot.run_id);
    let result = start_ordinary_session(
        orch,
        &snapshot.run_id,
        &prompt,
        session_start,
        model,
        agent_config.as_ref(),
        output.as_ref(),
        Some(&snapshot.execution_id),
        identity_override,
    );
    drop(claim);
    match result {
        Ok(()) => {
            super::start_prepared_turn_and_publish(
                orch,
                &snapshot.turn_id,
                &snapshot.run_id,
                &snapshot.run_id,
            )?;
            crate::orchestrator::lifecycle::notify_watchdog_successor_started(
                owning,
                &snapshot.run_id,
                &snapshot.turn_id,
            );
            transition(orch, lease, "starting", "active")?;
            if let OrdinaryLease::Team { request, .. } = lease {
                super::team_admission_lifecycle::start_heartbeat(
                    orch,
                    snapshot.run_id.clone(),
                    request.id.clone(),
                    request.lease_generation,
                );
            }
            Ok(true)
        }
        Err(error) => Err(error),
    }
}

/// Exit reason for the run of a leased launch the promoter withdrew before any
/// process existed for it, because something else already owns the session's
/// launch: its live process, a competing launch, or whoever settled its turn.
const LAUNCH_SUPERSEDED_EXIT_REASON: &str = "launch_superseded";

/// End a leased launch the promoter will not start, before any process existed
/// for it, and return `Ok(false)` (nothing started).
///
/// The leased admission row is the run's only launch cover, so the run and the
/// pending turn it was created to serve are settled first, while the lease
/// still stands and the job's launch lock is held: there is no instant in which
/// the stale-run reaper could find the run uncovered and invent a crash for it.
/// Only then does the lease close, in this generation's one terminal write.
fn withdraw_unlaunched(
    orch: &Orchestrator,
    lease: &OrdinaryLease,
    snapshot: &crate::storage::PreparedOrdinaryLaunch,
    launch_guard: std::sync::MutexGuard<'_, ()>,
    detail: &str,
) -> Result<bool, String> {
    return_undelivered_attention(
        orch,
        &snapshot.run_id,
        &snapshot.turn_id,
        TurnDelivery::Unknown,
    );
    let settled = settle_unlaunched_run(orch, &snapshot.run_id, &snapshot.turn_id)?;
    finish(orch, lease, "cancelled", Some(detail))?;
    // Re-deriving the job may launch it again, and the launch lock is not
    // reentrant.
    drop(launch_guard);
    if settled {
        log::info!(
            "withdrew promoted launch of run {} for job {} before any process: {detail}",
            &snapshot.run_id[..snapshot.run_id.len().min(8)],
            &snapshot.job_id[..snapshot.job_id.len().min(8)],
        );
        if let Err(error) = crate::execution::advancement::recompute_job(orch, &snapshot.job_id) {
            log::warn!(
                "could not re-derive job {} after withdrawing its launch: {error}",
                snapshot.job_id
            );
        }
    }
    Ok(false)
}

/// Settle a still-`starting` run whose launch is being withdrawn before it
/// reached a process, with the pending turn bound to it. Returns whether this
/// call settled it; a run no longer `starting` was settled by whoever ended it,
/// and its turn is theirs too.
///
/// Nothing failed, so the run ends `exited` rather than `crashed`, and the turn
/// `cancelled` as `launch_refused` rather than `failed`: the job projection
/// reads a cancelled turn as resumable, never as failed work, so the job rests
/// on whatever the session's live process does next.
fn settle_unlaunched_run(orch: &Orchestrator, run_id: &str, turn_id: &str) -> Result<bool, String> {
    let dbs = orch.db.clone();
    let run_id = run_id.to_string();
    let turn_id = turn_id.to_string();
    let changes = run_db(async move {
        let db = crate::execution::routing::owning_db_for_run(&dbs, &run_id)
            .await
            .map_err(|e| e.to_string())?;
        let settled = db
            .write({
                let run_id = run_id.clone();
                let turn_id = turn_id.clone();
                move |conn| {
                    let run_id = run_id.clone();
                    let turn_id = turn_id.clone();
                    Box::pin(async move {
                        let now = chrono::Utc::now().timestamp();
                        let changed = conn
                            .execute(
                                "UPDATE runs SET status='exited', exit_reason=?2, exited_at=?3, updated_at=?3 WHERE id=?1 AND status='starting'",
                                cairn_db::turso::params![
                                    run_id.as_str(),
                                    LAUNCH_SUPERSEDED_EXIT_REASON,
                                    now
                                ],
                            )
                            .await?;
                        if changed == 0 {
                            return Ok(false);
                        }
                        conn.execute(
                            // Every turn the launch would have started: pending, or
                            // a retry reserved on this run (`TURN_AWAITS_LAUNCH_SQL`).
                            "UPDATE turns SET state='cancelled', end_reason=?3, ended_at=?4, updated_at=?4 WHERE id=?1 AND run_id=?2 AND (state='pending' OR (state='running' AND start_reason='retry'))",
                            cairn_db::turso::params![
                                turn_id.as_str(),
                                run_id.as_str(),
                                crate::models::TurnEndReason::LaunchRefused.to_string(),
                                now
                            ],
                        )
                        .await?;
                        Ok(true)
                    })
                }
            })
            .await
            .map_err(|e| e.to_string())?;
        if !settled {
            return Ok(None);
        }
        Ok(Some([
            crate::notify::run_db_change_for_id(&db, &run_id, "update").await,
            crate::notify::turn_db_change_for_id(&db, &turn_id, "update").await,
        ]))
    })?;
    let settled = changes.is_some();
    for change in changes.into_iter().flatten() {
        let _ = orch.services.emitter.emit("db-change", change);
    }
    Ok(settled)
}

/// Return the attention delivery a queued launch's turn carried, because the
/// launch is ending without a process.
///
/// A queued launch's briefing stays stamped while it waits, so a concurrent
/// resume cannot drain the same pushes twice. Admission owns it from then on,
/// and every exit that ends the launch short of a process returns it here.
///
/// This never fails its caller. A revert that errors costs one wake, but if the
/// error also skipped the caller's own bookkeeping (failing the job, closing the
/// lease), the job would read as running with no process behind it.
pub(super) fn return_undelivered_attention(
    orch: &Orchestrator,
    run_id: &str,
    turn_id: &str,
    delivery: TurnDelivery,
) {
    let dbs = orch.db.clone();
    let run_id = run_id.to_string();
    let turn_id = turn_id.to_string();
    let _ = run_db(async move {
        return_undelivered_attention_in(&dbs, &run_id, &turn_id, delivery).await;
        Ok::<(), String>(())
    });
}

/// [`return_undelivered_attention`] for a caller already inside async code.
pub(super) async fn return_undelivered_attention_in(
    dbs: &crate::db::DbState,
    run_id: &str,
    turn_id: &str,
    delivery: TurnDelivery,
) {
    let reverted = async {
        let db = crate::execution::routing::owning_db_for_run(dbs, run_id)
            .await
            .map_err(|error| error.to_string())?;
        crate::orchestrator::attention_push::revert_turn_deliveries(&db, turn_id, delivery)
            .await
            .map_err(|error| error.to_string())
    }
    .await;
    match reverted {
        Ok(0) => {}
        Ok(restored) => log::info!(
            "ordinary launch of run {run_id} ended without a process; returned {restored} push(es) from turn {turn_id} to the pending queue"
        ),
        Err(error) => log::warn!(
            "ordinary launch of run {run_id} ended without a process, and returning turn {turn_id}'s attention delivery failed ({error}); that wake will not be redelivered"
        ),
    }
}

fn transition(
    orch: &Orchestrator,
    lease: &OrdinaryLease,
    from: &str,
    to: &str,
) -> Result<(), String> {
    if matches!(lease, OrdinaryLease::Team { .. }) {
        // Postgres owns the team lease; the mirror records this runner's launch
        // phase, which the reaper and boot reconcile read as launch cover. So a
        // phase that did not land fails the start like any other start error:
        // a launch reported successful with a mirror still `leased`/`starting`
        // would shield its run from the reaper after its process died.
        let db = orch.db.local.clone();
        let id = lease.request_id().to_string();
        let phase = to.to_string();
        let recorded = run_db(async move {
            crate::storage::advance_team_mirror_phase(
                &db,
                &id,
                &phase,
                chrono::Utc::now().timestamp(),
            )
            .await
            .map_err(|e| e.to_string())
        })
        .map_err(|error| {
            format!("team ordinary admission could not record launch phase {from}->{to}: {error}")
        })?;
        return if recorded {
            Ok(())
        } else {
            Err(format!(
                "team ordinary admission mirror is gone or already terminal; cannot record {from}->{to}"
            ))
        };
    }
    let db = orch.db.local.clone();
    let id = lease.request_id().to_string();
    let owner = lease.owner()?;
    let generation = lease.generation();
    let from = from.to_string();
    let to = to.to_string();
    let changed = run_db(async move {
        crate::storage::transition_admission(
            &db,
            &id,
            &owner,
            generation,
            &from,
            &to,
            chrono::Utc::now().timestamp(),
        )
        .await
        .map_err(|e| e.to_string())
    })?;
    if changed {
        Ok(())
    } else {
        Err("ordinary admission lease was replaced".into())
    }
}

fn finish(
    orch: &Orchestrator,
    lease: &OrdinaryLease,
    state: &str,
    detail: Option<&str>,
) -> Result<(), String> {
    if let OrdinaryLease::Team { request, .. } = lease {
        return super::team_admission_lifecycle::journal_and_retry_terminal(
            orch,
            &crate::agent_process::process::ProcessAdmissionClaim {
                request_id: request.id.clone(),
                workspace_key: lease.workspace_key(),
                launch_key: request.launch_key.clone(),
                owner: lease.owner()?,
                generation: request.lease_generation,
                team_workspace_id: lease.team_workspace_id(),
            },
            if state == "cancelled" {
                crate::runtime_admission::AdmissionTerminalAction::Cancelled
            } else {
                crate::runtime_admission::AdmissionTerminalAction::Failed
            },
            detail,
        );
    }
    let db = orch.db.local.clone();
    let key = lease.launch_key().to_string();
    let owner = lease.owner()?;
    let generation = lease.generation();
    let state = state.to_string();
    let detail = detail.map(str::to_string);
    run_db(async move {
        crate::storage::finish_admission_by_launch_key(
            &db,
            WORKSPACE,
            &key,
            &owner,
            generation,
            &state,
            detail.as_deref(),
            chrono::Utc::now().timestamp(),
        )
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
    })
}

#[cfg(test)]
mod tests {
    use super::{
        apply_prepared_identity_metadata, heartbeat_private_once_if_current,
        next_private_heartbeat_delay, PRIVATE_HEARTBEAT_INTERVAL, PRIVATE_HEARTBEAT_RETRY_INTERVAL,
    };
    use crate::agent_process::process::{AgentProcessState, ProcessAdmissionClaim};

    async fn leased_private_request(name: &str) -> (crate::storage::LocalDb, AgentProcessState) {
        let db = crate::storage::migrated_test_db(name).await;
        db.execute_script(
            r#"INSERT INTO runtime_admission_requests(
                request_id,workspace_key,resource_lane,launch_key,claim_json,priority,
                enqueue_seq,state,lease_owner,lease_generation,lease_expires_at,created_at,updated_at
             ) VALUES(
                'request-1','private-runtime','codex','ordinary:run-1','{"residentProcessUnits":1,"logicalStreamUnits":1}',0,
                1,'active','ordinary-session-promoter',7,300,0,0
             );"#,
        )
        .await
        .unwrap();
        let state = AgentProcessState::default();
        state.prepare_admission_claim(
            "run-1".into(),
            ProcessAdmissionClaim {
                request_id: "request-1".into(),
                workspace_key: "private-runtime".into(),
                launch_key: "ordinary:run-1".into(),
                owner: "ordinary-session-promoter".into(),
                generation: 7,
                team_workspace_id: None,
            },
        );
        (db, state)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn queued_watchdog_successor_completes_only_after_capacity_promotes_it_once() {
        let (orch, _, snapshot) = lifecycle_fixture("ordinary-queued-watchdog-successor.db").await;
        set_ceiling(&orch, 4);
        let starts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        crate::orchestrator::session::install_test_session_backend(Box::new(CountingBackend(
            starts.clone(),
        )));
        crate::storage::persist_prepared_ordinary_launch(&orch.db.local, snapshot)
            .await
            .unwrap();
        orch.db.local.execute_script(
            "UPDATE runtime_admission_requests
                SET state='queued', lease_owner=NULL, lease_expires_at=NULL
              WHERE request_id='req';
             INSERT INTO runtime_admission_requests(
                request_id,workspace_key,resource_lane,launch_key,claim_json,priority,
                enqueue_seq,state,lease_owner,lease_generation,lease_expires_at,created_at,updated_at
             ) VALUES(
                'blocker','private-runtime','lane','ordinary:blocker',
                '{\"residentProcessUnits\":1,\"logicalStreamUnits\":1}',0,
                2,'active','ordinary-session-promoter',1,9999999999,1,1
             );
             INSERT INTO runtime_admission_requests(
                request_id,workspace_key,resource_lane,launch_key,claim_json,priority,
                enqueue_seq,state,lease_owner,lease_generation,lease_expires_at,created_at,updated_at
             ) VALUES
                ('blocker-2','private-runtime','lane','ordinary:blocker-2','{\"residentProcessUnits\":1,\"logicalStreamUnits\":1}',0,3,'active','ordinary-session-promoter',1,9999999999,1,1),
                ('blocker-3','private-runtime','lane','ordinary:blocker-3','{\"residentProcessUnits\":1,\"logicalStreamUnits\":1}',0,4,'active','ordinary-session-promoter',1,9999999999,1,1),
                ('blocker-4','private-runtime','lane','ordinary:blocker-4','{\"residentProcessUnits\":1,\"logicalStreamUnits\":1}',0,5,'active','ordinary-session-promoter',1,9999999999,1,1);
             INSERT INTO sessions(id,job_id,backend,status,sequence,created_at,updated_at)
               VALUES('predecessor-session','j','claude','open',2,1,1);
             INSERT INTO runs(id,project_id,issue_id,job_id,session_id,status,created_at,updated_at)
               VALUES('predecessor','p','i','j','predecessor-session','live',1,1);
             INSERT INTO turns(id,session_id,run_id,job_id,sequence,state,start_reason,created_at,updated_at)
               VALUES('predecessor-turn','predecessor-session','predecessor','j',1,'running','initial',1,1);",
        ).await.unwrap();
        let identity = crate::runs::watchdog_ledger::WatchdogIdentity {
            run_id: "predecessor".into(),
            session_id: "predecessor-session".into(),
            provider_turn_id: "provider-turn".into(),
            generation: "generation".into(),
        };
        crate::runs::watchdog_ledger::arm_watchdog(
            &orch.db.local,
            crate::runs::watchdog_ledger::NewWatchdogLease {
                identity: identity.clone(),
                runner_boot_id: "boot".into(),
                phase: crate::runs::watchdog_ledger::WatchdogPhase::ProviderProgress,
                phase_deadline_at: 2,
                now: 1,
            },
            "provider silence".into(),
        )
        .await
        .unwrap();
        assert!(crate::runs::watchdog_ledger::claim_watchdog_recovery(
            &orch.db.local,
            &identity,
            3,
        )
        .await
        .unwrap());
        assert!(
            crate::runs::watchdog_ledger::record_pending_recovery_successor(
                &orch.db.local,
                &identity,
                3,
                "cold",
                "t",
            )
            .await
            .unwrap()
        );

        super::promote_private_lane_blocking(&orch, "lane").unwrap();
        assert_eq!(starts.load(std::sync::atomic::Ordering::SeqCst), 0);
        let lease = crate::runs::watchdog_ledger::get_watchdog_lease(
            &orch.db.local,
            "predecessor",
            "predecessor-session",
            "provider-turn",
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            lease.state,
            crate::runs::watchdog_ledger::WatchdogLeaseState::RecoveryClaimed
        );
        assert_eq!(lease.successor_run_id.as_deref(), Some("cold"));
        assert_eq!(lease.successor_turn_id.as_deref(), Some("t"));
        assert_eq!(lease.terminal_reason, None);
        assert_eq!(
            crate::runs::watchdog_ledger::list_watchdog_lifecycle(&orch.db.local, &identity)
                .await
                .unwrap()
                .into_iter()
                .filter(|event| event.kind
                    == crate::runs::watchdog_ledger::WatchdogLifecycleKind::SuccessorStarted)
                .count(),
            0,
        );

        assert!(crate::storage::finish_admission_by_launch_key(
            &orch.db.local,
            "private-runtime",
            "ordinary:blocker",
            "ordinary-session-promoter",
            1,
            "released",
            None,
            chrono::Utc::now().timestamp(),
        )
        .await
        .unwrap());
        super::promote_private_lane_blocking(&orch, "lane").unwrap();
        super::promote_private_lane_blocking(&orch, "lane").unwrap();

        assert_eq!(starts.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(orch.process_state.has_process("cold"), Some(true));
        assert_eq!(
            orch.process_state.get_current_turn_id("cold").as_deref(),
            Some("t")
        );
        let lease = crate::runs::watchdog_ledger::get_watchdog_lease(
            &orch.db.local,
            "predecessor",
            "predecessor-session",
            "provider-turn",
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            lease.state,
            crate::runs::watchdog_ledger::WatchdogLeaseState::Terminalized
        );
        assert_eq!(
            lease.terminal_reason.as_deref(),
            Some("provider_silence_recovery")
        );
        assert_eq!(lease.successor_run_id.as_deref(), Some("cold"));
        assert_eq!(lease.successor_turn_id.as_deref(), Some("t"));
        assert_eq!(
            crate::runs::watchdog_ledger::list_watchdog_lifecycle(&orch.db.local, &identity)
                .await
                .unwrap()
                .into_iter()
                .filter(|event| event.kind
                    == crate::runs::watchdog_ledger::WatchdogLifecycleKind::SuccessorStarted)
                .count(),
            1,
        );
    }

    #[tokio::test]
    async fn private_heartbeat_keeps_process_leased_beyond_original_lifetime() {
        let (db, state) = leased_private_request("private-heartbeat-lifetime.db").await;

        assert!(heartbeat_private_once_if_current(
            &state,
            &db,
            "run-1",
            "request-1",
            "ordinary-session-promoter",
            7,
            240,
        )
        .await
        .unwrap());
        assert!(heartbeat_private_once_if_current(
            &state,
            &db,
            "run-1",
            "request-1",
            "ordinary-session-promoter",
            7,
            480,
        )
        .await
        .unwrap());

        assert_eq!(
            crate::storage::reap_expired_admissions(&db, 601)
                .await
                .unwrap(),
            0
        );
        let request = crate::storage::get_admission_request(&db, "request-1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(request.state, "active");
        assert_eq!(request.lease_expires_at, Some(780));
    }

    #[tokio::test]
    async fn private_heartbeat_stops_before_touching_a_replacement_generation() {
        let (db, state) = leased_private_request("private-heartbeat-stale.db").await;
        state.prepare_admission_claim(
            "run-1".into(),
            ProcessAdmissionClaim {
                request_id: "request-1".into(),
                workspace_key: "private-runtime".into(),
                launch_key: "ordinary:run-1".into(),
                owner: "replacement".into(),
                generation: 8,
                team_workspace_id: None,
            },
        );

        assert!(!heartbeat_private_once_if_current(
            &state,
            &db,
            "run-1",
            "request-1",
            "ordinary-session-promoter",
            7,
            240,
        )
        .await
        .unwrap());
        let request = crate::storage::get_admission_request(&db, "request-1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(request.lease_expires_at, Some(300));
    }

    #[test]
    fn private_heartbeat_retries_transient_errors_without_losing_its_generation() {
        let transient = Err("database temporarily unavailable".to_string());
        assert_eq!(
            next_private_heartbeat_delay(&transient),
            Some(PRIVATE_HEARTBEAT_RETRY_INTERVAL)
        );
        assert_eq!(
            next_private_heartbeat_delay(&Ok(true)),
            Some(PRIVATE_HEARTBEAT_INTERVAL)
        );
        assert_eq!(next_private_heartbeat_delay(&Ok(false)), None);
    }

    use super::run_db;
    use crate::backends::{AgentBackend, ResolvedTools, SessionConfig};
    use crate::db::DbState;
    use crate::orchestrator::Orchestrator;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{RowExt, SearchIndex};
    use cairn_models::types::catalog::DiscoveredModel;
    use std::io::Write;
    use std::sync::{Arc, Mutex};

    struct CountingBackend(Arc<std::sync::atomic::AtomicUsize>);

    impl AgentBackend for CountingBackend {
        fn name(&self) -> &str {
            "Claude"
        }
        fn is_available(&self) -> Result<(), String> {
            Ok(())
        }
        fn discover_models(&self) -> Result<Vec<DiscoveredModel>, String> {
            Ok(Vec::new())
        }
        fn serves(
            &self,
            _: &crate::orchestrator::Orchestrator,
            _: Option<&str>,
        ) -> Vec<cairn_models::types::catalog::ServedVerb> {
            Vec::new()
        }
        fn resolve_tools(&self, _: &[String], _: &[String]) -> ResolvedTools {
            ResolvedTools {
                allowed: Vec::new(),
                disallowed: Vec::new(),
            }
        }
        fn start_session(
            &self,
            config: SessionConfig,
            orch: &Orchestrator,
            _admitted: crate::backends::Admitted,
        ) -> Result<(), String> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let run_id = config.run_id;
            orch.process_state
                .register_process(
                    run_id.clone(),
                    crate::agent_process::process::RunHandle::test_handle(Some("s"), Some("j")),
                )
                .map_err(|error| error.to_string())?;
            orch.process_state.begin_turn(&run_id, "t");
            Ok(())
        }
        fn supports_resume(&self) -> bool {
            true
        }
        fn supports_warm_processes(&self) -> bool {
            true
        }
    }

    /// One real Codex reader [`ProviderFirstBackend`] started, with the provider's
    /// end of its notification channel.
    struct StartedReader {
        provider: crossbeam_channel::Sender<serde_json::Value>,
        reader: std::thread::JoinHandle<()>,
        markers_before_session_start_returned: i64,
    }

    /// Starts a launch the way a Codex session does when the provider is as early
    /// as it can be: the process is registered under the launch's real identity,
    /// the real reader is running, and `turn/started` is on its channel before
    /// `start_session` returns, so before the launch persists or publishes its
    /// turn. It then gives the reader time to read that notification and records
    /// whether a marker appeared, which would mean the reader admitted the turn
    /// ahead of publication.
    struct ProviderFirstBackend(Arc<Mutex<Vec<StartedReader>>>);

    impl AgentBackend for ProviderFirstBackend {
        fn name(&self) -> &str {
            "Codex"
        }
        fn is_available(&self) -> Result<(), String> {
            Ok(())
        }
        fn discover_models(&self) -> Result<Vec<DiscoveredModel>, String> {
            Ok(Vec::new())
        }
        fn serves(
            &self,
            _: &crate::orchestrator::Orchestrator,
            _: Option<&str>,
        ) -> Vec<cairn_models::types::catalog::ServedVerb> {
            Vec::new()
        }
        fn resolve_tools(&self, _: &[String], _: &[String]) -> ResolvedTools {
            ResolvedTools {
                allowed: Vec::new(),
                disallowed: Vec::new(),
            }
        }
        fn start_session(
            &self,
            config: SessionConfig,
            orch: &Orchestrator,
            _admitted: crate::backends::Admitted,
        ) -> Result<(), String> {
            let run_id = config.run_id;
            let session_id = config.session_start.session_id().to_string();
            let db = orch.db.local.clone();
            let job_id = run_db({
                let db = db.clone();
                let run_id = run_id.clone();
                async move {
                    db.query_opt_text("SELECT job_id FROM runs WHERE id = ?1", (run_id,))
                        .await
                        .map_err(|error| error.to_string())
                }
            })?
            .ok_or("the launch's run has no job")?;
            orch.process_state
                .register_process(
                    run_id.clone(),
                    crate::agent_process::process::RunHandle::test_handle(
                        Some(&session_id),
                        Some(&job_id),
                    ),
                )
                .map_err(|error| error.to_string())?;

            let (provider, notifications) = crossbeam_channel::unbounded();
            let reader = {
                let orch = orch.clone();
                let run_id = run_id.clone();
                let db = db.clone();
                std::thread::spawn(move || {
                    crate::backends::codex::CodexBackend::reader_thread_app_server(
                        &orch,
                        &orch.services.emitter,
                        &run_id,
                        Some(session_id),
                        notifications,
                        Arc::new(
                            cairn_models::codex::app_server::AppServerClient::for_test_scripted(
                                vec![],
                                Arc::new(Mutex::new(Vec::new())),
                            ),
                        ),
                        Arc::new(Mutex::new(None)),
                        None,
                        "codex".into(),
                        db,
                        None,
                    )
                })
            };
            provider
                .send(serde_json::json!({
                    "method": "turn/started",
                    "params": {"turn": {"id": format!("provider-{run_id}")}},
                }))
                .map_err(|error| error.to_string())?;
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            while !provider.is_empty() && std::time::Instant::now() < deadline {
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
            let markers = run_db(async move {
                db.query_opt_i64(
                    "SELECT COUNT(*) FROM events WHERE run_id = ?1 AND event_type = 'system:codex_goal_turn_started'",
                    (run_id,),
                )
                .await
                .map_err(|error| error.to_string())
            })?
            .unwrap_or_default();
            self.0.lock().unwrap().push(StartedReader {
                provider,
                reader,
                markers_before_session_start_returned: markers,
            });
            Ok(())
        }
        fn supports_resume(&self) -> bool {
            true
        }
        fn supports_warm_processes(&self) -> bool {
            true
        }
    }

    /// A launch shaped like the specimens' fresh Review nodes: a delegated
    /// `pr-review` job whose prompt opens with the `create-pr` handoff, holding a
    /// `pending` initial turn bound to its run, as enqueue leaves it.
    async fn queue_review_launch(
        orch: &Orchestrator,
        template: &crate::storage::PreparedOrdinaryLaunch,
        n: u32,
    ) {
        let prompt = "The create-pr artifact you are reviewing follows. Review this change.";
        orch.db.local.execute_script(format!("\
            INSERT INTO jobs(id,execution_id,recipe_node_id,issue_id,project_id,node_name,uri_segment,agent_config_id,parent_job_id,status,current_session_id,created_at,updated_at) VALUES('j{n}','e','review{n}','i','p','Review','review{n}','pr-review','j','running','s{n}',1,1);\
            INSERT INTO sessions(id,job_id,backend,status,sequence,created_at,updated_at) VALUES('s{n}','j{n}','codex','open',1,1,1);\
            INSERT INTO runs(id,project_id,issue_id,job_id,session_id,status,start_mode,created_at,updated_at) VALUES('r{n}','p','i','j{n}','s{n}','starting','fresh',1,1);\
            INSERT INTO turns(id,session_id,run_id,job_id,sequence,state,start_reason,created_at,updated_at) VALUES('t{n}','s{n}','r{n}','j{n}',1,'pending','initial',1,1);\
            INSERT INTO events(id,run_id,turn_id,sequence,event_type,data,timestamp,created_at) VALUES('ev{n}','r{n}','t{n}',1,'user:launch','{{\"eventType\":\"user:launch\",\"content\":\"{prompt}\",\"isError\":false}}',1,1);\
            INSERT INTO runtime_admission_requests(request_id,workspace_key,resource_lane,launch_key,claim_json,priority,enqueue_seq,state,lease_owner,lease_generation,lease_expires_at,created_at,updated_at) VALUES('req{n}','private-runtime','lane','ordinary:r{n}','{{\"residentProcessUnits\":1,\"logicalStreamUnits\":1}}',0,{n},'queued',NULL,0,NULL,1,1);"
        )).await.unwrap();
        let mut launch = template.clone();
        launch.request_id = format!("req{n}");
        launch.job_id = format!("j{n}");
        launch.run_id = format!("r{n}");
        launch.session_id = format!("s{n}");
        launch.turn_id = format!("t{n}");
        launch.prompt = prompt.to_string();
        launch.model = "gpt-5.6-sol".to_string();
        launch.reason = crate::models::TurnStartReason::Initial;
        launch.session_start = crate::storage::PreparedSessionStart::New {
            session_id: format!("s{n}"),
        };
        crate::storage::persist_prepared_ordinary_launch(&orch.db.local, launch)
            .await
            .unwrap();
    }

    /// The provider's first turn on launch `n` bound to its one launch turn, once,
    /// and the launch is healthy.
    async fn assert_first_provider_turn_bound(orch: &Orchestrator, n: u32) {
        for (sql, expected) in [
            (
                format!("SELECT CAST(COUNT(*) AS TEXT) FROM events WHERE run_id='r{n}' AND event_type='system:codex_goal_turn_started'"),
                "1".to_string(),
            ),
            (
                format!("SELECT turn_id FROM events WHERE run_id='r{n}' AND event_type='system:codex_goal_turn_started'"),
                format!("t{n}"),
            ),
            (
                format!("SELECT json_extract(data,'$.providerTurnId') FROM events WHERE run_id='r{n}' AND event_type='system:codex_goal_turn_started'"),
                format!("provider-r{n}"),
            ),
            (format!("SELECT state FROM turns WHERE id='t{n}'"), "running".to_string()),
            (format!("SELECT run_id FROM turns WHERE id='t{n}'"), format!("r{n}")),
            (
                format!("SELECT CAST(COUNT(*) AS TEXT) FROM turns WHERE job_id='j{n}'"),
                "1".to_string(),
            ),
            (format!("SELECT current_turn_id FROM jobs WHERE id='j{n}'"), format!("t{n}")),
            (format!("SELECT COALESCE(exit_reason,'none') FROM runs WHERE id='r{n}'"), "none".to_string()),
            (
                format!("SELECT CAST(COUNT(*) AS TEXT) FROM events WHERE run_id='r{n}' AND event_type='system:error'"),
                "0".to_string(),
            ),
        ] {
            assert_eq!(text(orch, &sql).await, expected, "{sql}");
        }
    }

    /// The provider's `turn/started` can arrive before `start_session` returns.
    /// Driving two launches in a row through one orchestrator keeps process state
    /// left by the first from passing for the second.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_providers_first_turn_binds_to_the_launch_turn_on_consecutive_fresh_launches() {
        let (orch, _, template) = lifecycle_fixture("ordinary-codex-provider-first.db").await;
        set_ceiling(&orch, 4);
        let readers = Arc::new(Mutex::new(Vec::new()));
        let started = readers.clone();
        let _backend = crate::orchestrator::session::install_orchestrator_test_session_backend(
            &orch,
            move || Box::new(ProviderFirstBackend(started.clone())),
        );
        orch.db
            .local
            .execute(
                "UPDATE runtime_admission_requests SET state='released' WHERE request_id='req'",
                (),
            )
            .await
            .unwrap();

        for n in [2, 3] {
            queue_review_launch(&orch, &template, n).await;
            promote_until_started(&orch, &format!("r{n}")).await;
            let StartedReader {
                provider,
                reader,
                markers_before_session_start_returned,
            } = readers
                .lock()
                .unwrap()
                .pop()
                .expect("the launch started its reader");
            assert_eq!(
                markers_before_session_start_returned, 0,
                "launch {n}: the reader must not admit the turn before it is published"
            );
            drop(provider);
            reader.join().unwrap();
            assert_first_provider_turn_bound(&orch, n).await;
        }
        assert_first_provider_turn_bound(&orch, 2).await;
    }

    /// Registers its process (moving the admission claim onto the live handle)
    /// and then fails, as a backend does when opening its first turn fails.
    struct RegisterThenFailBackend;

    impl AgentBackend for RegisterThenFailBackend {
        fn name(&self) -> &str {
            "Claude"
        }
        fn is_available(&self) -> Result<(), String> {
            Ok(())
        }
        fn discover_models(&self) -> Result<Vec<DiscoveredModel>, String> {
            Ok(Vec::new())
        }
        fn serves(
            &self,
            _: &crate::orchestrator::Orchestrator,
            _: Option<&str>,
        ) -> Vec<cairn_models::types::catalog::ServedVerb> {
            Vec::new()
        }
        fn resolve_tools(&self, _: &[String], _: &[String]) -> ResolvedTools {
            ResolvedTools {
                allowed: Vec::new(),
                disallowed: Vec::new(),
            }
        }
        fn start_session(
            &self,
            config: SessionConfig,
            orch: &Orchestrator,
            _admitted: crate::backends::Admitted,
        ) -> Result<(), String> {
            orch.process_state
                .register_process(
                    config.run_id,
                    crate::agent_process::process::RunHandle::test_handle(Some("s"), Some("j")),
                )
                .map_err(|error| error.to_string())?;
            Err("open turn failed after registration".into())
        }
        fn supports_resume(&self) -> bool {
            true
        }
        fn supports_warm_processes(&self) -> bool {
            true
        }
    }

    /// The team mirror's `active` phase is what ends reaper cover. If recording
    /// it fails after registration, the launch must not report success: the
    /// start fails, the process stops, and the generation's terminal obligation
    /// is journaled, so nothing is left with permanent pre-registration cover.
    #[tokio::test(flavor = "current_thread")]
    async fn a_failed_team_active_phase_write_terminalizes_instead_of_succeeding() {
        let (orch, _, snapshot) = lifecycle_fixture("ordinary-team-active-phase-failure.db").await;
        let starts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        crate::orchestrator::session::install_test_session_backend(Box::new(CountingBackend(
            starts.clone(),
        )));
        // The lease journal belongs to a prepared launch, as in the promoter.
        crate::storage::persist_prepared_ordinary_launch(&orch.db.local, snapshot.clone())
            .await
            .unwrap();
        orch.db
            .local
            .execute_script(
                "UPDATE runtime_admission_requests SET workspace_key='team:t' WHERE request_id='req';
                 INSERT INTO team_ordinary_admission_authority(request_id,lease_owner,lease_generation,lease_host_id,updated_at)
                   VALUES('req','ordinary-session-promoter',1,'host',1);",
            )
            .await
            .unwrap();
        exclusive_ddl(
            &orch,
            "CREATE TRIGGER fail_active_phase BEFORE UPDATE OF state ON runtime_admission_requests
             WHEN NEW.state = 'active' BEGIN SELECT RAISE(ABORT, 'injected active phase failure'); END;",
        )
        .await;
        let request = crate::runtime_admission::AdmissionRequest {
            id: "req".into(),
            launch_key: "ordinary:cold".into(),
            workspace_id: "t".into(),
            backend_lane: "lane".into(),
            runtime_class: "dedicated_process".into(),
            resource_claim: serde_json::json!({"resident_process_units": 1}),
            priority: crate::runtime_admission::AdmissionPriority::Ordinary,
            state: "leased".into(),
            lease_generation: 1,
            lease_owner: Some("ordinary-session-promoter".into()),
            lease_runner_id: Some("ordinary-session-promoter".into()),
            lease_host_id: Some("host".into()),
            lease_expires_at: None,
            admission_policy_revision: None,
            admission_capacity: None,
        };

        let error = super::start_leased(
            &orch,
            super::OrdinaryLease::Team {
                request,
                workspace_id: "t".into(),
            },
            snapshot,
        )
        .unwrap_err();
        assert!(error.contains("injected active phase failure"), "{error}");

        assert_eq!(starts.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_ne!(
            orch.process_state.has_process("cold"),
            Some(true),
            "the process does not outlive a launch that could not record its phase"
        );
        assert_ne!(
            text(&orch, "SELECT status FROM runs WHERE id='cold'").await,
            "starting",
            "the run is terminal, so no pre-registration cover is left protecting it"
        );
        assert_eq!(
            text(&orch, "SELECT CAST(pending_terminal_action IS NOT NULL AS TEXT) FROM team_ordinary_admission_authority WHERE request_id='req'").await,
            "1",
            "the generation's terminal obligation is journaled for retry"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn post_registration_start_failure_stops_the_process_and_releases_its_claim_once() {
        let (orch, request, snapshot) =
            lifecycle_fixture("ordinary-post-registration-failure.db").await;
        crate::orchestrator::session::install_test_session_backend(Box::new(
            RegisterThenFailBackend,
        ));

        let error = super::start_leased(&orch, super::OrdinaryLease::Private(request), snapshot)
            .unwrap_err();
        assert!(error.contains("after registration"), "{error}");

        assert!(
            orch.process_state.find_process_by_session("s").is_none(),
            "a dedicated process must not outlive the release of its claim"
        );
        assert!(orch.process_state.admission_claim("cold").is_none());
        let request = crate::storage::get_admission_request(&orch.db.local, "req")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(request.state, "released");
        assert_eq!(request.lease_generation, 1);
    }

    #[derive(Clone)]
    struct CapturingWriter(Arc<Mutex<Vec<u8>>>);
    impl Write for CapturingWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    async fn lifecycle_fixture(
        name: &str,
    ) -> (
        Orchestrator,
        crate::storage::AdmissionRequest,
        crate::storage::PreparedOrdinaryLaunch,
    ) {
        lifecycle_fixture_with_gc(name, None).await
    }

    /// An explicit operator ceiling, as the settings file would carry it.
    fn set_ceiling(orch: &Orchestrator, ceiling: u32) {
        orch.update_settings(crate::models::UpdateSettings {
            private_dedicated_process_capacity: Some(ceiling),
            ..Default::default()
        })
        .unwrap();
    }

    /// A further job with its own session, run, pending turn bound to that run,
    /// launch event, prepared snapshot and `queued` admission row in `lane`.
    async fn queue_launch(
        orch: &Orchestrator,
        template: &crate::storage::PreparedOrdinaryLaunch,
        n: u32,
        run_status: &str,
    ) {
        orch.db.local.execute_script(format!("\
            INSERT INTO jobs(id,execution_id,recipe_node_id,issue_id,project_id,node_name,uri_segment,status,current_session_id,created_at,updated_at) VALUES('j{n}','e','builder{n}','i','p','Builder','builder{n}','running','s{n}',1,1);\
            INSERT INTO sessions(id,job_id,backend,status,sequence,created_at,updated_at) VALUES('s{n}','j{n}','claude','open',1,1,1);\
            INSERT INTO runs(id,project_id,issue_id,job_id,session_id,status,start_mode,created_at,updated_at) VALUES('r{n}','p','i','j{n}','s{n}','{run_status}','resume',1,1);\
            INSERT INTO turns(id,session_id,run_id,job_id,sequence,state,start_reason,created_at,updated_at) VALUES('t{n}','s{n}','r{n}','j{n}',1,'pending','resume',1,1);\
            INSERT INTO events(id,run_id,turn_id,sequence,event_type,data,timestamp,created_at) VALUES('ev{n}','r{n}','t{n}',1,'user','{{\"eventType\":\"user\",\"content\":\"queued {n}\",\"isError\":false}}',1,1);\
            INSERT INTO runtime_admission_requests(request_id,workspace_key,resource_lane,launch_key,claim_json,priority,enqueue_seq,state,lease_owner,lease_generation,lease_expires_at,created_at,updated_at) VALUES('req{n}','private-runtime','lane','ordinary:r{n}','{{\"residentProcessUnits\":1,\"logicalStreamUnits\":1}}',0,{n},'queued',NULL,0,NULL,1,1);"
        )).await.unwrap();
        let mut launch = template.clone();
        launch.request_id = format!("req{n}");
        launch.job_id = format!("j{n}");
        launch.run_id = format!("r{n}");
        launch.session_id = format!("s{n}");
        launch.turn_id = format!("t{n}");
        launch.session_start = crate::storage::PreparedSessionStart::New {
            session_id: format!("s{n}"),
        };
        crate::storage::persist_prepared_ordinary_launch(&orch.db.local, launch)
            .await
            .unwrap();
    }

    /// DDL cannot run inside the concurrent transactions `execute_script` opens.
    async fn exclusive_ddl(orch: &Orchestrator, sql: &'static str) {
        orch.db
            .local
            .exclusive(move |conn| {
                Box::pin(async move {
                    conn.execute(sql, ()).await?;
                    Ok(())
                })
            })
            .await
            .unwrap();
    }

    async fn request_state(orch: &Orchestrator, id: &str) -> (String, Option<String>) {
        let request = crate::storage::get_admission_request(&orch.db.local, id)
            .await
            .unwrap()
            .unwrap();
        (request.state, request.failure_detail)
    }

    async fn text(orch: &Orchestrator, sql: &str) -> String {
        orch.db
            .local
            .query_opt(sql.to_string(), (), |row| row.text(0))
            .await
            .unwrap()
            .unwrap()
    }

    /// Promote until `run` registers. A release re-triggers promotion in the
    /// background, so an explicit pass can find the next head already leased
    /// and mid-start; yielding lets that pass finish rather than racing it.
    /// Callers still assert the start count, so a double start is never hidden.
    async fn promote_until_started(orch: &Orchestrator, run: &str) {
        for _ in 0..200 {
            super::promote_private_lane_blocking(orch, "lane").unwrap();
            if orch.process_state.has_process(run) == Some(true) {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("{run} was never promoted");
    }

    /// The fixture's own `req` as the lane's one resident process.
    async fn hold_cold_active(orch: &Orchestrator) {
        orch.db
            .local
            .execute(
                "UPDATE runtime_admission_requests SET state='active' WHERE request_id='req'",
                (),
            )
            .await
            .unwrap();
        orch.process_state.prepare_admission_claim(
            "cold".into(),
            ProcessAdmissionClaim {
                request_id: "req".into(),
                workspace_key: "private-runtime".into(),
                launch_key: "ordinary:cold".into(),
                owner: "ordinary-session-promoter".into(),
                generation: 1,
                team_workspace_id: None,
            },
        );
        orch.process_state
            .register_process(
                "cold".into(),
                crate::agent_process::process::RunHandle::test_handle(Some("s"), Some("j")),
            )
            .unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn saturation_queues_launches_past_the_grace_without_crashing_them() {
        let (orch, _, snapshot) = lifecycle_fixture("ordinary-saturation.db").await;
        let starts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted = starts.clone();
        let _backend = crate::orchestrator::session::install_orchestrator_test_session_backend(
            &orch,
            move || Box::new(CountingBackend(counted.clone())),
        );
        set_ceiling(&orch, 1);
        hold_cold_active(&orch).await;
        queue_launch(&orch, &snapshot, 2, "starting").await;
        queue_launch(&orch, &snapshot, 3, "starting").await;

        // Both queued runs were created at t=1, far past the stale-run grace,
        // and neither has a process or an in-memory claim. Their durable
        // admission rows are the claim.
        assert_eq!(crate::runs::reap::reap_stale_runs(&orch), 0);
        for run in ["r2", "r3"] {
            assert_eq!(
                text(&orch, &format!("SELECT status FROM runs WHERE id='{run}'")).await,
                "starting",
                "{run} must stay queued, not crashed"
            );
        }
        let positions: Vec<_> = crate::storage::provider_admission_records(&orch.db.local, &[])
            .await
            .unwrap()
            .into_iter()
            .filter(|projection| projection.position.is_some())
            .map(|projection| (projection.owner.run_id.unwrap(), projection.position))
            .collect();
        assert_eq!(
            positions,
            vec![("r2".to_string(), Some(1)), ("r3".to_string(), Some(2))]
        );

        assert!(crate::orchestrator::lifecycle::release_and_stop_process(
            &orch, "cold"
        ));
        promote_until_started(&orch, "r2").await;
        assert_eq!(starts.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(request_state(&orch, "req3").await.0, "queued");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn promoter_cancels_a_terminal_runs_request_and_takes_the_next_without_failing_the_job_twice(
    ) {
        let (orch, _, snapshot) = lifecycle_fixture("ordinary-terminal-head.db").await;
        let starts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        crate::orchestrator::session::install_test_session_backend(Box::new(CountingBackend(
            starts.clone(),
        )));
        orch.db
            .local
            .execute(
                "UPDATE runtime_admission_requests SET state='released' WHERE request_id='req'",
                (),
            )
            .await
            .unwrap();
        queue_launch(&orch, &snapshot, 2, "crashed").await;
        queue_launch(&orch, &snapshot, 3, "starting").await;

        super::promote_private_lane_blocking(&orch, "lane").unwrap();

        assert_eq!(
            request_state(&orch, "req2").await,
            (
                "cancelled".to_string(),
                Some("run or turn is no longer launchable".to_string())
            )
        );
        assert_eq!(starts.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(orch.process_state.has_process("r3"), Some(true));
        // The crash was already the job's outcome: no start failure on top.
        assert_eq!(
            text(&orch, "SELECT status FROM jobs WHERE id='j2'").await,
            "running"
        );
        assert_eq!(
            text(
                &orch,
                "SELECT CAST(COUNT(*) AS TEXT) FROM runs WHERE job_id='j2'"
            )
            .await,
            "1"
        );
        assert_eq!(
            text(&orch, "SELECT CAST(COUNT(*) AS TEXT) FROM events WHERE run_id='r2' AND event_type<>'user'").await,
            "0"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn recover_on_startup_withdraws_queued_rows_whose_run_is_terminal() {
        let (orch, _, snapshot) = lifecycle_fixture("ordinary-recover-terminal.db").await;
        let starts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted = starts.clone();
        let _backend = crate::orchestrator::session::install_orchestrator_test_session_backend(
            &orch,
            move || Box::new(CountingBackend(counted.clone())),
        );
        orch.db
            .local
            .execute(
                "UPDATE runtime_admission_requests SET state='released' WHERE request_id='req'",
                (),
            )
            .await
            .unwrap();
        queue_launch(&orch, &snapshot, 2, "crashed").await;

        super::recover_on_startup(&orch).await;

        assert_eq!(
            request_state(&orch, "req2").await,
            (
                "cancelled".to_string(),
                Some("run settled before admission".to_string())
            )
        );
        assert_eq!(starts.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stop_job_of_a_queued_launch_withdraws_its_row_and_leaves_the_job_steerable() {
        let (orch, _, _) = lifecycle_fixture("ordinary-stop-queued.db").await;
        orch.db.local.execute(
            "UPDATE runtime_admission_requests SET state='queued', lease_owner=NULL, lease_expires_at=NULL WHERE request_id='req'",
            (),
        ).await.unwrap();

        crate::orchestrator::lifecycle::stop_job(&orch, "j").unwrap();

        assert_eq!(request_state(&orch, "req").await.0, "cancelled");
        assert_eq!(
            text(&orch, "SELECT status FROM runs WHERE id='cold'").await,
            "exited"
        );
        assert_eq!(
            text(&orch, "SELECT state FROM turns WHERE id='t'").await,
            "cancelled"
        );
        assert!(
            !crate::messages::delivery::head_turn_active_sync(&orch.db.local, "j"),
            "a stopped queued launch must not leave the job reading as mid-turn"
        );
    }

    /// Deliver one push to `recipient` through a briefing on `turn`, the way a
    /// cold continuation does before it hands its launch to admission.
    async fn brief(orch: &Orchestrator, run: &str, turn: &str, recipient: &str) -> String {
        use crate::orchestrator::attention_push as attention;
        orch.db.local.execute_script(format!(
            "INSERT INTO events(id,run_id,turn_id,sequence,event_type,data,timestamp,created_at) VALUES('brief-{turn}','{run}','{turn}',2,'attention:briefing','{{}}',1,1);"
        )).await.unwrap();
        let (id, _) = attention::push(
            &orch.db.local,
            recipient,
            "cairn://p/P/1",
            attention::Wake::Wake,
            attention::Boundary::Event,
            &format!("direct:{turn}"),
        )
        .await
        .unwrap();
        assert_eq!(
            attention::stamp_delivered(
                &orch.db.local,
                std::slice::from_ref(&id),
                &format!("brief-{turn}")
            )
            .await
            .unwrap(),
            1
        );
        id
    }

    /// Whether the push is pending again and its briefing gone: the state a
    /// delivery that never happened must leave.
    async fn delivery_returned(orch: &Orchestrator, recipient: &str, turn: &str) -> bool {
        let pending = crate::orchestrator::attention_push::list_pending(&orch.db.local, recipient)
            .await
            .unwrap()
            .len();
        let briefings = text(
            orch,
            &format!("SELECT CAST(COUNT(*) AS TEXT) FROM events WHERE id='brief-{turn}'"),
        )
        .await;
        match (pending, briefings.as_str()) {
            (1, "0") => true,
            (0, "1") => false,
            other => panic!("a delivery is undone whole or not at all, got {other:?}"),
        }
    }

    /// A leased launch the promoter cancels never reached a process, so the
    /// wake its turn carried goes back to the queue rather than being spent.
    #[tokio::test(flavor = "current_thread")]
    async fn a_cancelled_launch_returns_its_turns_attention_delivery() {
        let (orch, _, snapshot) = lifecycle_fixture("ordinary-cancel-returns-wake.db").await;
        orch.db
            .local
            .execute(
                "UPDATE runtime_admission_requests SET state='released' WHERE request_id='req'",
                (),
            )
            .await
            .unwrap();
        queue_launch(&orch, &snapshot, 2, "crashed").await;
        brief(&orch, "r2", "t2", "j2").await;
        assert!(!delivery_returned(&orch, "j2", "t2").await);

        super::promote_private_lane_blocking(&orch, "lane").unwrap();

        assert_eq!(
            request_state(&orch, "req2").await,
            (
                "cancelled".to_string(),
                Some("run or turn is no longer launchable".to_string())
            ),
            "the launch was attempted and cancelled at promotion"
        );
        assert!(delivery_returned(&orch, "j2", "t2").await);
    }

    /// The same cancellation after the launch's run already went live (a lease
    /// requeued after its process ran) keeps the delivery: a process received
    /// that prompt, so returning the push would deliver it twice.
    #[tokio::test(flavor = "current_thread")]
    async fn a_cancelled_launch_keeps_a_delivery_its_run_already_carried() {
        let (orch, _, snapshot) = lifecycle_fixture("ordinary-cancel-keeps-delivery.db").await;
        orch.db
            .local
            .execute(
                "UPDATE runtime_admission_requests SET state='released' WHERE request_id='req'",
                (),
            )
            .await
            .unwrap();
        queue_launch(&orch, &snapshot, 2, "crashed").await;
        orch.db
            .local
            .execute("UPDATE runs SET started_at=1 WHERE id='r2'", ())
            .await
            .unwrap();
        brief(&orch, "r2", "t2", "j2").await;

        super::promote_private_lane_blocking(&orch, "lane").unwrap();

        assert_eq!(request_state(&orch, "req2").await.0, "cancelled");
        assert!(!delivery_returned(&orch, "j2", "t2").await);
    }

    /// Boot recovery withdraws a queued row whose run settled while the runner
    /// was down, and returns the wake with it.
    #[tokio::test(flavor = "current_thread")]
    async fn recovery_withdrawal_returns_the_launchs_attention_delivery() {
        let (orch, _, snapshot) = lifecycle_fixture("ordinary-recover-returns-wake.db").await;
        orch.db
            .local
            .execute(
                "UPDATE runtime_admission_requests SET state='released' WHERE request_id='req'",
                (),
            )
            .await
            .unwrap();
        queue_launch(&orch, &snapshot, 2, "crashed").await;
        brief(&orch, "r2", "t2", "j2").await;

        super::recover_on_startup(&orch).await;

        assert_eq!(
            request_state(&orch, "req2").await,
            (
                "cancelled".to_string(),
                Some("run settled before admission".to_string())
            )
        );
        assert!(delivery_returned(&orch, "j2", "t2").await);
    }

    /// The orphan sweep cancels a queued private row and returns its wake in one
    /// write, so an interruption cannot leave a cancelled row (which no sweep
    /// revisits) beside a stamped wake. With the cancellation forced to fail,
    /// nothing moves; the next sweep then does both.
    #[tokio::test(flavor = "current_thread")]
    async fn an_interrupted_orphan_withdrawal_leaves_row_and_wake_together() {
        let (orch, _, snapshot) = lifecycle_fixture("ordinary-orphan-atomic.db").await;
        orch.db
            .local
            .execute(
                "UPDATE runtime_admission_requests SET state='released' WHERE request_id='req'",
                (),
            )
            .await
            .unwrap();
        queue_launch(&orch, &snapshot, 2, "crashed").await;
        brief(&orch, "r2", "t2", "j2").await;
        exclusive_ddl(
            &orch,
            "CREATE TRIGGER interrupt_withdrawal BEFORE UPDATE OF state ON runtime_admission_requests
             WHEN NEW.state = 'cancelled' BEGIN SELECT RAISE(ABORT, 'interrupted withdrawal'); END;",
        )
        .await;
        let sweep = |orch: Orchestrator| {
            tokio::task::spawn_blocking(move || {
                crate::execution::jobs::withdraw_orphaned_ordinary_admissions(&orch)
            })
        };

        assert_eq!(sweep(orch.clone()).await.unwrap(), 0);
        assert_eq!(request_state(&orch, "req2").await.0, "queued");
        assert!(
            !delivery_returned(&orch, "j2", "t2").await,
            "the failed write took the revert with it"
        );

        exclusive_ddl(&orch, "DROP TRIGGER interrupt_withdrawal").await;
        assert_eq!(sweep(orch.clone()).await.unwrap(), 1);
        assert_eq!(request_state(&orch, "req2").await.0, "cancelled");
        assert!(delivery_returned(&orch, "j2", "t2").await);
    }

    /// A warm process that won the launch race is handed the queued prompt.
    /// Delivery binds the turn to that already-live run before it sends, so if
    /// the send fails the run's `started_at` proves nothing: the wake must still
    /// come back. The successful send is the witness that the same setup
    /// otherwise delivers and keeps the stamp.
    #[tokio::test(flavor = "current_thread")]
    async fn a_failed_warm_race_handoff_returns_the_wake_and_a_sent_one_keeps_it() {
        for sends in [false, true] {
            let (orch, request, snapshot) =
                lifecycle_fixture(&format!("ordinary-warm-race-wake-{sends}.db")).await;
            orch.db.local.execute("INSERT INTO runs(id,project_id,issue_id,job_id,session_id,status,started_at,created_at,updated_at) VALUES('warm','p','i','j','s','live',1,1,1)", ()).await.unwrap();
            brief(&orch, "cold", "t", "j").await;
            let bytes = Arc::new(Mutex::new(Vec::new()));
            let stdin = Arc::new(Mutex::new(sends.then(|| {
                cairn_models::session::claude::ClaudeControl::boxed(Box::new(CapturingWriter(
                    bytes.clone(),
                )))
            })));
            let handle = crate::agent_process::process::RunHandle::new(
                Arc::new(Mutex::new(None)),
                stdin,
                Some("s".into()),
                Some("j".into()),
            );
            orch.process_state
                .register_process("warm".into(), handle)
                .unwrap();
            orch.process_state.transition_to_warm("warm");
            let starts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            crate::orchestrator::session::install_test_session_backend(Box::new(CountingBackend(
                starts.clone(),
            )));

            let result =
                super::start_leased(&orch, super::OrdinaryLease::Private(request), snapshot);

            assert_eq!(starts.load(std::sync::atomic::Ordering::SeqCst), 0);
            assert_eq!(
                text(&orch, "SELECT run_id FROM turns WHERE id='t'").await,
                "warm",
                "the turn was bound to the live warm run before the send"
            );
            if sends {
                assert!(result.is_ok(), "{result:?}");
                assert!(String::from_utf8(bytes.lock().unwrap().clone())
                    .unwrap()
                    .contains("queued hello"));
                assert!(!delivery_returned(&orch, "j", "t").await);
            } else {
                assert!(
                    result.is_err(),
                    "a warm process with no stdin cannot take the prompt"
                );
                assert!(delivery_returned(&orch, "j", "t").await);
            }
        }
    }

    /// Stopping a job whose launch is still queued settles the run and withdraws
    /// the row in one write, and that same write returns the wake, so the next
    /// resume redelivers it.
    #[tokio::test(flavor = "current_thread")]
    async fn stopping_a_queued_launch_returns_its_attention_delivery() {
        let (orch, _, snapshot) = lifecycle_fixture("ordinary-stop-returns-wake.db").await;
        orch.db.local.execute(
            "UPDATE runtime_admission_requests SET state='queued', lease_owner=NULL, lease_expires_at=NULL WHERE request_id='req'",
            (),
        ).await.unwrap();
        crate::storage::persist_prepared_ordinary_launch(&orch.db.local, snapshot)
            .await
            .unwrap();
        brief(&orch, "cold", "t", "j").await;

        crate::orchestrator::lifecycle::stop_job(&orch, "j").unwrap();

        assert_eq!(request_state(&orch, "req").await.0, "cancelled");
        assert!(delivery_returned(&orch, "j", "t").await);
    }

    /// Settling a queued launch is a side effect of the one
    /// `transitions::transition_run`, so the stop path and the start-failed path
    /// both withdraw the row and wake the promoter through it. A second copy of
    /// the transition once carried these effects for only one of the two paths.
    #[tokio::test(flavor = "current_thread")]
    async fn stop_and_start_failure_settle_queued_launches_through_one_transition() {
        let (orch, _, snapshot) = lifecycle_fixture("ordinary-one-transition.db").await;
        orch.db.local.execute(
            "UPDATE runtime_admission_requests SET state='queued', lease_owner=NULL, lease_expires_at=NULL WHERE request_id='req'",
            (),
        ).await.unwrap();
        queue_launch(&orch, &snapshot, 2, "starting").await;
        let mut admission = orch.db.local.subscribe_provider_admission_changes();
        admission.mark_unchanged();

        crate::orchestrator::lifecycle::stop_job(&orch, "j").unwrap();

        assert_eq!(
            request_state(&orch, "req").await,
            (
                "cancelled".to_string(),
                Some("run exited before admission".to_string())
            )
        );
        assert!(
            admission.has_changed().unwrap(),
            "the stop path woke the promoter"
        );
        admission.mark_unchanged();

        crate::execution::jobs::mark_job_start_failed(&orch, &orch.db.local, "j2", "boom", false)
            .await;

        assert_eq!(
            text(&orch, "SELECT status FROM runs WHERE id='r2'").await,
            "crashed"
        );
        assert_eq!(
            request_state(&orch, "req2").await,
            (
                "cancelled".to_string(),
                Some("run crashed before admission".to_string())
            )
        );
        assert!(
            admission.has_changed().unwrap(),
            "the start-failed path woke the promoter"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn blocked_head_evicts_one_unprotected_warm_process_under_a_unit_ceiling() {
        let gc = Arc::new(crate::agent_process::gc::WarmProcessGC::with_probe(
            6,
            Arc::new(crate::agent_process::memory::StubMemoryProbe::new(Some(
                crate::agent_process::memory::SystemMemory {
                    total: 64 << 30,
                    available: 32 << 30,
                },
            ))),
        ));
        let (orch, _, snapshot) =
            lifecycle_fixture_with_gc("ordinary-unit-eviction.db", Some(gc)).await;
        let starts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted = starts.clone();
        let _backend = crate::orchestrator::session::install_orchestrator_test_session_backend(
            &orch,
            move || Box::new(CountingBackend(counted.clone())),
        );
        set_ceiling(&orch, 1);
        hold_cold_active(&orch).await;
        orch.process_state.transition_to_warm("cold");
        queue_launch(&orch, &snapshot, 2, "starting").await;

        // The ceiling is full, so the head is blocked; nothing is mid-spawn,
        // so the promoter gives up the idle warm process holding the unit.
        super::promote_private_lane_blocking(&orch, "lane").unwrap();
        assert_eq!(orch.process_state.has_process("cold"), Some(false));
        assert_eq!(request_state(&orch, "req").await.0, "released");

        promote_until_started(&orch, "r2").await;
        assert_eq!(starts.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn promoter_continues_a_launch_whose_own_enqueue_claim_is_still_held() {
        let (orch, request, snapshot) = lifecycle_fixture("ordinary-own-claim.db").await;
        let starts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        crate::orchestrator::session::install_test_session_backend(Box::new(CountingBackend(
            starts.clone(),
        )));
        // The enqueuer drops its claim just after the row commits; a promoter
        // that leased the row in between is continuing that same launch.
        let enqueue_claim = orch.claim_job_launch("j", "cold");
        assert!(
            super::start_leased(&orch, super::OrdinaryLease::Private(request), snapshot).unwrap()
        );
        drop(enqueue_claim);
        assert_eq!(starts.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn enqueue_holds_the_launch_claim_until_the_row_is_durable() {
        let (orch, _, _) = lifecycle_fixture("ordinary-claim-ordering.db").await;
        let starts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted = starts.clone();
        let _backend = crate::orchestrator::session::install_orchestrator_test_session_backend(
            &orch,
            move || Box::new(CountingBackend(counted.clone())),
        );
        orch.db
            .local
            .execute_script(
                "DELETE FROM runtime_admission_requests WHERE request_id='req';
                 UPDATE turns SET run_id=NULL WHERE id='t';",
            )
            .await
            .unwrap();
        exclusive_ddl(
            &orch,
            "CREATE TRIGGER refuse_admission BEFORE INSERT ON runtime_admission_requests
             BEGIN SELECT RAISE(ABORT, 'injected enqueue failure'); END;",
        )
        .await;
        let prepared = |orch: &Orchestrator| super::PreparedJob {
            job_id: "j".into(),
            project_id: "p".into(),
            run_id: "cold".into(),
            session_id: "s".into(),
            session_start: crate::backends::SessionStart::New {
                session_id: "s".into(),
            },
            prompt: "queued hello".into(),
            job_model: None,
            agent_config: None,
            artifact_schema_info: None,
            execution_id: Some("e".into()),
            turn_id: "t".into(),
            start_reason: crate::models::TurnStartReason::FollowUp,
            origin: crate::storage::LaunchOrigin::Background,
            launch_claim: Some(orch.claim_job_launch("j", "cold")),
        };
        let identity = || {
            Some(crate::identity::UserIdentity {
                user_id: "u".into(),
                email: "u@example.com".into(),
                name: "U".into(),
                claude_auth: None,
                codex_auth: None,
                github_token: None,
            })
        };

        let mut failed = prepared(&orch);
        assert!(super::enqueue_or_start(&orch, &mut failed, identity()).is_err());
        assert!(
            failed.launch_claim.is_some(),
            "a launch whose admission row never became durable still holds its claim"
        );
        assert_eq!(orch.claimed_launch_run("j").as_deref(), Some("cold"));
        drop(failed);

        exclusive_ddl(&orch, "DROP TRIGGER refuse_admission").await;
        orch.db
            .local
            .execute("UPDATE turns SET run_id=NULL WHERE id='t'", ())
            .await
            .unwrap();
        let mut queued = prepared(&orch);
        assert!(!super::enqueue_or_start(&orch, &mut queued, identity()).unwrap());
        assert!(queued.launch_claim.is_none());
        assert_eq!(
            text(&orch, "SELECT run_id FROM turns WHERE id='t'").await,
            "cold",
            "the prepared turn is bound to its run from enqueue"
        );
        assert_eq!(
            text(&orch, "SELECT CAST(COUNT(*) AS TEXT) FROM runtime_admission_requests WHERE launch_key='ordinary:cold'").await,
            "1"
        );
    }

    /// A measured host with ample headroom, so only the ceiling decides.
    fn roomy_gc() -> Arc<crate::agent_process::gc::WarmProcessGC> {
        Arc::new(crate::agent_process::gc::WarmProcessGC::with_probe(
            6,
            Arc::new(crate::agent_process::memory::StubMemoryProbe::new(Some(
                crate::agent_process::memory::SystemMemory {
                    total: 64 << 30,
                    available: 32 << 30,
                },
            ))),
        ))
    }

    /// Wait for `run` to register WITHOUT promoting: the trigger under test
    /// must have scheduled the pass itself, as no sweep runs in these tests.
    async fn wait_until_started(orch: &Orchestrator, run: &str) {
        for _ in 0..500 {
            if orch.process_state.has_process(run) == Some(true) {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("{run} was never promoted by its own trigger");
    }

    /// Process teardown callbacks release on plain OS threads (the provider
    /// reader and exit watcher), which carry no Tokio runtime context. The unit
    /// they free must still wake the queue head immediately.
    #[tokio::test(flavor = "current_thread")]
    async fn release_off_the_runtime_promotes_the_queue_head_without_the_sweep() {
        let (orch, _, snapshot) =
            lifecycle_fixture_with_gc("ordinary-off-runtime-release.db", Some(roomy_gc())).await;
        let starts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted = starts.clone();
        let _backend = crate::orchestrator::session::install_orchestrator_test_session_backend(
            &orch,
            move || Box::new(CountingBackend(counted.clone())),
        );
        set_ceiling(&orch, 1);
        hold_cold_active(&orch).await;
        queue_launch(&orch, &snapshot, 2, "starting").await;

        let releaser = orch.clone();
        let released = std::thread::spawn(move || {
            assert!(tokio::runtime::Handle::try_current().is_err());
            crate::orchestrator::lifecycle::release_and_stop_process(&releaser, "cold")
        })
        .join()
        .unwrap();
        assert!(released);

        wait_until_started(&orch, "r2").await;
        assert_eq!(starts.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(request_state(&orch, "req").await.0, "released");
    }

    /// Once the admission row is durable the launch IS queued, so an enqueue
    /// from a thread without a runtime (attention delivery's flush-on-idle
    /// resume) must report it queued rather than failed.
    #[tokio::test(flavor = "current_thread")]
    async fn enqueue_off_the_runtime_reports_a_durably_queued_launch_as_queued() {
        let (orch, _, _) =
            lifecycle_fixture_with_gc("ordinary-off-runtime-enqueue.db", Some(roomy_gc())).await;
        let starts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted = starts.clone();
        let _backend = crate::orchestrator::session::install_orchestrator_test_session_backend(
            &orch,
            move || Box::new(CountingBackend(counted.clone())),
        );
        set_ceiling(&orch, 1);
        // The launch resolves to the real Claude capability, so its lane is
        // `claude-cli`; a resident blocker there holds the one unit.
        orch.db
            .local
            .execute_script(
                "DELETE FROM runtime_admission_requests WHERE request_id='req';
                 UPDATE turns SET run_id=NULL WHERE id='t';
                 INSERT INTO runtime_admission_requests(
                    request_id,workspace_key,resource_lane,launch_key,claim_json,priority,
                    enqueue_seq,state,lease_owner,lease_generation,lease_expires_at,created_at,updated_at
                 ) VALUES(
                    'blocker','private-runtime','claude-cli','ordinary:blocker',
                    '{\"residentProcessUnits\":1,\"logicalStreamUnits\":1}',0,
                    1,'active','ordinary-session-promoter',1,9999999999,1,1
                 );",
            )
            .await
            .unwrap();

        let enqueuer = orch.clone();
        let (result, claim_dropped) = std::thread::spawn(move || {
            assert!(tokio::runtime::Handle::try_current().is_err());
            let mut prepared = super::PreparedJob {
                job_id: "j".into(),
                project_id: "p".into(),
                run_id: "cold".into(),
                session_id: "s".into(),
                session_start: crate::backends::SessionStart::New {
                    session_id: "s".into(),
                },
                prompt: "queued hello".into(),
                job_model: None,
                agent_config: None,
                artifact_schema_info: None,
                execution_id: Some("e".into()),
                turn_id: "t".into(),
                start_reason: crate::models::TurnStartReason::FollowUp,
                origin: crate::storage::LaunchOrigin::Background,
                launch_claim: Some(enqueuer.claim_job_launch("j", "cold")),
            };
            let identity = crate::identity::UserIdentity {
                user_id: "u".into(),
                email: "u@example.com".into(),
                name: "U".into(),
                claude_auth: None,
                codex_auth: None,
                github_token: None,
            };
            let result = super::enqueue_or_start(&enqueuer, &mut prepared, Some(identity));
            (result, prepared.launch_claim.is_none())
        })
        .join()
        .unwrap();
        assert_eq!(
            result,
            Ok(false),
            "a durably queued launch is not a failure"
        );
        assert!(claim_dropped, "the durable row is the claim once it exists");
        assert_eq!(
            request_state(&orch, "ordinary-admission:cold").await.0,
            "queued"
        );
        assert_eq!(starts.load(std::sync::atomic::Ordering::SeqCst), 0);

        assert!(crate::storage::finish_admission_by_launch_key(
            &orch.db.local,
            "private-runtime",
            "ordinary:blocker",
            "ordinary-session-promoter",
            1,
            "released",
            None,
            chrono::Utc::now().timestamp(),
        )
        .await
        .unwrap());
        let promoter = orch.clone();
        std::thread::spawn(move || super::promote_private_lane(&promoter, "claude-cli"))
            .join()
            .unwrap();

        wait_until_started(&orch, "cold").await;
        assert_eq!(starts.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    async fn lifecycle_fixture_with_gc(
        name: &str,
        warm_gc: Option<Arc<crate::agent_process::gc::WarmProcessGC>>,
    ) -> (
        Orchestrator,
        crate::storage::AdmissionRequest,
        crate::storage::PreparedOrdinaryLaunch,
    ) {
        let db = Arc::new(crate::storage::migrated_test_db(name).await);
        let root = tempfile::tempdir().unwrap().keep();
        let repo = root.to_string_lossy().replace('\'', "''");
        db.execute_script(format!("\
            INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);\
            INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES('p','w','P','P','{repo}',1,1);\
            INSERT INTO issues(id,project_id,number,title,status,progress,attention,created_at,updated_at) VALUES('i','p',1,'I','active','active','none',1,1);\
            INSERT INTO executions(id,recipe_id,issue_id,project_id,status,started_at,seq) VALUES('e','r','i','p','running',1,1);\
            INSERT INTO jobs(id,execution_id,recipe_node_id,issue_id,project_id,node_name,uri_segment,status,current_session_id,created_at,updated_at) VALUES('j','e','builder','i','p','Builder','builder','running','s',1,1);\
            INSERT INTO sessions(id,job_id,backend,status,sequence,created_at,updated_at) VALUES('s','j','claude','open',1,1,1);\
            INSERT INTO runs(id,project_id,issue_id,job_id,session_id,status,start_mode,created_at,updated_at) VALUES('cold','p','i','j','s','starting','resume',1,1);\
            INSERT INTO turns(id,session_id,run_id,job_id,sequence,state,start_reason,created_at,updated_at) VALUES('t','s','cold','j',1,'pending','resume',1,1);\
            INSERT INTO events(id,run_id,turn_id,sequence,event_type,data,timestamp,created_at) VALUES('ev','cold','t',1,'user','{{\"eventType\":\"user\",\"content\":\"queued hello\",\"isError\":false}}',1,1);\
            INSERT INTO runtime_admission_requests(request_id,workspace_key,resource_lane,launch_key,claim_json,priority,enqueue_seq,state,lease_owner,lease_generation,lease_expires_at,created_at,updated_at) VALUES('req','private-runtime','lane','ordinary:cold','{{\"residentProcessUnits\":1,\"logicalStreamUnits\":1}}',0,1,'leased','ordinary-session-promoter',1,9999999999,1,1);"
        )).await.unwrap();
        let search = Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap());
        let orch = Orchestrator::builder(
            Arc::new(DbState::new(db.clone(), search)),
            Arc::new(TestServicesBuilder::new().build()),
            root.join("config"),
        )
        .boot_at(0)
        .warm_gc(warm_gc)
        .build();
        let request = crate::storage::get_admission_request(&db, "req")
            .await
            .unwrap()
            .unwrap();
        let snapshot = crate::storage::PreparedOrdinaryLaunch {
            request_id: "req".into(), job_id: "j".into(), run_id: "cold".into(), session_id: "s".into(), turn_id: "t".into(), execution_id: "e".into(), prompt: "queued hello".into(), reason: crate::models::TurnStartReason::FollowUp,
            session_start: crate::storage::PreparedSessionStart::New { session_id: "s".into() }, model: String::new(), agent_config: serde_json::Value::Null, output_schema: None,
            runtime_capability: serde_json::from_value(serde_json::json!({"class":"dedicated_process","lane":"lane","host_requirement":{"requirement":"resident_on","host_kind":"runner"},"claim":{"resident_process_units":1,"server_instance_key":null,"logical_stream_units":1,"estimated_memory_bytes":null},"supports_warm_reuse":true})).unwrap(),
            provider_account_id: None, identity_override: None, origin: crate::storage::LaunchOrigin::Background, created_at: 1,
        };
        (orch, request, snapshot)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn startup_recovers_queued_and_leased_private_work_without_duplicate_backend_start() {
        for initial_state in ["queued", "leased"] {
            let (orch, _, snapshot) =
                lifecycle_fixture(&format!("ordinary-startup-recovery-{initial_state}.db")).await;
            let starts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            // Recovery promotes on the blocking pool, so the backend must be
            // reachable from that thread, and it must keep counting so a
            // duplicate start is observed rather than hidden.
            let counted = starts.clone();
            let _backend = crate::orchestrator::session::install_orchestrator_test_session_backend(
                &orch,
                move || Box::new(CountingBackend(counted.clone())),
            );
            crate::storage::persist_prepared_ordinary_launch(&orch.db.local, snapshot)
                .await
                .unwrap();
            if initial_state == "queued" {
                orch.db.local.execute(
                    "UPDATE runtime_admission_requests SET state='queued', lease_owner=NULL, lease_expires_at=NULL WHERE request_id='req'",
                    (),
                ).await.unwrap();
            }

            super::recover_on_startup(&orch).await;
            super::recover_on_startup(&orch).await;
            super::promote_private_lane_blocking(&orch, "lane").unwrap();

            assert_eq!(
                starts.load(std::sync::atomic::Ordering::SeqCst),
                1,
                "{initial_state} startup recovery must start the backend exactly once"
            );
            assert_eq!(orch.process_state.has_process("cold"), Some(true));
        }
    }

    /// A launch this boot leased before startup recovery reaches the admission
    /// step (a watchdog successor, an early operator send) keeps its lease: its
    /// start completes, its row stays the live process's, and recovery does
    /// not hand the row to a second start.
    #[tokio::test(flavor = "current_thread")]
    async fn startup_recovery_spares_a_launch_this_boot_already_leased() {
        let (orch, _, snapshot) = lifecycle_fixture("ordinary-startup-boot-fence.db").await;
        let starts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted = starts.clone();
        let _backend = crate::orchestrator::session::install_orchestrator_test_session_backend(
            &orch,
            move || Box::new(CountingBackend(counted.clone())),
        );
        crate::storage::persist_prepared_ordinary_launch(&orch.db.local, snapshot.clone())
            .await
            .unwrap();
        orch.db.local.execute(
            "UPDATE runtime_admission_requests SET state='queued', lease_owner=NULL, lease_expires_at=NULL WHERE request_id='req'",
            (),
        ).await.unwrap();
        let owner = super::private_lease_owner(&orch);
        let now = chrono::Utc::now().timestamp();
        let lease = match crate::storage::lease_next_admission_within(
            &orch.db.local,
            "private-runtime",
            "lane",
            &owner,
            now,
            now + 300,
            crate::storage::RuntimeResourceCapacity {
                resident_process_units: u32::MAX,
                logical_stream_units: u32::MAX,
            },
            None,
            None,
            crate::storage::MemoryPriority::Interactive,
        )
        .await
        .unwrap()
        {
            crate::storage::LeaseDecision::Leased(lease) => lease,
            other => panic!("expected a lease, got {other:?}"),
        };

        // Recovery runs between this boot's lease and its start.
        super::recover_on_startup(&orch).await;
        assert_eq!(
            request_state(&orch, "req").await,
            ("leased".to_string(), None),
            "this boot's lease is not requeued"
        );
        assert!(
            super::start_leased(&orch, super::OrdinaryLease::Private(lease), snapshot).unwrap()
        );

        // Recovery after the start leaves the live process's row alone too.
        super::recover_on_startup(&orch).await;
        let request = crate::storage::get_admission_request(&orch.db.local, "req")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(request.state, "active");
        assert_eq!(request.lease_owner.as_deref(), Some(owner.as_str()));
        assert_eq!(request.lease_generation, 2);
        assert_eq!(starts.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(orch.process_state.has_process("cold"), Some(true));
        let status = text(&orch, "SELECT status FROM runs WHERE id='cold'").await;
        assert_ne!(status, "crashed");
        let replaced: i64 = orch
            .db
            .local
            .query_opt(
                "SELECT COUNT(*) FROM events WHERE data LIKE '%lease was replaced%'",
                (),
                |row| row.i64(0),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(replaced, 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn watchdog_reclaim_cannot_start_the_same_prepared_launch_twice() {
        let (orch, request, snapshot) = lifecycle_fixture("ordinary-watchdog-reclaim.db").await;
        let starts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        crate::orchestrator::session::install_test_session_backend(Box::new(CountingBackend(
            starts.clone(),
        )));

        super::start_leased(&orch, super::OrdinaryLease::Private(request), snapshot).unwrap();
        orch.db.local.execute(
            "UPDATE runtime_admission_requests SET lease_expires_at = 1 WHERE request_id = 'req'",
            (),
        ).await.unwrap();
        assert_eq!(
            crate::storage::reap_expired_admissions(&orch.db.local, 2)
                .await
                .unwrap(),
            1
        );
        super::promote_private_lane_blocking(&orch, "lane").unwrap();

        assert_eq!(starts.load(std::sync::atomic::Ordering::SeqCst), 1,
            "watchdog reclamation must not cross the backend start boundary for a consumed prepared launch");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stale_generation_terminal_callback_cannot_release_replacement_claim() {
        let (orch, _, _) = lifecycle_fixture("ordinary-stale-terminal.db").await;
        orch.db.local.execute(
            "UPDATE runtime_admission_requests SET state = 'active', lease_owner = 'replacement', lease_generation = 2, lease_expires_at = 9999999999 WHERE request_id = 'req'",
            (),
        ).await.unwrap();

        assert!(!crate::storage::finish_admission_by_launch_key(
            &orch.db.local,
            "private-runtime",
            "ordinary:cold",
            "ordinary-session-promoter",
            1,
            "released",
            None,
            3,
        )
        .await
        .unwrap());
        let request = crate::storage::get_admission_request(&orch.db.local, "req")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (
                request.state.as_str(),
                request.lease_owner.as_deref(),
                request.lease_generation
            ),
            ("active", Some("replacement"), 2)
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn warm_eviction_releases_the_dedicated_process_claim_exactly_once() {
        let (orch, _, _) = lifecycle_fixture("ordinary-warm-eviction.db").await;
        orch.db
            .local
            .execute(
                "UPDATE runtime_admission_requests SET state = 'active' WHERE request_id = 'req'",
                (),
            )
            .await
            .unwrap();
        orch.process_state.prepare_admission_claim(
            "cold".into(),
            ProcessAdmissionClaim {
                request_id: "req".into(),
                workspace_key: "private-runtime".into(),
                launch_key: "ordinary:cold".into(),
                owner: "ordinary-session-promoter".into(),
                generation: 1,
                team_workspace_id: None,
            },
        );
        orch.process_state
            .register_process(
                "cold".into(),
                crate::agent_process::process::RunHandle::test_handle(Some("s"), Some("j")),
            )
            .unwrap();
        orch.process_state.transition_to_warm("cold");

        assert!(crate::orchestrator::lifecycle::release_and_stop_process(
            &orch, "cold"
        ));
        assert!(
            !crate::orchestrator::lifecycle::release_and_stop_process(&orch, "cold"),
            "a repeated eviction callback must find neither a process nor a claim"
        );
        assert_eq!(orch.process_state.take_admission_claim("cold"), None);
        let request = crate::storage::get_admission_request(&orch.db.local, "req")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(request.state, "released");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn process_death_retains_claim_until_release_retry_succeeds_and_fences_stale_callback() {
        let (orch, _, _) = lifecycle_fixture("ordinary-death-release-retry.db").await;
        orch.db
            .local
            .execute(
                "UPDATE runtime_admission_requests SET state = 'active' WHERE request_id = 'req'",
                (),
            )
            .await
            .unwrap();
        orch.process_state.prepare_admission_claim(
            "cold".into(),
            ProcessAdmissionClaim {
                request_id: "req".into(),
                workspace_key: "private-runtime".into(),
                launch_key: "ordinary:cold".into(),
                owner: "ordinary-session-promoter".into(),
                generation: 1,
                team_workspace_id: None,
            },
        );
        orch.process_state
            .register_process(
                "cold".into(),
                crate::agent_process::process::RunHandle::test_handle(Some("s"), Some("j")),
            )
            .unwrap();
        orch.db
            .local
            .exclusive(|conn| {
                Box::pin(async move {
                    conn.execute(
                        "CREATE TRIGGER fail_process_release BEFORE UPDATE OF state ON runtime_admission_requests
                         WHEN NEW.state = 'released' BEGIN SELECT RAISE(ABORT, 'injected release failure'); END;",
                        (),
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
            .unwrap();

        crate::orchestrator::lifecycle::finalize_run(
            &orch,
            "cold",
            crate::models::RunStatus::Crashed,
        );
        assert!(!crate::orchestrator::lifecycle::release_and_stop_process(
            &orch, "cold"
        ));
        assert!(
            orch.process_state.admission_claim("cold").is_some(),
            "a transient release failure must retain the dead handle's retry carrier"
        );

        orch.db
            .local
            .exclusive(|conn| {
                Box::pin(async move {
                    conn.execute("DROP TRIGGER fail_process_release", ())
                        .await?;
                    Ok(())
                })
            })
            .await
            .unwrap();
        assert!(crate::orchestrator::lifecycle::release_and_stop_process(
            &orch, "cold"
        ));
        assert!(orch.process_state.admission_claim("cold").is_none());
        let released = crate::storage::get_admission_request(&orch.db.local, "req")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(released.state, "released");

        orch.db.local.execute(
            "UPDATE runtime_admission_requests SET state='active', lease_owner='replacement', lease_generation=2 WHERE request_id='req'",
            (),
        ).await.unwrap();
        assert!(!crate::orchestrator::lifecycle::release_and_stop_process(
            &orch, "cold"
        ));
        let replacement = crate::storage::get_admission_request(&orch.db.local, "req")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (
                replacement.state.as_str(),
                replacement.lease_owner.as_deref(),
                replacement.lease_generation
            ),
            ("active", Some("replacement"), 2)
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn promoted_cold_releases_its_lease_and_delivers_to_the_warm_process_without_starting_backend(
    ) {
        let (orch, request, snapshot) = lifecycle_fixture("ordinary-warm-race.db").await;
        orch.db.local.execute("INSERT INTO runs(id,project_id,issue_id,job_id,session_id,status,created_at,updated_at) VALUES('warm','p','i','j','s','live',1,1)", ()).await.unwrap();
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let stdin = Arc::new(Mutex::new(Some(
            cairn_models::session::claude::ClaudeControl::boxed(Box::new(CapturingWriter(
                bytes.clone(),
            ))),
        )));
        let handle = crate::agent_process::process::RunHandle::new(
            Arc::new(Mutex::new(None)),
            stdin,
            Some("s".into()),
            Some("j".into()),
        );
        orch.process_state
            .register_process("warm".into(), handle)
            .unwrap();
        orch.process_state.transition_to_warm("warm");
        let starts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        crate::orchestrator::session::install_test_session_backend(Box::new(CountingBackend(
            starts.clone(),
        )));

        super::start_leased(&orch, super::OrdinaryLease::Private(request), snapshot).unwrap();

        assert_eq!(starts.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert!(String::from_utf8(bytes.lock().unwrap().clone())
            .unwrap()
            .contains("queued hello"));
        let state = orch
            .db
            .local
            .query_opt(
                "SELECT state FROM runtime_admission_requests WHERE request_id='req'",
                (),
                |row| row.text(0),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            state, "cancelled",
            "the unused cold lease must be generation-fenced and released"
        );
        // No process ever ran for the cold run, so nothing crashed.
        assert_eq!(
            text(
                &orch,
                "SELECT status || ':' || COALESCE(exit_reason, '') FROM runs WHERE id='cold'"
            )
            .await,
            "exited:warm_race_reused"
        );
    }

    /// cairn/5050: an automatic retry reserves its turn `running` on its launch
    /// run before admission, so a launch withdrawn before any process settles
    /// that reserved turn too instead of leaving it running on an exited run.
    #[tokio::test(flavor = "current_thread")]
    async fn a_withdrawn_retry_launch_settles_the_turn_it_reserved() {
        let (orch, request, snapshot) = lifecycle_fixture("ordinary-withdrawn-retry.db").await;
        orch.db
            .local
            .execute(
                "UPDATE turns SET state='running', start_reason='retry', started_at=1 WHERE id='t'",
                (),
            )
            .await
            .unwrap();
        orch.process_state
            .register_process(
                "existing".into(),
                crate::agent_process::process::RunHandle::test_handle(Some("s"), Some("j")),
            )
            .unwrap();
        orch.process_state.begin_turn("existing", "existing-turn");
        let starts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        crate::orchestrator::session::install_test_session_backend(Box::new(CountingBackend(
            starts.clone(),
        )));

        assert!(
            !super::start_leased(&orch, super::OrdinaryLease::Private(request), snapshot).unwrap()
        );

        assert_eq!(starts.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(
            request_state(&orch, "req").await,
            (
                "cancelled".to_string(),
                Some("same-session process is not warm and idle".to_string())
            )
        );
        assert_eq!(
            text(
                &orch,
                "SELECT state || ':' || COALESCE(end_reason, '') FROM turns WHERE id='t'"
            )
            .await,
            format!("cancelled:{}", crate::models::TurnEndReason::LaunchRefused)
        );
        assert_eq!(
            text(&orch, "SELECT status FROM runs WHERE id='cold'").await,
            "exited"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn promoted_cold_never_writes_to_a_non_idle_same_session_process() {
        for (name, occupancy) in [
            ("starting", 0_u8),
            ("active", 1_u8),
            ("awaiting-host", 2_u8),
        ] {
            let (orch, request, snapshot) =
                lifecycle_fixture(&format!("ordinary-non-warm-race-{name}.db")).await;
            let bytes = Arc::new(Mutex::new(Vec::new()));
            let handle = crate::agent_process::process::RunHandle::new(
                Arc::new(Mutex::new(None)),
                Arc::new(Mutex::new(Some(
                    cairn_models::session::claude::ClaudeControl::boxed(Box::new(CapturingWriter(
                        bytes.clone(),
                    ))),
                ))),
                Some("s".into()),
                Some("j".into()),
            );
            orch.process_state
                .register_process("existing".into(), handle)
                .unwrap();
            if occupancy > 0 {
                orch.process_state.begin_turn("existing", "existing-turn");
            }
            if occupancy == 2 {
                orch.process_state
                    .yield_for_host("existing", "existing-turn");
            }
            let starts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            crate::orchestrator::session::install_test_session_backend(Box::new(CountingBackend(
                starts.clone(),
            )));

            assert!(
                !super::start_leased(&orch, super::OrdinaryLease::Private(request), snapshot)
                    .unwrap()
            );
            assert_eq!(starts.load(std::sync::atomic::Ordering::SeqCst), 0);
            assert!(
                bytes.lock().unwrap().is_empty(),
                "{name} process received concurrent stdin"
            );
            assert_eq!(
                crate::storage::get_admission_request(&orch.db.local, "req")
                    .await
                    .unwrap()
                    .unwrap()
                    .state,
                "cancelled"
            );
        }
    }

    /// The run, turn, wake and job a withdrawn launch must leave: the run
    /// settled without a crash, its turn cancelled rather than failed, the wake
    /// back in the queue, nothing left for the stale-run reaper to invent a
    /// crash from, and a job that runs on `rival` (the run that superseded the
    /// launch) and rests resumable once `rival` ends.
    async fn assert_withdrawn_without_a_crash(orch: &Orchestrator, detail: &str, rival: &str) {
        assert_eq!(
            request_state(orch, "req").await,
            ("cancelled".to_string(), Some(detail.to_string()))
        );
        assert_eq!(
            text(
                orch,
                "SELECT status || ':' || COALESCE(exit_reason, '') FROM runs WHERE id='cold'"
            )
            .await,
            format!("exited:{}", super::LAUNCH_SUPERSEDED_EXIT_REASON),
            "the run is settled the moment the lease closes, not left to the reaper"
        );
        assert_eq!(
            text(
                orch,
                "SELECT state || ':' || COALESCE(end_reason, '') FROM turns WHERE id='t'"
            )
            .await,
            "cancelled:launch_refused",
            "a superseded launch's turn is not failed work"
        );
        assert!(delivery_returned(orch, "j", "t").await);
        // The run was created at t=1, far past the grace; were it still
        // `starting`, this is the sweep that would have crashed it.
        assert_eq!(crate::runs::reap::reap_stale_runs(orch), 0);
        assert_eq!(
            text(orch, "SELECT status FROM runs WHERE id='cold'").await,
            "exited"
        );
        // The projection reads the job's node from its execution's recipe; a
        // shipping recipe keeps the builder off the long-running (Idle) path.
        let snapshot = serde_json::json!({
            "recipe": {
                "id": "r", "name": "R", "description": null, "trigger": "manual",
                "nodes": [
                    {"id": "builder", "nodeType": "agent", "name": "Builder", "position": {"x": 0.0, "y": 0.0}},
                    {"id": "pr", "nodeType": "pr", "name": "PR", "position": {"x": 1.0, "y": 0.0}}
                ],
                "edges": []
            },
            "agents": {}, "skills": {},
            "triggerContext": {"issueId": "i", "projectId": "p", "triggerType": "manual"},
            "delegatedPackets": [], "createdAt": 1
        })
        .to_string();
        orch.db
            .local
            .execute("UPDATE executions SET snapshot=?1 WHERE id='e'", [snapshot])
            .await
            .unwrap();
        let derived = || async {
            orch.db
                .local
                .read(|conn| {
                    Box::pin(async move {
                        crate::execution::advancement::recompute_job_status_conn(conn, "j").await
                    })
                })
                .await
                .unwrap()
        };
        assert_eq!(
            derived().await,
            crate::models::JobStatus::Running,
            "the superseding run is the job's live work"
        );
        orch.db
            .local
            .execute(
                "UPDATE runs SET status='exited', exited_at=2, updated_at=2 WHERE id=?1",
                [rival],
            )
            .await
            .unwrap();
        assert_eq!(
            derived().await,
            crate::models::JobStatus::Pending,
            "with its live run gone the job rests resumable; a failed turn would read failed"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn session_fence_withdraws_closed_and_noncurrent_cold_and_warm_launches() {
        for warm in [false, true] {
            for closed in [false, true] {
                let (orch, request, snapshot) =
                    lifecycle_fixture(&format!("ordinary-session-fence-{warm}-{closed}.db")).await;
                brief(&orch, "cold", "t", "j").await;
                let bytes = Arc::new(Mutex::new(Vec::new()));
                if warm {
                    orch.db.local.execute(
                        "INSERT INTO runs(id,project_id,issue_id,job_id,session_id,status,created_at,updated_at) VALUES('warm','p','i','j','s','live',1,1)", (),
                    ).await.unwrap();
                    let handle = crate::agent_process::process::RunHandle::new(
                        Arc::new(Mutex::new(None)),
                        Arc::new(Mutex::new(Some(
                            cairn_models::session::claude::ClaudeControl::boxed(Box::new(
                                CapturingWriter(bytes.clone()),
                            )),
                        ))),
                        Some("s".into()),
                        Some("j".into()),
                    );
                    orch.process_state
                        .register_process("warm".into(), handle)
                        .unwrap();
                    orch.process_state.transition_to_warm("warm");
                }
                if closed {
                    orch.db
                        .local
                        .execute(
                            "UPDATE sessions SET status='closed',closed_at=2 WHERE id='s'",
                            (),
                        )
                        .await
                        .unwrap();
                } else {
                    // Even an open predecessor cannot receive work after the job
                    // moves to a successor.
                    orch.db.local.execute_script(
                        "INSERT INTO sessions(id,job_id,backend,status,sequence,created_at,updated_at) VALUES('successor','j','claude','open',2,2,2);
                         UPDATE jobs SET current_session_id='successor' WHERE id='j';",
                    ).await.unwrap();
                }
                let starts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
                let counted = starts.clone();
                let _backend =
                    crate::orchestrator::session::install_orchestrator_test_session_backend(
                        &orch,
                        move || Box::new(CountingBackend(counted.clone())),
                    );
                assert!(!super::start_leased(
                    &orch,
                    super::OrdinaryLease::Private(request),
                    snapshot,
                )
                .unwrap());
                assert_eq!(starts.load(std::sync::atomic::Ordering::SeqCst), 0);
                assert!(
                    bytes.lock().unwrap().is_empty(),
                    "closed or stale warm process received input"
                );
                assert!(!orch.process_state.has_process("cold").unwrap_or(false));
                assert_eq!(
                    request_state(&orch, "req").await,
                    (
                        "cancelled".into(),
                        Some("session closed before launch".into())
                    )
                );
                assert_eq!(
                    text(
                        &orch,
                        "SELECT status || ':' || exit_reason FROM runs WHERE id='cold'"
                    )
                    .await,
                    "exited:launch_superseded"
                );
                assert_eq!(
                    text(
                        &orch,
                        "SELECT state || ':' || end_reason FROM turns WHERE id='t'"
                    )
                    .await,
                    "cancelled:launch_refused"
                );
                assert!(delivery_returned(&orch, "j", "t").await);
                if warm {
                    assert!(
                        orch.process_state.is_warm("warm"),
                        "fence must not consume the existing process"
                    );
                }
            }
        }
    }

    /// A promoted launch whose session's process is busy is withdrawn in favour
    /// of that process, which serves the returned wake at its next turn
    /// boundary. The launch's run must end with the withdrawal: left `starting`
    /// with no process and no cover, the reaper would settle it as a crash.
    #[tokio::test(flavor = "current_thread")]
    async fn a_launch_superseded_by_a_busy_same_session_process_settles_its_run_without_a_crash() {
        let (orch, request, snapshot) =
            lifecycle_fixture("ordinary-superseded-by-busy-process.db").await;
        brief(&orch, "cold", "t", "j").await;
        orch.db.local.execute("INSERT INTO runs(id,project_id,issue_id,job_id,session_id,status,started_at,created_at,updated_at) VALUES('existing','p','i','j','s','live',1,1,1)", ()).await.unwrap();
        orch.process_state
            .register_process(
                "existing".into(),
                crate::agent_process::process::RunHandle::test_handle(Some("s"), Some("j")),
            )
            .unwrap();
        orch.process_state.begin_turn("existing", "existing-turn");
        let starts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        crate::orchestrator::session::install_test_session_backend(Box::new(CountingBackend(
            starts.clone(),
        )));

        assert!(
            !super::start_leased(&orch, super::OrdinaryLease::Private(request), snapshot).unwrap()
        );

        assert_eq!(starts.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_withdrawn_without_a_crash(
            &orch,
            "same-session process is not warm and idle",
            "existing",
        )
        .await;
    }

    /// The same withdrawal when another run of the job holds the launch claim.
    #[tokio::test(flavor = "current_thread")]
    async fn a_launch_superseded_by_a_competing_launch_settles_its_run_without_a_crash() {
        let (orch, request, snapshot) =
            lifecycle_fixture("ordinary-superseded-by-competing-launch.db").await;
        brief(&orch, "cold", "t", "j").await;
        orch.db.local.execute("INSERT INTO runs(id,project_id,issue_id,job_id,session_id,status,created_at,updated_at) VALUES('rival','p','i','j','s','starting',1,1)", ()).await.unwrap();
        let _rival = orch.claim_job_launch("j", "rival");
        let starts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        crate::orchestrator::session::install_test_session_backend(Box::new(CountingBackend(
            starts.clone(),
        )));

        assert!(
            !super::start_leased(&orch, super::OrdinaryLease::Private(request), snapshot).unwrap()
        );

        assert_eq!(starts.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_withdrawn_without_a_crash(
            &orch,
            "session already has a process or competing launch",
            "rival",
        )
        .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn restoration_failure_terminalizes_the_leased_generation_run_and_job() {
        let (orch, request, mut snapshot) =
            lifecycle_fixture("ordinary-restoration-failure.db").await;
        snapshot.agent_config = serde_json::Value::String("not an agent config".into());

        let error = super::start_leased(&orch, super::OrdinaryLease::Private(request), snapshot)
            .unwrap_err();
        assert!(error.contains("restore ordinary agent config"));
        let request = crate::storage::get_admission_request(&orch.db.local, "req")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(request.state, "failed");
        assert_eq!(request.lease_generation, 1);
        assert_eq!(request.lease_owner.as_deref(), Some(super::OWNER));

        // Enqueue already reported success upstream, so the promoter itself must
        // leave no run stuck `starting` and no job stuck `running`.
        let run = orch
            .db
            .local
            .query_opt(
                "SELECT status, exit_reason FROM runs WHERE id='cold'",
                (),
                |row| Ok((row.text(0)?, row.opt_text(1)?)),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (run.0.as_str(), run.1.as_deref()),
            ("crashed", Some("session_start_failed"))
        );
        // The job's failure is recorded as a failed turn fact; the execution
        // projection derives the job's `failed` status from it.
        let turn_state = orch
            .db
            .local
            .query_opt("SELECT state FROM turns WHERE id='t'", (), |row| {
                row.text(0)
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(turn_state, "failed");
    }

    /// Registers like [`CountingBackend`] and, as Claude's reader does, tells the
    /// boot ramp that its process will report first output.
    struct ClaudeLikeBackend(Arc<std::sync::atomic::AtomicUsize>, std::time::Duration);

    impl AgentBackend for ClaudeLikeBackend {
        fn name(&self) -> &str {
            "Claude"
        }
        fn is_available(&self) -> Result<(), String> {
            Ok(())
        }
        fn discover_models(&self) -> Result<Vec<DiscoveredModel>, String> {
            Ok(Vec::new())
        }
        fn serves(
            &self,
            _: &crate::orchestrator::Orchestrator,
            _: Option<&str>,
        ) -> Vec<cairn_models::types::catalog::ServedVerb> {
            Vec::new()
        }
        fn resolve_tools(&self, _: &[String], _: &[String]) -> ResolvedTools {
            ResolvedTools {
                allowed: Vec::new(),
                disallowed: Vec::new(),
            }
        }
        fn start_session(
            &self,
            config: SessionConfig,
            orch: &Orchestrator,
            _admitted: crate::backends::Admitted,
        ) -> Result<(), String> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            std::thread::sleep(self.1);
            let run_id = config.run_id;
            crate::execution::jobs::spawn_reports_first_output(orch, &run_id);
            orch.process_state
                .register_process(
                    run_id.clone(),
                    crate::agent_process::process::RunHandle::test_handle(
                        Some(config.session_start.session_id()),
                        None,
                    ),
                )
                .map_err(|error| error.to_string())?;
            Ok(())
        }
        fn supports_resume(&self) -> bool {
            true
        }
        fn supports_warm_processes(&self) -> bool {
            true
        }
    }

    fn install_claude_like(
        orch: &Orchestrator,
    ) -> (Arc<std::sync::atomic::AtomicUsize>, impl Sized) {
        install_slow_claude_like(orch, std::time::Duration::ZERO)
    }

    /// [`install_claude_like`] with each start taking `delay`, so concurrent
    /// promotion passes overlap.
    fn install_slow_claude_like(
        orch: &Orchestrator,
        delay: std::time::Duration,
    ) -> (Arc<std::sync::atomic::AtomicUsize>, impl Sized) {
        let starts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted = starts.clone();
        let backend = crate::orchestrator::session::install_orchestrator_test_session_backend(
            orch,
            move || Box::new(ClaudeLikeBackend(counted.clone(), delay)),
        );
        (starts, backend)
    }

    /// A restart's enqueue burst fires one promotion pass per launch. However
    /// those passes overlap, they share one window: no first output arrives
    /// here, so exactly four background spawns may ever start.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_promotion_passes_never_lease_past_the_boot_ramp_window() {
        let orch = ramp_fixture("boot-ramp-concurrent.db", 10).await;
        let (starts, _backend) =
            install_slow_claude_like(&orch, std::time::Duration::from_millis(50));
        let passes: Vec<_> = (0..10)
            .map(|_| {
                let orch = orch.clone();
                tokio::task::spawn_blocking(move || {
                    super::promote_private_lane_blocking(&orch, "lane")
                })
            })
            .collect();
        for pass in passes {
            pass.await.unwrap().unwrap();
        }
        assert_eq!(started(&starts), 4);
        assert_eq!(orch.private_lanes.warming_runs().len(), 4);
        assert_eq!(
            text(
                &orch,
                "SELECT CAST(COUNT(*) AS TEXT) FROM runtime_admission_requests WHERE state IN ('leased','starting','active')"
            )
            .await,
            "4"
        );
    }

    /// cairn/4758: launches that reach admission while startup recovery runs
    /// (startup's own promotion pass and event-driven passes alike) stay durably
    /// queued, say why at every queue position, survive the stale-run reaper,
    /// and each start exactly once when readiness opens.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn startup_recovery_holds_every_queued_launch_until_readiness_then_starts_each_once() {
        let (mut orch, _, snapshot) = lifecycle_fixture("startup-readiness-hold.db").await;
        orch.startup_readiness = crate::orchestrator::StartupReadiness::pending();
        orch.db
            .local
            .execute(
                "DELETE FROM runtime_admission_requests WHERE request_id='req'",
                (),
            )
            .await
            .unwrap();
        for n in 1..=2 {
            queue_launch(&orch, &snapshot, n, "starting").await;
        }
        let (starts, _backend) = install_claude_like(&orch);

        super::recover_on_startup(&orch).await;
        let pass = orch.clone();
        tokio::task::spawn_blocking(move || super::promote_private_lane_blocking(&pass, "lane"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(started(&starts), 0, "nothing starts before readiness");
        for n in 1..=2 {
            assert_eq!(request_state(&orch, &format!("req{n}")).await.0, "queued");
            assert_eq!(
                text(&orch, &format!("SELECT state FROM turns WHERE id='t{n}'")).await,
                "pending"
            );
        }
        let reaper = orch.clone();
        tokio::task::spawn_blocking(move || crate::runs::reap::reap_stale_runs(&reaper))
            .await
            .unwrap();
        for n in 1..=2 {
            assert_eq!(
                text(&orch, &format!("SELECT status FROM runs WHERE id='r{n}'")).await,
                "starting",
                "a held launch is a launch in flight, not a run to settle"
            );
        }
        let behind_the_head = crate::execution::jobs::describe_job_launch(&orch, "j2")
            .await
            .expect("a queued launch renders its wait");
        assert!(
            behind_the_head.contains("position 2 of 2")
                && behind_the_head.contains("still finishing startup recovery"),
            "{behind_the_head}"
        );

        orch.mark_startup_recovery_ready();
        for _ in 0..500 {
            if started(&starts) == 2 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(started(&starts), 2, "readiness drains the held queue");
        let pass = orch.clone();
        tokio::task::spawn_blocking(move || super::promote_private_lane_blocking(&pass, "lane"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(started(&starts), 2, "each held launch starts once");
    }

    /// The fixture's job `j` and run `cold` with no admission row, before
    /// readiness, with `session_backend` recorded on session `s`, a counting
    /// session backend installed, and an attention delivery on turn `t`.
    async fn held_launch_fixture(
        name: &str,
        session_backend: &str,
    ) -> (
        Orchestrator,
        Arc<std::sync::atomic::AtomicUsize>,
        impl Sized,
    ) {
        let (mut orch, _, _) = lifecycle_fixture(name).await;
        orch.startup_readiness = crate::orchestrator::StartupReadiness::pending();
        orch.db
            .local
            .execute_script(format!(
                "DELETE FROM runtime_admission_requests WHERE request_id='req';
                 UPDATE sessions SET backend='{session_backend}' WHERE id='s';"
            ))
            .await
            .unwrap();
        let starts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted = starts.clone();
        let backend = crate::orchestrator::session::install_orchestrator_test_session_backend(
            &orch,
            move || Box::new(CountingBackend(counted.clone())),
        );
        brief(&orch, "cold", "t", "j").await;
        (orch, starts, backend)
    }

    fn stateless_prepared(orch: &Orchestrator) -> super::PreparedJob {
        super::PreparedJob {
            job_id: "j".into(),
            project_id: "p".into(),
            run_id: "cold".into(),
            session_id: "s".into(),
            session_start: crate::backends::SessionStart::New {
                session_id: "s".into(),
            },
            prompt: "queued hello".into(),
            // A slash-qualified model resolves to OpenRouter, a stateless HTTP
            // backend.
            job_model: Some(crate::models::Model::from("openrouter/auto")),
            agent_config: None,
            artifact_schema_info: None,
            execution_id: Some("e".into()),
            turn_id: "t".into(),
            start_reason: crate::models::TurnStartReason::FollowUp,
            origin: crate::storage::LaunchOrigin::Background,
            launch_claim: Some(orch.claim_job_launch("j", "cold")),
        }
    }

    /// Defer an OpenRouter launch of the fixture's run `cold` before readiness,
    /// asserting that it holds its launch claim, that the stale-run reaper
    /// spares it, and that nothing has started.
    async fn defer_stateless_launch(orch: &Orchestrator, starts: &std::sync::atomic::AtomicUsize) {
        let mut prepared = stateless_prepared(orch);
        assert!(
            !super::enqueue_or_start(orch, &mut prepared, None).unwrap(),
            "a deferred launch reports that no process holds its prompt yet"
        );
        drop(prepared);
        assert_held_before_readiness(orch, starts).await;
    }

    /// A launch with no durable row is held by its in-memory claim alone: the
    /// claim is taken, the stale-run reaper spares the run, nothing started.
    async fn assert_held_before_readiness(
        orch: &Orchestrator,
        starts: &std::sync::atomic::AtomicUsize,
    ) {
        assert_eq!(
            orch.claimed_launch_run("j").as_deref(),
            Some("cold"),
            "the deferral keeps the launch claim"
        );
        let reaper = orch.clone();
        tokio::task::spawn_blocking(move || crate::runs::reap::reap_stale_runs(&reaper))
            .await
            .unwrap();
        assert_eq!(
            text(orch, "SELECT status FROM runs WHERE id='cold'").await,
            "starting",
            "the reaper spares a claimed launch"
        );
        assert_eq!(started(starts), 0, "nothing starts before readiness");
    }

    /// Open readiness and wait until the deferred launch registers a process
    /// or settles the withdrawn run and turn, then releases its claim.
    async fn open_readiness_and_await_release(orch: &Orchestrator) {
        orch.mark_startup_recovery_ready();
        for _ in 0..3000 {
            if orch.claimed_launch_run("j").is_none() {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("the deferral never released its claim after readiness");
    }

    async fn invalidate_stateless_session(orch: &Orchestrator, noncurrent: bool) {
        let sql = if noncurrent {
            "INSERT INTO sessions(id,job_id,backend,status,sequence,created_at,updated_at)
               VALUES('successor','j','openrouter','open',2,2,2);
             UPDATE jobs SET current_session_id='successor' WHERE id='j';"
        } else {
            "UPDATE sessions SET status='closed', terminal_reason='rotated_cache_expiry',
               closed_at=2 WHERE id='s';"
        };
        orch.db.local.execute_script(sql).await.unwrap();
    }

    async fn assert_stateless_withdrawn(
        orch: &Orchestrator,
        starts: &std::sync::atomic::AtomicUsize,
    ) {
        assert_eq!(started(starts), 0, "no provider received the prompt");
        assert_eq!(orch.process_state.has_process("cold"), Some(false));
        assert_eq!(
            text(orch, "SELECT status FROM runs WHERE id='cold'").await,
            "exited"
        );
        assert_eq!(
            text(orch, "SELECT exit_reason FROM runs WHERE id='cold'").await,
            super::LAUNCH_SUPERSEDED_EXIT_REASON
        );
        assert_eq!(
            text(orch, "SELECT state FROM turns WHERE id='t'").await,
            "cancelled"
        );
        assert_eq!(
            text(orch, "SELECT end_reason FROM turns WHERE id='t'").await,
            crate::models::TurnEndReason::LaunchRefused.to_string()
        );
        assert!(delivery_returned(orch, "j", "t").await);
        assert_eq!(
            text(
                orch,
                "SELECT CAST(COUNT(*) AS TEXT) FROM runtime_admission_requests"
            )
            .await,
            "0",
            "stateless withdrawal never creates an admission row"
        );
        assert!(orch.claimed_launch_run("j").is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn direct_stateless_session_fence_withdraws_closed_and_noncurrent_launches() {
        for noncurrent in [false, true] {
            for claimed in [false, true] {
                let (orch, starts, _backend) = held_launch_fixture(
                    &format!("direct-stateless-fence-{noncurrent}-{claimed}.db"),
                    "openrouter",
                )
                .await;
                // Initial starts can reach this seam with an unbound pending
                // turn; withdrawal must still cancel that turn, not strand it.
                orch.db
                    .local
                    .execute("UPDATE turns SET run_id=NULL WHERE id='t'", ())
                    .await
                    .unwrap();
                invalidate_stateless_session(&orch, noncurrent).await;
                orch.mark_startup_recovery_ready();
                let mut prepared = stateless_prepared(&orch);
                if !claimed {
                    drop(prepared.launch_claim.take());
                }
                assert!(!super::enqueue_or_start(&orch, &mut prepared, None).unwrap());
                assert!(
                    prepared.launch_claim.is_none(),
                    "withdrawal relinquishes its own claim before recomputing, even while preparation lives"
                );
                assert_stateless_withdrawn(&orch, &starts).await;
                assert_eq!(
                    text(&orch, "SELECT run_id FROM turns WHERE id='t'").await,
                    "cold"
                );
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn deferred_stateless_session_fence_rechecks_closed_and_noncurrent_after_readiness() {
        for noncurrent in [false, true] {
            let (orch, starts, _backend) = held_launch_fixture(
                &format!("deferred-stateless-fence-{noncurrent}.db"),
                "openrouter",
            )
            .await;
            defer_stateless_launch(&orch, &starts).await;
            invalidate_stateless_session(&orch, noncurrent).await;

            open_readiness_and_await_release(&orch).await;
            assert_stateless_withdrawn(&orch, &starts).await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stateless_session_fence_fails_closed_when_durable_session_facts_are_missing() {
        for deferred in [false, true] {
            let (orch, starts, _backend) = held_launch_fixture(
                &format!("stateless-missing-session-facts-{deferred}.db"),
                "openrouter",
            )
            .await;
            if deferred {
                defer_stateless_launch(&orch, &starts).await;
            }
            orch.db
                .local
                .execute("UPDATE runs SET session_id=NULL WHERE id='cold'", ())
                .await
                .unwrap();

            if deferred {
                open_readiness_and_await_release(&orch).await;
            } else {
                orch.mark_startup_recovery_ready();
                let mut prepared = stateless_prepared(&orch);
                assert!(!super::enqueue_or_start(&orch, &mut prepared, None).unwrap());
            }
            assert_stateless_withdrawn(&orch, &starts).await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stateless_session_fence_refuses_a_settled_run_or_turn() {
        for deferred in [false, true] {
            for settled_run in [false, true] {
                let (orch, starts, _backend) = held_launch_fixture(
                    &format!("stateless-settled-fence-{deferred}-{settled_run}.db"),
                    "openrouter",
                )
                .await;
                if deferred {
                    defer_stateless_launch(&orch, &starts).await;
                }
                let sql = if settled_run {
                    "UPDATE runs SET status='exited' WHERE id='cold'"
                } else {
                    "UPDATE turns SET state='cancelled',end_reason='launch_refused' WHERE id='t'"
                };
                orch.db.local.execute(sql, ()).await.unwrap();

                if deferred {
                    open_readiness_and_await_release(&orch).await;
                } else {
                    orch.mark_startup_recovery_ready();
                    let mut prepared = stateless_prepared(&orch);
                    assert!(!super::enqueue_or_start(&orch, &mut prepared, None).unwrap());
                    assert!(prepared.launch_claim.is_none());
                }
                assert_eq!(started(&starts), 0);
                assert_eq!(orch.process_state.has_process("cold"), Some(false));
                assert!(delivery_returned(&orch, "j", "t").await);
                assert_eq!(
                    text(&orch, "SELECT status FROM runs WHERE id='cold'").await,
                    "exited"
                );
                assert_eq!(
                    text(
                        &orch,
                        "SELECT CAST(COUNT(*) AS TEXT) FROM runtime_admission_requests"
                    )
                    .await,
                    "0"
                );
            }
        }
    }

    /// cairn/4758: a stateless launch has no durable admission row, so before
    /// readiness it waits in the background holding its launch claim, which the
    /// stale-run reaper reads as a launch in flight. Readiness starts it once. A
    /// run settled while it waited starts nothing and returns the attention
    /// delivery its turn carried.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_stateless_launch_before_readiness_waits_holding_its_claim_then_starts_once() {
        for settle_while_held in [false, true] {
            // The session must already record the provider the launch spawns:
            // `start_agent_session` refuses to start under a session row that
            // names another one.
            let (orch, starts, _backend) = held_launch_fixture(
                &format!("stateless-deferral-{settle_while_held}.db"),
                "openrouter",
            )
            .await;
            defer_stateless_launch(&orch, &starts).await;
            if settle_while_held {
                orch.db
                    .local
                    .execute("UPDATE runs SET status='crashed' WHERE id='cold'", ())
                    .await
                    .unwrap();
            }

            open_readiness_and_await_release(&orch).await;
            if settle_while_held {
                assert_eq!(started(&starts), 0, "a settled run is not started");
                assert!(delivery_returned(&orch, "j", "t").await);
            } else {
                assert_eq!(started(&starts), 1, "readiness starts the launch once");
                assert_eq!(orch.process_state.has_process("cold"), Some(true));
                assert!(!delivery_returned(&orch, "j", "t").await);
            }
        }
    }

    /// cairn/5700: a deferred stateless start that fails after readiness settles
    /// the launch as a failed promoted start does (the run crashed as
    /// `session_start_failed`, its turn failed, the attention delivery
    /// returned) before it releases its claim. Nothing is lost silently.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_deferred_stateless_start_that_fails_settles_its_run_and_returns_its_delivery() {
        // The session still records Claude, so the authoritative-backend guard
        // in `start_agent_session` refuses the OpenRouter start.
        let (orch, starts, _backend) =
            held_launch_fixture("stateless-deferral-start-fails.db", "claude").await;
        defer_stateless_launch(&orch, &starts).await;

        open_readiness_and_await_release(&orch).await;
        assert_eq!(started(&starts), 0, "the refused start reached no backend");
        let run = orch
            .db
            .local
            .query_opt(
                "SELECT status, exit_reason, error_message FROM runs WHERE id='cold'",
                (),
                |row| Ok((row.text(0)?, row.opt_text(1)?, row.opt_text(2)?)),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (run.0.as_str(), run.1.as_deref()),
            ("crashed", Some("session_start_failed"))
        );
        assert!(
            run.2
                .as_deref()
                .is_some_and(|error| error.contains("authoritative backend")),
            "the run carries the start's own error: {:?}",
            run.2
        );
        assert_eq!(
            text(&orch, "SELECT state FROM turns WHERE id='t'").await,
            "failed"
        );
        assert!(delivery_returned(&orch, "j", "t").await);
        assert_eq!(
            orch.process_state.has_process("cold"),
            Some(false),
            "no process is left registered"
        );
    }

    /// cairn/4758 review: a call with no durable admission row (a stateless
    /// HTTP call, or a Codex call whose app-server pool key is not yet known)
    /// waits for readiness in the background holding its launch claim, which the
    /// stale-run reaper reads as a launch in flight, and starts once after.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_keyless_call_before_readiness_waits_holding_its_claim_then_starts_once() {
        for (model, backend) in [("openrouter/auto", "openrouter"), ("gpt-5", "codex")] {
            let (orch, starts, _backend) =
                held_launch_fixture(&format!("call-deferral-{backend}.db"), backend).await;
            let call = crate::execution::jobs::PreparedCallRun {
                job_id: "j".into(),
                run_id: "cold".into(),
                session_id: "s".into(),
                agent_config: crate::models::AgentConfig {
                    id: "Explore".into(),
                    name: "Explore".into(),
                    description: String::new(),
                    prompt: String::new(),
                    tools: Vec::new(),
                    models: Vec::new(),
                    workspace_id: None,
                    project_id: None,
                    created_at: 0,
                    updated_at: 0,
                    disallowed_tools: None,
                    skills: None,
                    fence: None,
                    icon: None,
                    selection: None,
                    extras: None,
                },
                selected_model: Some(crate::models::Model::from(model)),
                prompt: "do the thing".into(),
                output_schema: crate::models::OutputSchemaInfo {
                    schema: crate::models::OutputSchema::Preset("return".into()),
                    artifact_name: Some("return".into()),
                    confirm_policy: crate::models::ConfirmPolicy::default(),
                    tool_name: None,
                    description: None,
                },
                execution_id: None,
            };

            crate::execution::jobs::start_call_run(&orch, &call).unwrap();
            assert_held_before_readiness(&orch, &starts).await;
            assert_eq!(
                text(
                    &orch,
                    "SELECT CAST(COUNT(*) AS TEXT) FROM runtime_admission_requests"
                )
                .await,
                "0",
                "a {backend} call waits without a durable row"
            );

            open_readiness_and_await_release(&orch).await;
            assert_eq!(started(&starts), 1, "readiness starts the {backend} call");
            assert_eq!(orch.process_state.has_process("cold"), Some(true));
            let pass = orch.clone();
            tokio::task::spawn_blocking(move || crate::execution::jobs::promote_call_lanes(&pass))
                .await
                .unwrap();
            assert_eq!(started(&starts), 1, "the {backend} call starts once");
        }
    }

    /// `launches` background launches queued inside this boot's ramp horizon,
    /// with the fixture's own request removed (it holds enqueue sequence 1).
    async fn ramp_fixture(name: &str, launches: u32) -> Orchestrator {
        let (mut orch, _, snapshot) = lifecycle_fixture(name).await;
        orch.boot_at = chrono::Utc::now().timestamp();
        orch.db
            .local
            .execute(
                "DELETE FROM runtime_admission_requests WHERE request_id='req'",
                (),
            )
            .await
            .unwrap();
        for n in 1..=launches {
            queue_launch(&orch, &snapshot, n, "starting").await;
        }
        orch
    }

    fn started(starts: &std::sync::atomic::AtomicUsize) -> usize {
        starts.load(std::sync::atomic::Ordering::SeqCst)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn the_boot_ramp_keeps_four_spawns_warming_and_first_output_admits_the_next() {
        let orch = ramp_fixture("boot-ramp-window.db", 6).await;
        let (starts, _backend) = install_claude_like(&orch);

        super::promote_private_lane_blocking(&orch, "lane").unwrap();
        assert_eq!(started(&starts), 4);
        for n in 5..=6 {
            assert_eq!(request_state(&orch, &format!("req{n}")).await.0, "queued");
        }
        // Held launches stay durably queued, which the reaper reads as a launch
        // in flight rather than a run to settle.
        let pending = crate::storage::pending_launch_run_ids(&orch.db.local)
            .await
            .unwrap();
        assert!(
            pending.contains("r5") && pending.contains("r6"),
            "{pending:?}"
        );
        crate::runs::reap::reap_stale_runs(&orch);
        assert_eq!(
            text(&orch, "SELECT status FROM runs WHERE id='r5'").await,
            "starting"
        );
        assert_eq!(
            text(&orch, "SELECT status FROM runs WHERE id='r6'").await,
            "starting"
        );

        // A spawn past its first output no longer occupies the window, and its
        // slot goes to the next background launch.
        crate::execution::jobs::spawn_produced_first_output(&orch, "r1");
        assert!(!orch.private_lanes.warming_runs().contains("r1"));
        promote_until_started(&orch, "r5").await;
        assert_eq!(started(&starts), 5);
        assert_eq!(request_state(&orch, "req6").await.0, "queued");
        assert_eq!(
            orch.private_lanes.warming_runs(),
            ["r2", "r3", "r4", "r5"]
                .map(String::from)
                .into_iter()
                .collect()
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_failed_start_releases_its_boot_ramp_slot() {
        let orch = ramp_fixture("boot-ramp-start-failure.db", 6).await;
        orch.db
            .local
            .execute(
                "UPDATE prepared_ordinary_launches SET agent_config_json='\"not an agent config\"' WHERE request_id='req1'",
                (),
            )
            .await
            .unwrap();
        let (starts, _backend) = install_claude_like(&orch);

        super::promote_private_lane_blocking(&orch, "lane").unwrap();
        assert_eq!(request_state(&orch, "req1").await.0, "failed");
        assert_eq!(started(&starts), 4, "r2 through r5 take the window");
        assert_eq!(orch.process_state.has_process("r5"), Some(true));
        assert_eq!(request_state(&orch, "req6").await.0, "queued");
        assert!(!orch.private_lanes.warming_runs().contains("r1"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_spawn_silent_past_the_warming_timeout_releases_its_slot() {
        let orch = ramp_fixture("boot-ramp-timeout.db", 5).await;
        let (starts, _backend) = install_claude_like(&orch);

        super::promote_private_lane_blocking(&orch, "lane").unwrap();
        assert_eq!(started(&starts), 4);
        orch.private_lanes
            .backdate_warming("r1", super::super::private_lane::BOOT_RAMP_WARMING_TIMEOUT);
        super::promote_private_lane_blocking(&orch, "lane").unwrap();
        assert_eq!(started(&starts), 5);
        assert!(!orch.private_lanes.warming_runs().contains("r1"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn an_urgent_launch_starts_while_the_boot_ramp_is_full() {
        let (mut orch, _, snapshot) = lifecycle_fixture("boot-ramp-urgent.db").await;
        orch.boot_at = chrono::Utc::now().timestamp();
        orch.db
            .local
            .execute(
                "DELETE FROM runtime_admission_requests WHERE request_id='req'",
                (),
            )
            .await
            .unwrap();
        for n in 1..=5 {
            queue_launch(&orch, &snapshot, n, "starting").await;
        }
        let (starts, _backend) = install_claude_like(&orch);
        super::promote_private_lane_blocking(&orch, "lane").unwrap();
        assert_eq!(started(&starts), 4);

        queue_launch(&orch, &snapshot, 6, "starting").await;
        orch.db
            .local
            .execute(
                "UPDATE runtime_admission_requests SET priority=1 WHERE request_id='req6'",
                (),
            )
            .await
            .unwrap();
        super::promote_private_lane_blocking(&orch, "lane").unwrap();
        assert_eq!(orch.process_state.has_process("r6"), Some(true));
        assert_eq!(request_state(&orch, "req5").await.0, "queued");
        assert_eq!(
            orch.private_lanes.warming_runs().len(),
            5,
            "an urgent spawn still counts as warming"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn past_the_boot_horizon_no_spawn_window_applies() {
        let mut orch = ramp_fixture("boot-ramp-past-horizon.db", 6).await;
        orch.boot_at = 0;
        let (starts, _backend) = install_claude_like(&orch);
        super::promote_private_lane_blocking(&orch, "lane").unwrap();
        assert_eq!(started(&starts), 6);
        assert!(orch.private_lanes.warming_runs().is_empty());
    }

    /// A watchdog recovery whose successor the ramp holds is not recovered a
    /// second time. A periodic reconcile long after the claim may re-claim the
    /// lease, but it finds the predecessor no longer the job's current target
    /// and closes it without a successor of its own; the held successor stays
    /// queued (its `starting` run also keeps the stranded-job sweep off it) and
    /// starts exactly once when a slot frees.
    #[tokio::test(flavor = "current_thread")]
    async fn a_ramp_held_watchdog_successor_is_not_recovered_again_while_it_waits() {
        let (mut orch, _, snapshot) = lifecycle_fixture("boot-ramp-watchdog-successor.db").await;
        orch.boot_at = chrono::Utc::now().timestamp();
        let (starts, _backend) = install_claude_like(&orch);
        crate::storage::persist_prepared_ordinary_launch(&orch.db.local, snapshot)
            .await
            .unwrap();
        orch.db.local.execute_script(
            "UPDATE runtime_admission_requests SET state='queued', lease_owner=NULL, lease_expires_at=NULL WHERE request_id='req';
             INSERT INTO runtime_admission_requests(request_id,workspace_key,resource_lane,launch_key,claim_json,priority,enqueue_seq,state,lease_owner,lease_generation,lease_expires_at,created_at,updated_at) VALUES
                ('warm-1','private-runtime','lane','ordinary:warm-1','{\"residentProcessUnits\":1,\"logicalStreamUnits\":1}',0,2,'active','ordinary-session-promoter',1,9999999999,1,1),
                ('warm-2','private-runtime','lane','ordinary:warm-2','{\"residentProcessUnits\":1,\"logicalStreamUnits\":1}',0,3,'active','ordinary-session-promoter',1,9999999999,1,1),
                ('warm-3','private-runtime','lane','ordinary:warm-3','{\"residentProcessUnits\":1,\"logicalStreamUnits\":1}',0,4,'active','ordinary-session-promoter',1,9999999999,1,1),
                ('warm-4','private-runtime','lane','ordinary:warm-4','{\"residentProcessUnits\":1,\"logicalStreamUnits\":1}',0,5,'active','ordinary-session-promoter',1,9999999999,1,1);
             INSERT INTO sessions(id,job_id,backend,status,sequence,created_at,updated_at)
               VALUES('predecessor-session','j','claude','open',2,1,1);
             INSERT INTO runs(id,project_id,issue_id,job_id,session_id,status,created_at,updated_at)
               VALUES('predecessor','p','i','j','predecessor-session','live',1,1);
             INSERT INTO turns(id,session_id,run_id,job_id,sequence,state,start_reason,created_at,updated_at)
               VALUES('predecessor-turn','predecessor-session','predecessor','j',1,'running','initial',1,1);",
        ).await.unwrap();
        for n in 1..=4 {
            orch.private_lanes.begin_warming(
                &format!("warm-run-{n}"),
                "lane",
                &format!("warm-{n}"),
                false,
            );
        }
        let identity = crate::runs::watchdog_ledger::WatchdogIdentity {
            run_id: "predecessor".into(),
            session_id: "predecessor-session".into(),
            provider_turn_id: "provider-turn".into(),
            generation: "generation".into(),
        };
        crate::runs::watchdog_ledger::arm_watchdog(
            &orch.db.local,
            crate::runs::watchdog_ledger::NewWatchdogLease {
                identity: identity.clone(),
                runner_boot_id: "boot".into(),
                phase: crate::runs::watchdog_ledger::WatchdogPhase::ProviderProgress,
                phase_deadline_at: 2,
                now: 1,
            },
            "provider silence".into(),
        )
        .await
        .unwrap();
        assert!(crate::runs::watchdog_ledger::claim_watchdog_recovery(
            &orch.db.local,
            &identity,
            3,
        )
        .await
        .unwrap());
        assert!(
            crate::runs::watchdog_ledger::record_pending_recovery_successor(
                &orch.db.local,
                &identity,
                3,
                "cold",
                "t",
            )
            .await
            .unwrap()
        );

        super::promote_private_lane_blocking(&orch, "lane").unwrap();
        assert_eq!(started(&starts), 0);
        assert_eq!(request_state(&orch, "req").await.0, "queued");

        let report = crate::runs::watchdog_reconciler::reconcile_watchdog_leases(
            &orch.db.local,
            crate::runs::watchdog_reconciler::ReconcileScope::Periodic,
            chrono::Utc::now().timestamp() + 3_600,
            crate::runs::watchdog_reconciler::DEFAULT_HEARTBEAT_GRACE_SECS,
            &orch,
        )
        .await
        .unwrap();
        assert_eq!(report.recovery_failed, 0, "{report:?}");
        assert_eq!(started(&starts), 0);
        assert_eq!(
            text(
                &orch,
                "SELECT CAST(COUNT(*) AS TEXT) FROM runs WHERE job_id='j'"
            )
            .await,
            "2",
            "no second successor run"
        );
        assert_eq!(request_state(&orch, "req").await.0, "queued");

        orch.private_lanes
            .release_warming("warm-run-1", "first_output");
        promote_until_started(&orch, "cold").await;
        assert_eq!(started(&starts), 1);
        let lease = crate::runs::watchdog_ledger::get_watchdog_lease(
            &orch.db.local,
            "predecessor",
            "predecessor-session",
            "provider-turn",
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            lease.state,
            crate::runs::watchdog_ledger::WatchdogLeaseState::Terminalized
        );
    }

    #[test]
    fn prepared_human_metadata_does_not_erase_rehydrated_credentials() {
        let mut identity = crate::identity::UserIdentity {
            user_id: "credential-owner".into(),
            email: "credential@example.com".into(),
            name: "Credential Owner".into(),
            claude_auth: Some(crate::credentials::ClaudeAuth::ApiKey(
                "anthropic-secret".into(),
            )),
            codex_auth: Some(crate::credentials::CodexAuth::ApiKey(
                "openai-secret".into(),
            )),
            github_token: Some("github-secret".into()),
        };

        apply_prepared_identity_metadata(
            &mut identity,
            crate::storage::PreparedUserIdentity {
                user_id: "request-user".into(),
                email: "request@example.com".into(),
                name: "Request User".into(),
            },
        );

        assert_eq!(identity.user_id, "request-user");
        assert_eq!(identity.email, "request@example.com");
        assert_eq!(identity.name, "Request User");
        assert!(matches!(
            identity.claude_auth,
            Some(crate::credentials::ClaudeAuth::ApiKey(ref token)) if token == "anthropic-secret"
        ));
        assert!(matches!(
            identity.codex_auth,
            Some(crate::credentials::CodexAuth::ApiKey(ref token)) if token == "openai-secret"
        ));
        assert_eq!(identity.github_token.as_deref(), Some("github-secret"));
    }
}
