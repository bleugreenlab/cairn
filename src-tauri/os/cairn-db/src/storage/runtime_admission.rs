use super::{DbError, DbResult, LocalDb, RowExt};
use crate::turso::params;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashSet;

const COLUMNS: &str = "request_id,workspace_key,resource_lane,launch_key,claim_json,priority,enqueue_seq,state,lease_owner,lease_generation,lease_expires_at,failure_detail,created_at,updated_at";
pub const DEFAULT_URGENT_BURST: i64 = 3;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeResourceClaim {
    #[serde(alias = "resident_process_units")]
    pub resident_process_units: u32,
    #[serde(alias = "server_instance_key")]
    pub server_instance_key: Option<String>,
    #[serde(alias = "logical_stream_units")]
    pub logical_stream_units: u32,
    #[serde(alias = "estimated_memory_bytes")]
    pub estimated_memory_bytes: Option<u64>,
}

/// Idempotently records an ordinary admission request and its immutable launch
/// identity in one transaction. A queued ordinary request is authoritative only
/// when its prepared launch can be reconstructed after a crash.
pub async fn enqueue_prepared_ordinary_launch(
    db: &LocalDb,
    new: NewAdmissionRequest,
    mut launch: PreparedOrdinaryLaunch,
) -> DbResult<AdmissionRequest> {
    let claim_json = encode_json(&new.resource_claim, "runtime resource claim")?;
    let reason = decode_json::<String>(
        &encode_json(&launch.reason, "ordinary launch reason")?,
        "ordinary launch reason token",
    )?;
    let session_start = encode_json(&launch.session_start, "ordinary session start")?;
    let agent_config = encode_json(&launch.agent_config, "ordinary agent config")?;
    let output_schema = launch
        .output_schema
        .as_ref()
        .map(|schema| encode_json(schema, "ordinary output schema"))
        .transpose()?;
    let capability = encode_json(&launch.runtime_capability, "ordinary runtime capability")?;
    let identity_override = launch
        .identity_override
        .as_ref()
        .map(|identity| encode_json(identity, "ordinary launch identity"))
        .transpose()?;

    let launch_for_write = launch.clone();
    let resolved_id = db.write(move |conn| {
        let new = new.clone();
        let launch = launch_for_write.clone();
        let claim_json = claim_json.clone();
        let reason = reason.clone();
        let session_start = session_start.clone();
        let agent_config = agent_config.clone();
        let output_schema = output_schema.clone();
        let capability = capability.clone();
        let identity_override = identity_override.clone();
        Box::pin(async move {
            let mut rows = conn.query(
                "SELECT request_id FROM runtime_admission_requests WHERE workspace_key=?1 AND launch_key=?2",
                params![new.workspace_key.clone(), new.launch_key.clone()],
            ).await?;
            let request_id = if let Some(row) = rows.next().await? {
                row.text(0)?
            } else {
                let mut seq_rows = conn.query(
                    "SELECT COALESCE(MAX(enqueue_seq),0)+1 FROM runtime_admission_requests WHERE workspace_key=?1 AND resource_lane=?2",
                    params![new.workspace_key.clone(), new.resource_lane.clone()],
                ).await?;
                let seq = seq_rows.next().await?.expect("aggregate row").i64(0)?;
                conn.execute(
                    "INSERT INTO runtime_admission_requests(request_id,workspace_key,resource_lane,launch_key,claim_json,priority,enqueue_seq,created_at,updated_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?8)",
                    params![new.request_id.clone(),new.workspace_key,new.resource_lane,new.launch_key,claim_json,i64::from(new.urgent),seq,new.now],
                ).await?;
                new.request_id
            };
            conn.execute(
                "INSERT OR IGNORE INTO prepared_ordinary_launches(request_id,job_id,run_id,session_id,turn_id,execution_id,launch_event_id,prompt,reason,session_start_json,model,agent_config_json,output_schema_json,runtime_capability_json,provider_account_id,identity_override_json,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17)",
                params![request_id.clone(),launch.job_id,launch.run_id,launch.session_id,launch.turn_id,launch.execution_id,launch.launch_event_id,launch.prompt,reason,session_start,launch.model,agent_config,output_schema,capability,launch.provider_account_id,identity_override,launch.created_at],
            ).await?;
            let mut prepared = conn.query(
                "SELECT 1 FROM prepared_ordinary_launches WHERE request_id=?1",
                (request_id.clone(),),
            ).await?;
            if prepared.next().await?.is_none() {
                return Err(DbError::internal(
                    "ordinary admission request conflicts with another prepared run",
                ));
            }
            Ok(request_id)
        })
    }).await?;

    launch.request_id = resolved_id.clone();
    let persisted = get_prepared_ordinary_launch(db, &resolved_id)
        .await?
        .ok_or_else(|| DbError::internal("atomically enqueued ordinary launch disappeared"))?;
    if persisted != launch {
        return Err(DbError::internal(
            "ordinary launch identity conflicts with the immutable prepared launch",
        ));
    }
    get_admission_request(db, &resolved_id)
        .await?
        .ok_or_else(|| DbError::internal("atomically enqueued admission request disappeared"))
}

/// Runner-local journal of the exact Postgres lease generation attached to a
/// team ordinary process. The row is recovery metadata, never local authority.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TeamOrdinaryAuthority {
    pub request_id: String,
    pub lease_owner: String,
    pub lease_generation: i64,
    pub lease_host_id: String,
    pub pending_terminal_action: Option<String>,
    pub pending_terminal_detail: Option<String>,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RecoverableTeamOrdinaryLaunch {
    pub workspace_id: String,
    pub launch: PreparedOrdinaryLaunch,
    pub authority: Option<TeamOrdinaryAuthority>,
}

pub async fn persist_team_ordinary_lease(
    db: &LocalDb,
    authority: TeamOrdinaryAuthority,
) -> DbResult<()> {
    if authority.lease_owner.is_empty() || authority.lease_host_id.is_empty() {
        return Err(DbError::internal(
            "team ordinary lease identity is incomplete",
        ));
    }
    db.write(move |conn| {
        let authority = authority.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO team_ordinary_admission_authority(request_id,lease_owner,lease_generation,lease_host_id,pending_terminal_action,pending_terminal_detail,updated_at) VALUES(?1,?2,?3,?4,NULL,NULL,?5) ON CONFLICT(request_id) DO UPDATE SET lease_owner=excluded.lease_owner,lease_generation=excluded.lease_generation,lease_host_id=excluded.lease_host_id,pending_terminal_action=CASE WHEN excluded.lease_generation>team_ordinary_admission_authority.lease_generation THEN NULL ELSE team_ordinary_admission_authority.pending_terminal_action END,pending_terminal_detail=CASE WHEN excluded.lease_generation>team_ordinary_admission_authority.lease_generation THEN NULL ELSE team_ordinary_admission_authority.pending_terminal_detail END,updated_at=excluded.updated_at WHERE excluded.lease_generation >= team_ordinary_admission_authority.lease_generation",
                params![authority.request_id.clone(), authority.lease_owner, authority.lease_generation, authority.lease_host_id, authority.updated_at],
            ).await?;
            // The mirror's phase is this runner's local launch fact, not
            // authority: leased here, not yet registered. Set in the same write
            // as the journal so reaper cover never lapses between them.
            conn.execute(
                "UPDATE runtime_admission_requests SET state='leased',updated_at=?2 WHERE request_id=?1 AND state='queued'",
                params![authority.request_id, authority.updated_at],
            ).await?;
            Ok(())
        })
    })
    .await
}

/// Record a team mirror's local launch phase as `starting` or `active`. Not
/// fenced: the mirror has no local lease owner, and Postgres stays the only
/// authority. The phase is a correctness input to the reaper, so this reports
/// whether it now holds: re-recording the same phase, or a recovered relaunch
/// moving an old `active` mirror back to `starting`, succeeds; a mirror that is
/// missing or already terminal is a rejection (`false`), and a failed write is
/// an error. Neither may be read as success.
pub async fn advance_team_mirror_phase(
    db: &LocalDb,
    request_id: &str,
    to: &str,
    now: i64,
) -> DbResult<bool> {
    if !matches!(to, "starting" | "active") {
        return Err(DbError::internal("invalid team mirror launch phase"));
    }
    let changed = db
        .execute(
            "UPDATE runtime_admission_requests SET state=?2,updated_at=?3 WHERE request_id=?1 AND workspace_key LIKE 'team:%' AND state IN ('queued','leased','starting','active')",
            params![request_id.to_string(), to.to_string(), now],
        )
        .await?;
    Ok(changed == 1)
}

/// Retire the mirror of a team request Postgres has already let go of, from any
/// phase before its process registered.
pub async fn retire_team_mirror(
    db: &LocalDb,
    request_id: &str,
    detail: &str,
    now: i64,
) -> DbResult<bool> {
    let changed = db
        .execute(
            "UPDATE runtime_admission_requests SET state='cancelled',failure_detail=?2,updated_at=?3 WHERE request_id=?1 AND workspace_key LIKE 'team:%' AND state IN ('queued','leased','starting')",
            params![request_id.to_string(), detail.to_string(), now],
        )
        .await?;
    Ok(changed == 1)
}

pub async fn queue_team_ordinary_terminal(
    db: &LocalDb,
    request_id: &str,
    generation: i64,
    action: &str,
    detail: Option<&str>,
    now: i64,
) -> DbResult<bool> {
    if !matches!(action, "released" | "cancelled" | "failed") {
        return Err(DbError::internal("invalid team ordinary terminal action"));
    }
    let changed = db.execute(
        "UPDATE team_ordinary_admission_authority SET pending_terminal_action=?3,pending_terminal_detail=?4,updated_at=?5 WHERE request_id=?1 AND lease_generation=?2",
        params![request_id.to_string(), generation, action.to_string(), detail.map(str::to_string), now],
    ).await?;
    Ok(changed == 1)
}

pub async fn clear_team_ordinary_terminal(
    db: &LocalDb,
    request_id: &str,
    generation: i64,
) -> DbResult<bool> {
    let request_id = request_id.to_string();
    db.write(move |conn| {
        let request_id = request_id.clone();
        Box::pin(async move {
            let mut rows = conn.query(
                "SELECT pending_terminal_action FROM team_ordinary_admission_authority WHERE request_id=?1 AND lease_generation=?2",
                params![request_id.clone(), generation],
            ).await?;
            let Some(row) = rows.next().await? else { return Ok(false) };
            let action = row.text(0)?;
            conn.execute(
                "UPDATE runtime_admission_requests SET state=?2,updated_at=strftime('%s','now') WHERE request_id=?1",
                params![request_id.clone(), action],
            ).await?;
            conn.execute(
                "DELETE FROM team_ordinary_admission_authority WHERE request_id=?1 AND lease_generation=?2",
                params![request_id, generation],
            ).await?;
            Ok(true)
        })
    }).await
}

pub async fn list_pending_team_ordinary_terminals(
    db: &LocalDb,
) -> DbResult<Vec<TeamOrdinaryAuthority>> {
    db.query_all(
        "SELECT request_id,lease_owner,lease_generation,lease_host_id,pending_terminal_action,pending_terminal_detail,updated_at FROM team_ordinary_admission_authority WHERE pending_terminal_action IS NOT NULL ORDER BY updated_at,request_id",
        (),
        |row| Ok(TeamOrdinaryAuthority { request_id: row.text(0)?, lease_owner: row.text(1)?, lease_generation: row.i64(2)?, lease_host_id: row.text(3)?, pending_terminal_action: row.opt_text(4)?, pending_terminal_detail: row.opt_text(5)?, updated_at: row.i64(6)? }),
    ).await
}

pub async fn list_recoverable_team_ordinary_launches(
    db: &LocalDb,
) -> DbResult<Vec<RecoverableTeamOrdinaryLaunch>> {
    let ids = db.query_all(
        "SELECT p.request_id,r.workspace_key FROM prepared_ordinary_launches p JOIN runtime_admission_requests r ON r.request_id=p.request_id WHERE r.workspace_key LIKE 'team:%' AND r.state IN ('queued','leased','starting','active') ORDER BY r.enqueue_seq",
        (),
        |row| Ok((row.text(0)?, row.text(1)?)),
    ).await?;
    let mut result = Vec::with_capacity(ids.len());
    for (request_id, workspace_key) in ids {
        let Some(launch) = get_prepared_ordinary_launch(db, &request_id).await? else {
            continue;
        };
        let authority = db.query_opt(
            "SELECT request_id,lease_owner,lease_generation,lease_host_id,pending_terminal_action,pending_terminal_detail,updated_at FROM team_ordinary_admission_authority WHERE request_id=?1",
            (request_id,),
            |row| Ok(TeamOrdinaryAuthority { request_id: row.text(0)?, lease_owner: row.text(1)?, lease_generation: row.i64(2)?, lease_host_id: row.text(3)?, pending_terminal_action: row.opt_text(4)?, pending_terminal_detail: row.opt_text(5)?, updated_at: row.i64(6)? }),
        ).await?;
        result.push(RecoverableTeamOrdinaryLaunch {
            workspace_id: workspace_key.trim_start_matches("team:").to_string(),
            launch,
            authority,
        });
    }
    Ok(result)
}

/// Provider conversation reference needed to reconstruct an ordinary backend start.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PreparedSessionStart {
    New {
        session_id: String,
    },
    Resume {
        session_id: String,
        backend_id: String,
    },
    Fork {
        session_id: String,
        source_backend_id: String,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PreparedRuntimeClass {
    DedicatedProcess,
    MultiplexedServer,
    StatelessHttp,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PreparedRuntimeHostKind {
    Runner,
    Executor,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "requirement", content = "host_kind", rename_all = "snake_case")]
pub enum PreparedRuntimeHostRequirement {
    None,
    ResidentOn(PreparedRuntimeHostKind),
}

/// Complete non-secret admission metadata published by the selected backend.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PreparedRuntimeCapability {
    pub class: PreparedRuntimeClass,
    pub lane: String,
    pub host_requirement: PreparedRuntimeHostRequirement,
    pub claim: RuntimeResourceClaim,
    pub supports_warm_reuse: bool,
}

/// Immutable inputs required to reconstruct an ordinary session launch after restart.
/// The prompt remains canonical in `launch_event_id`; provider credentials are resolved
/// from `provider_account_id` only when the request is promoted.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PreparedOrdinaryLaunch {
    pub request_id: String,
    pub job_id: String,
    pub run_id: String,
    pub session_id: String,
    pub turn_id: String,
    pub execution_id: String,
    pub launch_event_id: String,
    /// Exact backend prompt. Continuations assemble this from multiple durable
    /// transcript events, so no single event can reconstruct it losslessly.
    pub prompt: String,
    pub reason: crate::models::TurnStartReason,
    pub session_start: PreparedSessionStart,
    pub model: String,
    pub agent_config: Value,
    pub output_schema: Option<Value>,
    pub runtime_capability: PreparedRuntimeCapability,
    pub provider_account_id: Option<String>,
    /// Request-scoped human identity metadata. Authentication material is never
    /// represented by this type and is resolved again by the launch host.
    pub identity_override: Option<PreparedUserIdentity>,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PreparedUserIdentity {
    pub user_id: String,
    pub email: String,
    pub name: String,
}

fn encode_json<T: Serialize>(value: &T, label: &str) -> DbResult<String> {
    serde_json::to_string(value)
        .map_err(|error| DbError::internal(format!("failed to serialize {label}: {error}")))
}

fn decode_json<T: for<'de> Deserialize<'de>>(value: &str, label: &str) -> DbResult<T> {
    serde_json::from_str(value)
        .map_err(|error| DbError::internal(format!("invalid {label}: {error}")))
}

pub async fn persist_prepared_ordinary_launch(
    db: &LocalDb,
    launch: PreparedOrdinaryLaunch,
) -> DbResult<()> {
    let reason = encode_json(&launch.reason, "ordinary launch reason")?;
    let reason = decode_json::<String>(&reason, "ordinary launch reason token")?;
    let session_start = encode_json(&launch.session_start, "ordinary session start")?;
    let agent_config = encode_json(&launch.agent_config, "ordinary agent config")?;
    let output_schema = launch
        .output_schema
        .as_ref()
        .map(|schema| encode_json(schema, "ordinary output schema"))
        .transpose()?;
    let capability = encode_json(&launch.runtime_capability, "ordinary runtime capability")?;
    let identity_override = launch
        .identity_override
        .as_ref()
        .map(|identity| encode_json(identity, "ordinary launch identity"))
        .transpose()?;
    db.execute(
        "INSERT INTO prepared_ordinary_launches(request_id,job_id,run_id,session_id,turn_id,execution_id,launch_event_id,prompt,reason,session_start_json,model,agent_config_json,output_schema_json,runtime_capability_json,provider_account_id,identity_override_json,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17)",
        params![launch.request_id,launch.job_id,launch.run_id,launch.session_id,launch.turn_id,launch.execution_id,launch.launch_event_id,launch.prompt,reason,session_start,launch.model,agent_config,output_schema,capability,launch.provider_account_id,identity_override,launch.created_at],
    ).await?;
    Ok(())
}

pub async fn get_prepared_ordinary_launch(
    db: &LocalDb,
    request_id: &str,
) -> DbResult<Option<PreparedOrdinaryLaunch>> {
    db.query_opt(
        "SELECT request_id,job_id,run_id,session_id,turn_id,execution_id,launch_event_id,prompt,reason,session_start_json,model,agent_config_json,output_schema_json,runtime_capability_json,provider_account_id,identity_override_json,created_at FROM prepared_ordinary_launches WHERE request_id=?1",
        (request_id.to_string(),),
        |row| {
            let reason = format!("\"{}\"", row.text(8)?);
            Ok(PreparedOrdinaryLaunch {
                request_id: row.text(0)?, job_id: row.text(1)?, run_id: row.text(2)?,
                session_id: row.text(3)?, turn_id: row.text(4)?, execution_id: row.text(5)?,
                launch_event_id: row.text(6)?, prompt: row.text(7)?, reason: decode_json(&reason, "ordinary launch reason")?,
                session_start: decode_json(&row.text(9)?, "ordinary session start")?, model: row.text(10)?,
                agent_config: decode_json(&row.text(11)?, "ordinary agent config")?,
                output_schema: row.opt_text(12)?.map(|value| decode_json(&value, "ordinary output schema")).transpose()?,
                runtime_capability: decode_json(&row.text(13)?, "ordinary runtime capability")?,
                provider_account_id: row.opt_text(14)?,
                identity_override: row.opt_text(15)?.map(|value| decode_json(&value, "ordinary launch identity")).transpose()?,
                created_at: row.i64(16)?,
            })
        },
    ).await
}

/// Requeue leases owned by a runner process which is being reconstructed after
/// restart. The old process and heartbeat tasks are gone, so retaining an
/// apparently unexpired generation would make recovery timing-dependent.
pub async fn requeue_orphaned_admissions(db: &LocalDb, workspace: &str, now: i64) -> DbResult<u64> {
    let workspace = workspace.to_string();
    db.write(move |conn| {
        let workspace = workspace.clone();
        Box::pin(async move {
            Ok(conn.execute(
                "UPDATE runtime_admission_requests SET state='queued',lease_owner=NULL,lease_expires_at=NULL,updated_at=?2 WHERE workspace_key=?1 AND state IN ('leased','starting','active')",
                params![workspace, now],
            ).await?)
        })
    }).await
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedRuntimeCall {
    pub request_id: String,
    pub run_id: String,
    pub prepared_json: String,
}

pub async fn persist_prepared_runtime_call(
    db: &LocalDb,
    call: PreparedRuntimeCall,
    now: i64,
) -> DbResult<()> {
    db.execute(
        "INSERT INTO prepared_runtime_calls(request_id,run_id,prepared_json,created_at) VALUES(?1,?2,?3,?4) ON CONFLICT(request_id) DO UPDATE SET run_id=excluded.run_id,prepared_json=excluded.prepared_json",
        params![call.request_id, call.run_id, call.prepared_json, now],
    )
    .await?;
    Ok(())
}

pub async fn get_prepared_runtime_call(
    db: &LocalDb,
    request_id: &str,
) -> DbResult<Option<PreparedRuntimeCall>> {
    db.query_opt(
        "SELECT request_id,run_id,prepared_json FROM prepared_runtime_calls WHERE request_id=?1",
        (request_id.to_string(),),
        |row| {
            Ok(PreparedRuntimeCall {
                request_id: row.text(0)?,
                run_id: row.text(1)?,
                prepared_json: row.text(2)?,
            })
        },
    )
    .await
}

pub async fn get_admission_request_by_launch_key(
    db: &LocalDb,
    workspace: &str,
    launch_key: &str,
) -> DbResult<Option<AdmissionRequest>> {
    db.query_opt(
        format!("SELECT {COLUMNS} FROM runtime_admission_requests WHERE workspace_key=?1 AND launch_key=?2"),
        params![workspace.to_string(), launch_key.to_string()],
        from_row,
    ).await
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeResourceCapacity {
    pub resident_process_units: u32,
    pub logical_stream_units: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewAdmissionRequest {
    pub request_id: String,
    pub workspace_key: String,
    pub resource_lane: String,
    pub launch_key: String,
    pub resource_claim: RuntimeResourceClaim,
    pub urgent: bool,
    pub now: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionRequest {
    pub request_id: String,
    pub workspace_key: String,
    pub resource_lane: String,
    pub launch_key: String,
    pub resource_claim: RuntimeResourceClaim,
    pub urgent: bool,
    pub enqueue_seq: i64,
    pub state: String,
    pub lease_owner: Option<String>,
    pub lease_generation: i64,
    pub lease_expires_at: Option<i64>,
    pub failure_detail: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionStatus {
    pub request: AdmissionRequest,
    pub position: Option<i64>,
    pub behind_count: Option<i64>,
    pub enqueue_age_ms: i64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeAdmissionReason {
    WaitingForRuntimeCapacity,
    AdmissionAuthorityUnavailable,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeAdmissionState {
    Queued,
    Leased,
    Starting,
    Active,
    Released,
    Cancelled,
    Failed,
}

impl RuntimeAdmissionState {
    fn parse(value: &str) -> DbResult<Self> {
        match value {
            "queued" => Ok(Self::Queued),
            "leased" => Ok(Self::Leased),
            "starting" => Ok(Self::Starting),
            "active" => Ok(Self::Active),
            "released" => Ok(Self::Released),
            "cancelled" => Ok(Self::Cancelled),
            "failed" => Ok(Self::Failed),
            other => Err(DbError::internal(format!(
                "invalid runtime admission state: {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeHostIdentity {
    pub kind: PreparedRuntimeHostKind,
    pub id: String,
}

/// Canonical projection of one ordinary launch's durable admission row.
/// Queue rank is computed at this read boundary, never by a rendered subset.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct OrdinaryAdmissionProjection {
    #[serde(skip)]
    pub workspace_key: String,
    pub request_id: String,
    pub job_id: String,
    pub run_id: String,
    pub state: RuntimeAdmissionState,
    pub reason: Option<RuntimeAdmissionReason>,
    pub position: Option<i64>,
    pub behind_count: Option<i64>,
    pub enqueue_age_ms: i64,
    pub runtime_class: PreparedRuntimeClass,
    pub effective_claim: Option<RuntimeResourceClaim>,
    pub effective_capacity: Option<RuntimeResourceCapacity>,
    /// The private lane's measured budget. Absent for team rows, whose
    /// capacity is Postgres's to report.
    #[serde(default)]
    pub lane_budget: Option<LaneBudget>,
    pub runtime_host: Option<RuntimeHostIdentity>,
    pub host_constraint: PreparedRuntimeHostRequirement,
}

/// Project every non-terminal ordinary admission. Private rows carry the
/// budget of their lane from `private_budgets`, so what a surface displays is
/// what the promoter decided against.
pub async fn ordinary_admission_projections(
    db: &LocalDb,
    now: i64,
    private_budgets: &[LaneBudget],
) -> DbResult<Vec<OrdinaryAdmissionProjection>> {
    let private_budgets = private_budgets.to_vec();
    db.query_all(
        "SELECT r.request_id,p.job_id,p.run_id,r.state,r.claim_json,r.lease_owner,r.created_at,p.runtime_capability_json,\
         CASE WHEN r.state='queued' THEN 1+(SELECT COUNT(*) FROM runtime_admission_requests q WHERE q.workspace_key=r.workspace_key AND q.resource_lane=r.resource_lane AND q.state='queued' AND q.enqueue_seq<r.enqueue_seq) END,\
         CASE WHEN r.state='queued' THEN (SELECT COUNT(*) FROM runtime_admission_requests q WHERE q.workspace_key=r.workspace_key AND q.resource_lane=r.resource_lane AND q.state='queued' AND q.enqueue_seq>r.enqueue_seq) END\
         ,r.workspace_key,r.resource_lane FROM runtime_admission_requests r JOIN prepared_ordinary_launches p ON p.request_id=r.request_id \
         WHERE r.state IN ('queued','leased','starting','active') ORDER BY r.enqueue_seq",
        (),
        move |row| {
            let state = RuntimeAdmissionState::parse(&row.text(3)?)?;
            let capability: PreparedRuntimeCapability =
                decode_json(&row.text(7)?, "ordinary runtime capability")?;
            let runtime_host = row.opt_text(5)?.map(|id| RuntimeHostIdentity {
                kind: match capability.host_requirement {
                    PreparedRuntimeHostRequirement::ResidentOn(kind) => kind,
                    PreparedRuntimeHostRequirement::None => PreparedRuntimeHostKind::Runner,
                },
                id,
            });
            let workspace_key = row.text(10)?;
            let lane = row.text(11)?;
            let lane_budget = (!workspace_key.starts_with("team:"))
                .then(|| private_budgets.iter().find(|budget| budget.lane == lane).cloned())
                .flatten();
            Ok(OrdinaryAdmissionProjection {
            workspace_key,
            request_id: row.text(0)?,
            job_id: row.text(1)?,
            run_id: row.text(2)?,
            state,
            reason: (state == RuntimeAdmissionState::Queued)
                .then_some(RuntimeAdmissionReason::WaitingForRuntimeCapacity),
            position: row.opt_i64(8)?,
            behind_count: row.opt_i64(9)?,
            enqueue_age_ms: now.saturating_sub(row.i64(6)?).saturating_mul(1_000),
            runtime_class: capability.class,
            effective_claim: Some(decode_json(&row.text(4)?, "runtime resource claim")?),
            effective_capacity: lane_budget.as_ref().map(LaneBudget::capacity),
            lane_budget,
            runtime_host,
                host_constraint: capability.host_requirement,
            })
        },
    )
    .await
}

fn from_row(row: &crate::turso::Row) -> DbResult<AdmissionRequest> {
    Ok(AdmissionRequest {
        request_id: row.text(0)?,
        workspace_key: row.text(1)?,
        resource_lane: row.text(2)?,
        launch_key: row.text(3)?,
        resource_claim: serde_json::from_str(&row.text(4)?).map_err(|error| {
            DbError::internal(format!("invalid runtime resource claim: {error}"))
        })?,
        urgent: row.i64(5)? != 0,
        enqueue_seq: row.i64(6)?,
        state: row.text(7)?,
        lease_owner: row.opt_text(8)?,
        lease_generation: row.i64(9)?,
        lease_expires_at: row.opt_i64(10)?,
        failure_detail: row.opt_text(11)?,
        created_at: row.i64(12)?,
        updated_at: row.i64(13)?,
    })
}

pub async fn get_admission_request(
    db: &LocalDb,
    request_id: &str,
) -> DbResult<Option<AdmissionRequest>> {
    db.query_opt(
        format!("SELECT {COLUMNS} FROM runtime_admission_requests WHERE request_id=?1"),
        (request_id.to_string(),),
        from_row,
    )
    .await
}

/// Idempotently enqueue by workspace and launch identity.
pub async fn enqueue_admission_request(
    db: &LocalDb,
    new: NewAdmissionRequest,
) -> DbResult<AdmissionRequest> {
    let claim_json = serde_json::to_string(&new.resource_claim).map_err(|error| {
        DbError::internal(format!(
            "failed to serialize runtime resource claim: {error}"
        ))
    })?;
    let resolved_id = db.write(move |conn| { let new = new.clone(); let claim_json = claim_json.clone(); Box::pin(async move {
        let mut rows = conn.query("SELECT request_id FROM runtime_admission_requests WHERE workspace_key=?1 AND launch_key=?2", params![new.workspace_key.clone(), new.launch_key.clone()]).await?;
        if let Some(row) = rows.next().await? { return row.text(0); }
        let mut seq_rows = conn.query("SELECT COALESCE(MAX(enqueue_seq),0)+1 FROM runtime_admission_requests WHERE workspace_key=?1 AND resource_lane=?2", params![new.workspace_key.clone(), new.resource_lane.clone()]).await?;
        let seq = seq_rows.next().await?.expect("aggregate row").i64(0)?;
        conn.execute("INSERT INTO runtime_admission_requests(request_id,workspace_key,resource_lane,launch_key,claim_json,priority,enqueue_seq,created_at,updated_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?8)", params![new.request_id.clone(),new.workspace_key,new.resource_lane,new.launch_key,claim_json,i64::from(new.urgent),seq,new.now]).await?;
        Ok(new.request_id)
    })}).await.and_then(|id| if id.is_empty() { Err(DbError::internal("empty admission request id")) } else { Ok(id) })?;
    get_admission_request(db, &resolved_id)
        .await?
        .ok_or_else(|| DbError::internal("enqueued admission request disappeared"))
}

/// Lease one request with restart-persistent bounded priority fairness.
pub async fn lease_next_admission(
    db: &LocalDb,
    workspace: &str,
    lane: &str,
    owner: &str,
    now: i64,
    expires_at: i64,
) -> DbResult<Option<AdmissionRequest>> {
    lease_next_admission_with_capacity(
        db,
        workspace,
        lane,
        owner,
        now,
        expires_at,
        RuntimeResourceCapacity {
            resident_process_units: u32::MAX,
            logical_stream_units: u32::MAX,
        },
    )
    .await
}

/// Lease one request only when the lane has room. Capacity inspection and the
/// queued-to-leased transition share the repository write transaction.
pub async fn lease_next_admission_with_capacity(
    db: &LocalDb,
    workspace: &str,
    lane: &str,
    owner: &str,
    now: i64,
    expires_at: i64,
    capacity: RuntimeResourceCapacity,
) -> DbResult<Option<AdmissionRequest>> {
    Ok(
        match lease_next_admission_within(
            db, workspace, lane, owner, now, expires_at, capacity, None,
        )
        .await?
        {
            LeaseDecision::Leased(request) => Some(request),
            LeaseDecision::Empty | LeaseDecision::Blocked { .. } => None,
        },
    )
}

/// Host memory a lane admits against. Measured by the caller immediately before
/// the lease, with the same probe and the same reserve the warm GC keeps, so
/// "memory this host keeps free" has one number.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct HostMemoryEnvelope {
    pub available_bytes: u64,
    pub reserve_bytes: u64,
    /// What a claim that declares no `estimated_memory_bytes` is charged. An
    /// unknown cost is not a zero cost.
    pub unmeasured_claim_bytes: u64,
}

impl HostMemoryEnvelope {
    pub fn charge(&self, claim: &RuntimeResourceClaim) -> u64 {
        claim
            .estimated_memory_bytes
            .unwrap_or(self.unmeasured_claim_bytes)
    }

    /// Bytes above the reserve that spawns already in flight have not claimed.
    pub fn headroom(&self, in_flight_bytes: u64) -> u64 {
        self.available_bytes
            .saturating_sub(self.reserve_bytes)
            .saturating_sub(in_flight_bytes)
    }
}

/// Why a lane with queued work admitted none of it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum LaneShortfall {
    /// Admitting the head would eat into the memory reserve.
    #[serde(rename_all = "camelCase")]
    Memory {
        available_bytes: u64,
        reserve_bytes: u64,
        in_flight_bytes: u64,
        head_estimate_bytes: u64,
    },
    /// An explicit resident-process ceiling is full.
    #[serde(rename_all = "camelCase")]
    Units { active: u64, ceiling: u32 },
    /// The lane's logical stream ceiling is full.
    #[serde(rename_all = "camelCase")]
    Streams { active: u64, ceiling: u32 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaseDecision {
    Leased(AdmissionRequest),
    /// Nothing is queued in the lane.
    Empty,
    /// Work is queued and none of it fits. `in_flight_requests` counts the
    /// lane's `leased`/`starting` rows: spawns the host probe cannot see yet.
    Blocked {
        head_request_id: String,
        shortfall: LaneShortfall,
        in_flight_requests: u64,
    },
}

fn adds_resident_process(claim: &RuntimeResourceClaim, resident_servers: &HashSet<String>) -> bool {
    claim.resident_process_units > 0
        && claim
            .server_instance_key
            .as_ref()
            .is_none_or(|key| !resident_servers.contains(key))
}

/// Lease the next queued request that fits both the unit ceilings in `capacity`
/// and, when `memory` is measured, the host memory envelope.
///
/// Memory is charged only for spawns the probe cannot see yet: `leased` and
/// `starting` rows. An `active` row's process is resident, so the measured
/// `available_bytes` already accounts for it. A lane holding nothing at all
/// always admits one request, so a host below its reserve still makes progress
/// (the executor substrate follows the same liveness rule). An unmeasured host
/// admits on memory: the unit ceilings and the warm GC's own fallback still
/// apply. Memory admission is FIFO: once the head is blocked on memory, no later
/// process-claiming request may pass it, so a large head cannot be starved.
#[allow(clippy::too_many_arguments)]
pub async fn lease_next_admission_within(
    db: &LocalDb,
    workspace: &str,
    lane: &str,
    owner: &str,
    now: i64,
    expires_at: i64,
    capacity: RuntimeResourceCapacity,
    memory: Option<HostMemoryEnvelope>,
) -> DbResult<LeaseDecision> {
    let (workspace, lane, owner) = (workspace.to_string(), lane.to_string(), owner.to_string());
    enum Outcome {
        Leased(String),
        Empty,
        Blocked(String, LaneShortfall, u64),
    }
    let outcome = db.write(move |conn| { let (workspace,lane,owner)=(workspace.clone(),lane.clone(),owner.clone()); Box::pin(async move {
        conn.execute("INSERT OR IGNORE INTO runtime_admission_cursors(workspace_key,resource_lane) VALUES(?1,?2)", params![workspace.clone(),lane.clone()]).await?;
        let mut cursor = conn.query("SELECT urgent_streak FROM runtime_admission_cursors WHERE workspace_key=?1 AND resource_lane=?2", params![workspace.clone(),lane.clone()]).await?;
        let streak = cursor.next().await?.expect("cursor row").i64(0)?;
        let prefer_urgent = streak < DEFAULT_URGENT_BURST;
        let mut active_rows = conn.query("SELECT claim_json,state FROM runtime_admission_requests WHERE workspace_key=?1 AND resource_lane=?2 AND state IN ('leased','starting','active')", params![workspace.clone(),lane.clone()]).await?;
        let mut occupied = 0_u64;
        let mut in_flight_requests = 0_u64;
        let mut in_flight_bytes = 0_u64;
        let mut active_process_units = 0_u64;
        let mut active_stream_units = 0_u64;
        let mut resident_servers = HashSet::new();
        while let Some(active_row) = active_rows.next().await? {
            let claim: RuntimeResourceClaim = serde_json::from_str(&active_row.text(0)?)
                .map_err(|error| DbError::internal(format!("invalid active runtime resource claim: {error}")))?;
            let spawning = matches!(active_row.text(1)?.as_str(), "leased" | "starting");
            occupied += 1;
            active_stream_units += u64::from(claim.logical_stream_units);
            if spawning {
                in_flight_requests += 1;
            }
            if adds_resident_process(&claim, &resident_servers) {
                active_process_units += u64::from(claim.resident_process_units);
                if spawning {
                    in_flight_bytes = in_flight_bytes
                        .saturating_add(memory.map_or(0, |memory| memory.charge(&claim)));
                }
                if let Some(key) = &claim.server_instance_key {
                    resident_servers.insert(key.clone());
                }
            }
        }
        // Units are a lane's own, but memory is one host: a spawn in flight on
        // any private lane spends the same headroom the probe has not seen yet.
        let mut other_rows = conn.query("SELECT resource_lane,claim_json FROM runtime_admission_requests WHERE workspace_key=?1 AND resource_lane<>?2 AND state IN ('leased','starting')", params![workspace.clone(),lane.clone()]).await?;
        let mut other_servers = HashSet::new();
        while let Some(other_row) = other_rows.next().await? {
            let other_lane = other_row.text(0)?;
            let claim: RuntimeResourceClaim = serde_json::from_str(&other_row.text(1)?)
                .map_err(|error| DbError::internal(format!("invalid in-flight runtime resource claim: {error}")))?;
            let new_process = claim.resident_process_units > 0
                && match &claim.server_instance_key {
                    None => true,
                    Some(key) => other_servers.insert((other_lane, key.clone())),
                };
            if new_process {
                in_flight_bytes = in_flight_bytes
                    .saturating_add(memory.map_or(0, |memory| memory.charge(&claim)));
            }
        }

        let mut rows = conn.query("SELECT request_id,priority,claim_json FROM runtime_admission_requests WHERE workspace_key=?1 AND resource_lane=?2 AND state='queued' ORDER BY CASE WHEN ?3=1 THEN priority ELSE 1-priority END DESC, enqueue_seq", params![workspace.clone(),lane.clone(),i64::from(prefer_urgent)]).await?;
        let mut selected = None;
        let mut head: Option<(String, LaneShortfall)> = None;
        let mut memory_blocked = false;
        while let Some(row) = rows.next().await? {
            let id = row.text(0)?;
            let candidate: RuntimeResourceClaim = serde_json::from_str(&row.text(2)?)
                .map_err(|error| DbError::internal(format!("invalid queued runtime resource claim: {error}")))?;
            let candidate_process_units = if adds_resident_process(&candidate, &resident_servers) {
                u64::from(candidate.resident_process_units)
            } else {
                0
            };
            let shortfall = if active_process_units + candidate_process_units > u64::from(capacity.resident_process_units) {
                Some(LaneShortfall::Units { active: active_process_units, ceiling: capacity.resident_process_units })
            } else if active_stream_units + u64::from(candidate.logical_stream_units) > u64::from(capacity.logical_stream_units) {
                Some(LaneShortfall::Streams { active: active_stream_units, ceiling: capacity.logical_stream_units })
            } else if candidate_process_units == 0 || occupied == 0 {
                None
            } else if memory_blocked {
                continue;
            } else {
                memory.and_then(|memory| {
                    let estimate = memory.charge(&candidate);
                    (memory.headroom(in_flight_bytes) < estimate).then_some(LaneShortfall::Memory {
                        available_bytes: memory.available_bytes,
                        reserve_bytes: memory.reserve_bytes,
                        in_flight_bytes,
                        head_estimate_bytes: estimate,
                    })
                })
            };
            match shortfall {
                None => {
                    selected = Some((id, row.i64(1)? != 0));
                    break;
                }
                Some(shortfall) => {
                    memory_blocked |= matches!(shortfall, LaneShortfall::Memory { .. });
                    if head.is_none() {
                        head = Some((id, shortfall));
                    }
                }
            }
        }
        let Some((id, urgent)) = selected else {
            return Ok(match head {
                Some((id, shortfall)) => Outcome::Blocked(id, shortfall, in_flight_requests),
                None => Outcome::Empty,
            });
        };
        conn.execute("UPDATE runtime_admission_requests SET state='leased',lease_owner=?2,lease_generation=lease_generation+1,lease_expires_at=?3,updated_at=?4 WHERE request_id=?1 AND state='queued'", params![id.clone(),owner,expires_at,now]).await?;
        conn.execute("UPDATE runtime_admission_cursors SET urgent_streak=?3 WHERE workspace_key=?1 AND resource_lane=?2", params![workspace,lane,if urgent { streak+1 } else { 0 }]).await?;
        Ok(Outcome::Leased(id))
    })}).await?;
    match outcome {
        Outcome::Leased(id) => get_admission_request(db, &id)
            .await?
            .map(LeaseDecision::Leased)
            .ok_or_else(|| DbError::internal("leased admission request disappeared")),
        Outcome::Empty => Ok(LeaseDecision::Empty),
        Outcome::Blocked(head_request_id, shortfall, in_flight_requests) => {
            Ok(LeaseDecision::Blocked {
                head_request_id,
                shortfall,
                in_flight_requests,
            })
        }
    }
}

/// One lane's non-terminal admission rows, summed the way the lease sums them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LaneOccupancy {
    pub lane: String,
    /// Resident-process units held by `leased`/`starting`/`active` rows.
    pub active_units: u64,
    /// `leased`/`starting` rows: spawns the host probe cannot see yet.
    pub in_flight_requests: u64,
    pub in_flight_estimated_bytes: u64,
    /// In-flight claims that declared no memory estimate.
    pub in_flight_unestimated: u64,
    pub queued: u64,
    /// The largest per-process estimate any non-terminal claim declared.
    pub typical_claim_bytes: Option<u64>,
}

impl LaneOccupancy {
    pub fn in_flight_bytes(&self, unmeasured_claim_bytes: u64) -> u64 {
        self.in_flight_estimated_bytes.saturating_add(
            self.in_flight_unestimated
                .saturating_mul(unmeasured_claim_bytes),
        )
    }
}

/// Every lane of `workspace` that holds a non-terminal admission row.
pub async fn lane_occupancy(db: &LocalDb, workspace: &str) -> DbResult<Vec<LaneOccupancy>> {
    let rows = db
        .query_all(
            "SELECT resource_lane,state,claim_json FROM runtime_admission_requests WHERE workspace_key=?1 AND state IN ('queued','leased','starting','active') ORDER BY resource_lane,enqueue_seq",
            (workspace.to_string(),),
            |row| Ok((row.text(0)?, row.text(1)?, row.text(2)?)),
        )
        .await?;
    let mut lanes: Vec<LaneOccupancy> = Vec::new();
    let mut resident_servers: HashSet<(String, String)> = HashSet::new();
    for (lane, state, claim_json) in rows {
        let claim: RuntimeResourceClaim = decode_json(&claim_json, "runtime resource claim")?;
        if lanes.last().is_none_or(|last| last.lane != lane) {
            lanes.push(LaneOccupancy {
                lane: lane.clone(),
                ..LaneOccupancy::default()
            });
        }
        let entry = lanes.last_mut().expect("lane pushed above");
        entry.typical_claim_bytes = entry.typical_claim_bytes.max(claim.estimated_memory_bytes);
        if state == "queued" {
            entry.queued += 1;
            continue;
        }
        let spawning = matches!(state.as_str(), "leased" | "starting");
        if spawning {
            entry.in_flight_requests += 1;
        }
        let new_process = claim.resident_process_units > 0
            && claim
                .server_instance_key
                .as_ref()
                .is_none_or(|key| resident_servers.insert((lane.clone(), key.clone())));
        if new_process {
            entry.active_units += u64::from(claim.resident_process_units);
            if spawning {
                match claim.estimated_memory_bytes {
                    Some(bytes) => {
                        entry.in_flight_estimated_bytes =
                            entry.in_flight_estimated_bytes.saturating_add(bytes)
                    }
                    None => entry.in_flight_unestimated += 1,
                }
            }
        }
    }
    Ok(lanes)
}

/// What a private lane admits against and where it stands, as one projection:
/// the units it holds, the optional operator ceiling, the measured memory
/// envelope with its provenance, and why the head is waiting when it is.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LaneBudget {
    pub lane: String,
    pub active_units: u64,
    pub queued: u64,
    pub in_flight_bytes: u64,
    /// Explicit operator ceiling; absent means memory alone bounds the lane.
    pub ceiling: Option<u32>,
    /// The envelope the last promotion pass decided against; absent when the
    /// host could not be measured (or no pass has run yet).
    pub memory: Option<HostMemoryEnvelope>,
    /// Unix seconds of that measurement.
    pub measured_at: Option<i64>,
    /// Per-process estimate used to express headroom as units.
    pub per_process_bytes: u64,
    /// Why the queue head is waiting, as of the last promotion pass.
    pub shortfall: Option<LaneShortfall>,
}

impl LaneBudget {
    pub fn headroom_bytes(&self) -> Option<u64> {
        self.memory
            .map(|memory| memory.headroom(self.in_flight_bytes))
    }

    /// Resident units the lane could hold right now: those it holds plus the
    /// measured headroom expressed in per-process estimates, never above the
    /// operator ceiling. Unbounded only when nothing bounds it.
    pub fn resident_process_units(&self) -> u32 {
        let measured = self.headroom_bytes().map(|headroom| {
            self.active_units
                .saturating_add(headroom / self.per_process_bytes.max(1))
        });
        let units = match (measured, self.ceiling) {
            (Some(units), Some(ceiling)) => units.min(u64::from(ceiling)),
            (Some(units), None) => units,
            (None, Some(ceiling)) => u64::from(ceiling),
            (None, None) => u64::from(u32::MAX),
        };
        u32::try_from(units).unwrap_or(u32::MAX)
    }

    pub fn capacity(&self) -> RuntimeResourceCapacity {
        RuntimeResourceCapacity {
            resident_process_units: self.resident_process_units(),
            logical_stream_units: u32::MAX,
        }
    }
}

/// A team mirror's state is this runner's local launch phase: journaling the
/// lease moves it to `leased` in the same write, and start advances it through
/// `starting` to `active` at registration, so team and private rows read alike.
/// A team mirror still `queued` with a journaled lease was leased by a build
/// that kept no local phase; whether its process registered is unknown, so it
/// is treated like `active` and gets no cover.
const AWAITING_LAUNCH: &str = "(r.state IN ('leased','starting') OR (r.state='queued' AND NOT EXISTS (SELECT 1 FROM team_ordinary_admission_authority a WHERE a.request_id=r.request_id)))";

/// Run ids whose ordinary launch is durably between enqueue and process
/// registration: `queued`, `leased` or `starting`, private or team. These are
/// launch claims as surely as the in-memory one is, and they hold through the
/// handoff to the promoter, which takes the job's launch lock only later. An
/// `active` row is deliberately excluded: it means a process registered, so a
/// run holding one with no process handle really is stale.
pub async fn pending_launch_run_ids(db: &LocalDb) -> DbResult<HashSet<String>> {
    Ok(db
        .query_all(
            format!("SELECT p.run_id FROM prepared_ordinary_launches p JOIN runtime_admission_requests r ON r.request_id=p.request_id WHERE {AWAITING_LAUNCH}"),
            (),
            |row| row.text(0),
        )
        .await?
        .into_iter()
        .collect())
}

/// A queued request withdrawn because its run settled first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WithdrawnAdmission {
    pub request_id: String,
    pub workspace_key: String,
    pub resource_lane: String,
    /// The turn the withdrawn launch would have served. Its attention delivery
    /// was stamped when the launch queued and is now owed back to the pending
    /// queue (`cairn_attention::push::return_withdrawn_delivery_conn`).
    pub turn_id: Option<String>,
}

/// Cancel the still-`queued` ordinary request of `run_id` inside the caller's
/// write, so settling a run and withdrawing its queue entry are one fact.
/// Callers return the withdrawn turn's attention delivery in the same write:
/// a launch withdrawn before admission never reached a process.
///
/// Only `queued` rows are touched and no lease fence applies, because a queued
/// row has no owner. A `leased`/`starting` row belongs to its promoter, which
/// cancels it on its own launchability check; an `active` row belongs to the
/// process claim. A team replica carries no admission table (the rows are
/// runner-local), so there this is a no-op.
pub async fn withdraw_queued_ordinary_admission(
    conn: &crate::turso::Connection,
    run_id: &str,
    detail: &str,
    now: i64,
) -> DbResult<Option<WithdrawnAdmission>> {
    let mut table = conn
        .query(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='runtime_admission_requests'",
            (),
        )
        .await?;
    if table.next().await?.is_none() {
        return Ok(None);
    }
    drop(table);
    let launch_key = format!("ordinary:{run_id}");
    let mut rows = conn
        .query(
            "SELECT r.request_id,r.workspace_key,r.resource_lane,p.turn_id FROM runtime_admission_requests r LEFT JOIN prepared_ordinary_launches p ON p.request_id=r.request_id WHERE r.launch_key=?1 AND r.state='queued'",
            params![launch_key.clone()],
        )
        .await?;
    let Some(row) = rows.next().await? else {
        return Ok(None);
    };
    let withdrawn = WithdrawnAdmission {
        request_id: row.text(0)?,
        workspace_key: row.text(1)?,
        resource_lane: row.text(2)?,
        turn_id: row.opt_text(3)?,
    };
    drop(rows);
    let changed = conn
        .execute(
            "UPDATE runtime_admission_requests SET state='cancelled',failure_detail=?2,updated_at=?3 WHERE request_id=?1 AND state='queued'",
            params![withdrawn.request_id.clone(), detail.to_string(), now],
        )
        .await?;
    Ok((changed == 1).then_some(withdrawn))
}

/// A queued ordinary request and the run it would launch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueuedOrdinaryLaunch {
    pub request_id: String,
    pub workspace_key: String,
    pub run_id: String,
    pub turn_id: String,
}

/// Queued launches not yet leased: private rows in `queued`, and team mirrors
/// with no journaled lease. A journaled team lease belongs to its generation-
/// fenced terminal path, never to a queued cancellation.
pub async fn list_queued_ordinary_launches(db: &LocalDb) -> DbResult<Vec<QueuedOrdinaryLaunch>> {
    db.query_all(
        "SELECT r.request_id,r.workspace_key,p.run_id,p.turn_id FROM runtime_admission_requests r JOIN prepared_ordinary_launches p ON p.request_id=r.request_id WHERE r.state='queued' AND NOT EXISTS (SELECT 1 FROM team_ordinary_admission_authority a WHERE a.request_id=r.request_id) ORDER BY r.enqueue_seq",
        (),
        |row| {
            Ok(QueuedOrdinaryLaunch {
                request_id: row.text(0)?,
                workspace_key: row.text(1)?,
                run_id: row.text(2)?,
                turn_id: row.text(3)?,
            })
        },
    )
    .await
}

#[cfg(test)]
#[tokio::test]
async fn prepared_ordinary_launch_round_trips_and_is_immutable() {
    let db = crate::storage::migrated_test_db("prepared-ordinary-launch.db").await;
    db.execute_script(
            "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES('p','w','P','p','/tmp/p',1,1);
             INSERT INTO executions(id,recipe_id,project_id,status,started_at) VALUES('x','recipe','p','running',1);
             INSERT INTO jobs(id,project_id,execution_id,status,created_at,updated_at) VALUES('j','p','x','running',1,1);
             INSERT INTO sessions(id,job_id,backend,status,account_id,created_at,updated_at) VALUES('s','j','codex','open','account-1',1,1);
             INSERT INTO runs(id,project_id,job_id,status,session_id,created_at,updated_at) VALUES('r','p','j','live','s',1,1);
             INSERT INTO turns(id,session_id,run_id,job_id,sequence,state,start_reason,created_at,updated_at) VALUES('t','s','r','j',1,'running','retry',1,1);
             UPDATE jobs SET current_session_id='s',current_turn_id='t' WHERE id='j';
             INSERT INTO events(id,run_id,session_id,turn_id,sequence,timestamp,event_type,data,created_at) VALUES('e','r','s','t',1,1,'user','{\"role\":\"launch\",\"content\":\"durable prompt\"}',1);",
        ).await.unwrap();
    let launch = PreparedOrdinaryLaunch {
        request_id: "ordinary".into(),
        job_id: "j".into(),
        run_id: "r".into(),
        session_id: "s".into(),
        turn_id: "t".into(),
        execution_id: "x".into(),
        launch_event_id: "e".into(),
        prompt: "durable prompt".into(),
        reason: crate::models::TurnStartReason::Retry,
        session_start: PreparedSessionStart::Fork {
            session_id: "s".into(),
            source_backend_id: "provider-thread".into(),
        },
        model: "gpt-5.6-sol".into(),
        agent_config: serde_json::json!({"id":"builder","tools":["read","write"]}),
        output_schema: Some(serde_json::json!({"type":"object","required":["content"]})),
        runtime_capability: PreparedRuntimeCapability {
            class: PreparedRuntimeClass::DedicatedProcess,
            lane: "codex-process".into(),
            host_requirement: PreparedRuntimeHostRequirement::ResidentOn(
                PreparedRuntimeHostKind::Runner,
            ),
            claim: RuntimeResourceClaim {
                resident_process_units: 1,
                server_instance_key: None,
                logical_stream_units: 1,
                estimated_memory_bytes: Some(1024),
            },
            supports_warm_reuse: true,
        },
        provider_account_id: Some("account-1".into()),
        identity_override: Some(PreparedUserIdentity {
            user_id: "user-1".into(),
            email: "user@example.com".into(),
            name: "Example User".into(),
        }),
        created_at: 2,
    };
    let ordinary_request = NewAdmissionRequest {
        request_id: "ordinary".into(),
        workspace_key: "w".into(),
        resource_lane: "codex-process".into(),
        launch_key: "ordinary:r".into(),
        resource_claim: RuntimeResourceClaim {
            resident_process_units: 1,
            server_instance_key: None,
            logical_stream_units: 1,
            estimated_memory_bytes: Some(1024),
        },
        urgent: false,
        now: 1,
    };
    let admission = enqueue_prepared_ordinary_launch(&db, ordinary_request.clone(), launch.clone())
        .await
        .unwrap();
    assert_eq!(admission.request_id, "ordinary");
    assert_eq!(
        get_prepared_ordinary_launch(&db, &launch.request_id)
            .await
            .unwrap(),
        Some(launch.clone())
    );

    // A duplicate wake may invent a new request id, but the launch fact is
    // idempotent by workspace + launch key and retains one reconstructable row.
    let mut duplicate_request = ordinary_request;
    duplicate_request.request_id = "duplicate-wake".into();
    duplicate_request.now = 3;
    let mut duplicate_launch = launch.clone();
    duplicate_launch.request_id = "duplicate-wake".into();
    let duplicate = enqueue_prepared_ordinary_launch(&db, duplicate_request, duplicate_launch)
        .await
        .unwrap();
    assert_eq!(duplicate.request_id, admission.request_id);
    assert_eq!(duplicate.enqueue_seq, admission.enqueue_seq);
    assert!(get_admission_request(&db, "duplicate-wake")
        .await
        .unwrap()
        .is_none());

    // Cancellation acts on the same atomically persisted identity and keeps it
    // out of promotion even after duplicate wake delivery.
    assert!(cancel_queued_admission(
        &db,
        &admission.request_id,
        Some("job stopped while queued"),
        4,
    )
    .await
    .unwrap());
    assert!(
        lease_next_admission(&db, "w", "codex-process", "runner", 5, 10)
            .await
            .unwrap()
            .is_none()
    );

    persist_team_ordinary_lease(
        &db,
        TeamOrdinaryAuthority {
            request_id: launch.request_id.clone(),
            lease_owner: "runner-1".into(),
            lease_generation: 7,
            lease_host_id: "host-1".into(),
            pending_terminal_action: None,
            pending_terminal_detail: None,
            updated_at: 3,
        },
    )
    .await
    .unwrap();
    assert!(
        !queue_team_ordinary_terminal(&db, &launch.request_id, 6, "released", None, 4,)
            .await
            .unwrap()
    );
    assert!(queue_team_ordinary_terminal(
        &db,
        &launch.request_id,
        7,
        "released",
        Some("process exited"),
        5,
    )
    .await
    .unwrap());
    let pending = list_pending_team_ordinary_terminals(&db).await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].lease_owner, "runner-1");
    assert_eq!(pending[0].lease_host_id, "host-1");
    assert_eq!(pending[0].lease_generation, 7);
    assert_eq!(
        pending[0].pending_terminal_action.as_deref(),
        Some("released")
    );
    assert!(!clear_team_ordinary_terminal(&db, &launch.request_id, 6)
        .await
        .unwrap());
    assert!(clear_team_ordinary_terminal(&db, &launch.request_id, 7)
        .await
        .unwrap());
    assert!(!clear_team_ordinary_terminal(&db, &launch.request_id, 7)
        .await
        .unwrap());

    let mut replacement = launch.clone();
    replacement.model = "different-model".into();
    assert!(persist_prepared_ordinary_launch(&db, replacement)
        .await
        .is_err());
    assert_eq!(
        get_prepared_ordinary_launch(&db, "ordinary")
            .await
            .unwrap()
            .unwrap()
            .model,
        "gpt-5.6-sol"
    );

    let capacity = [LaneBudget {
        lane: "codex-process".into(),
        active_units: 0,
        queued: 1,
        in_flight_bytes: 0,
        ceiling: Some(2),
        memory: None,
        measured_at: None,
        per_process_bytes: 1024,
        shortfall: None,
    }];
    // Terminal requests are history, not queue activity.
    assert!(ordinary_admission_projections(&db, 12, &capacity)
        .await
        .unwrap()
        .is_empty());

    let mut live_launch = launch.clone();
    live_launch.request_id = "ordinary-live".into();
    live_launch.run_id = "r-live".into();
    enqueue_prepared_ordinary_launch(
        &db,
        NewAdmissionRequest {
            request_id: "ordinary-live".into(),
            workspace_key: "w".into(),
            resource_lane: "codex-process".into(),
            launch_key: "ordinary:r-live".into(),
            resource_claim: live_launch.runtime_capability.claim.clone(),
            urgent: false,
            now: 1,
        },
        live_launch,
    )
    .await
    .unwrap();
    let projected = ordinary_admission_projections(&db, 12, &capacity)
        .await
        .unwrap();
    assert_eq!(projected.len(), 1);
    assert_eq!(projected[0].request_id, "ordinary-live");
    let projected = &projected[0];
    assert_eq!(projected.job_id, "j");
    assert_eq!(
        projected.reason,
        Some(RuntimeAdmissionReason::WaitingForRuntimeCapacity)
    );
    assert_eq!(
        (
            projected.position,
            projected.behind_count,
            projected.enqueue_age_ms
        ),
        (Some(1), Some(0), 11_000)
    );
    assert_eq!(
        projected.runtime_class,
        PreparedRuntimeClass::DedicatedProcess
    );
    assert_eq!(
        projected
            .effective_capacity
            .as_ref()
            .unwrap()
            .resident_process_units,
        2
    );
    assert_eq!(
        projected.lane_budget.as_ref().map(|budget| budget.ceiling),
        Some(Some(2))
    );
    assert_eq!(projected.runtime_host, None);
    assert_eq!(
        projected.host_constraint,
        PreparedRuntimeHostRequirement::ResidentOn(PreparedRuntimeHostKind::Runner)
    );
}

/// Fenced lifecycle mutation: stale owners or generations cannot change a replacement lease.
pub async fn transition_admission(
    db: &LocalDb,
    request_id: &str,
    owner: &str,
    generation: i64,
    from: &str,
    to: &str,
    now: i64,
) -> DbResult<bool> {
    let request_id = request_id.to_string();
    let owner = owner.to_string();
    let from = from.to_string();
    let to = to.to_string();
    let changed = db.write(move |conn| {
        let (request_id, owner, from, to) =
            (request_id.clone(), owner.clone(), from.clone(), to.clone());
        Box::pin(async move {
            Ok(conn.execute("UPDATE runtime_admission_requests SET state=?5,updated_at=?6 WHERE request_id=?1 AND lease_owner=?2 AND lease_generation=?3 AND state=?4", params![request_id,owner,generation,from,to,now]).await?)
        })
    }).await?;
    Ok(changed == 1)
}

pub async fn heartbeat_admission(
    db: &LocalDb,
    request_id: &str,
    owner: &str,
    generation: i64,
    now: i64,
    expires_at: i64,
) -> DbResult<bool> {
    let (request_id, owner) = (request_id.to_string(), owner.to_string());
    let changed = db.write(move |conn| { let (request_id, owner) = (request_id.clone(), owner.clone()); Box::pin(async move {
        Ok(conn.execute("UPDATE runtime_admission_requests SET lease_expires_at=?4,updated_at=?5 WHERE request_id=?1 AND lease_owner=?2 AND lease_generation=?3 AND state IN ('leased','starting','active')", params![request_id,owner,generation,expires_at,now]).await?)
    })}).await?;
    Ok(changed == 1)
}

pub async fn cancel_queued_admission(
    db: &LocalDb,
    request_id: &str,
    reason: Option<&str>,
    now: i64,
) -> DbResult<bool> {
    let (request_id, reason) = (request_id.to_string(), reason.map(str::to_string));
    let changed = db.write(move |conn| { let (request_id, reason) = (request_id.clone(), reason.clone()); Box::pin(async move {
        Ok(conn.execute("UPDATE runtime_admission_requests SET state='cancelled',failure_detail=?2,updated_at=?3 WHERE request_id=?1 AND state='queued'", params![request_id,reason,now]).await?)
    })}).await?;
    Ok(changed == 1)
}

#[allow(clippy::too_many_arguments)]
pub async fn finish_admission_by_launch_key(
    db: &LocalDb,
    workspace: &str,
    launch_key: &str,
    owner: &str,
    generation: i64,
    terminal_state: &str,
    detail: Option<&str>,
    now: i64,
) -> DbResult<bool> {
    if !matches!(terminal_state, "released" | "cancelled" | "failed") {
        return Err(DbError::internal(format!(
            "invalid terminal admission state: {terminal_state}"
        )));
    }
    let values = (
        workspace.to_string(),
        launch_key.to_string(),
        owner.to_string(),
        terminal_state.to_string(),
        detail.map(str::to_string),
    );
    let changed = db.write(move |conn| { let (workspace,launch_key,owner,state,detail)=values.clone(); Box::pin(async move {
        Ok(conn.execute("UPDATE runtime_admission_requests SET state=?5,failure_detail=?6,updated_at=?7 WHERE workspace_key=?1 AND launch_key=?2 AND lease_owner=?3 AND lease_generation=?4 AND state IN ('leased','starting','active')", params![workspace,launch_key,owner,generation,state,detail,now]).await?)
    })}).await?;
    Ok(changed == 1)
}

pub async fn reap_expired_admissions(db: &LocalDb, now: i64) -> DbResult<u64> {
    db.write(move |conn| Box::pin(async move {
        Ok(conn.execute("UPDATE runtime_admission_requests SET state='queued',lease_owner=NULL,lease_expires_at=NULL,updated_at=?1 WHERE state IN ('leased','starting','active') AND lease_expires_at < ?1", (now,)).await?)
    })).await
}

pub async fn admission_status(
    db: &LocalDb,
    request_id: &str,
    now: i64,
) -> DbResult<Option<AdmissionStatus>> {
    let Some(request) = get_admission_request(db, request_id).await? else {
        return Ok(None);
    };
    let (position, behind_count) = if request.state == "queued" {
        let ahead = db.query_one("SELECT COUNT(*) FROM runtime_admission_requests WHERE workspace_key=?1 AND resource_lane=?2 AND state='queued' AND enqueue_seq < ?3", params![request.workspace_key.clone(),request.resource_lane.clone(),request.enqueue_seq], |row| row.i64(0)).await?;
        let behind = db.query_one("SELECT COUNT(*) FROM runtime_admission_requests WHERE workspace_key=?1 AND resource_lane=?2 AND state='queued' AND enqueue_seq > ?3", params![request.workspace_key.clone(),request.resource_lane.clone(),request.enqueue_seq], |row| row.i64(0)).await?;
        (Some(ahead + 1), Some(behind))
    } else {
        (None, None)
    };
    let enqueue_age_ms = now.saturating_sub(request.created_at).saturating_mul(1_000);
    Ok(Some(AdmissionStatus {
        request,
        position,
        behind_count,
        enqueue_age_ms,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(id: &str, urgent: bool, now: i64) -> NewAdmissionRequest {
        claimed_request(id, urgent, now, 1, None, 1)
    }

    fn claimed_request(
        id: &str,
        urgent: bool,
        now: i64,
        process_units: u32,
        server_key: Option<&str>,
        stream_units: u32,
    ) -> NewAdmissionRequest {
        NewAdmissionRequest {
            request_id: id.into(),
            workspace_key: "w".into(),
            resource_lane: "process".into(),
            launch_key: id.into(),
            resource_claim: RuntimeResourceClaim {
                resident_process_units: process_units,
                server_instance_key: server_key.map(str::to_string),
                logical_stream_units: stream_units,
                estimated_memory_bytes: None,
            },
            urgent,
            now,
        }
    }

    #[tokio::test]
    async fn prepared_call_payload_survives_reopen_and_is_reconstructable() {
        let db = crate::storage::migrated_test_db("prepared-runtime-call.db").await;
        let admission = enqueue_admission_request(&db, request("one", false, 1))
            .await
            .unwrap();
        let prepared = PreparedRuntimeCall {
            request_id: admission.request_id.clone(),
            run_id: "run-one".into(),
            prepared_json: r#"{"prompt":"persisted"}"#.into(),
        };
        persist_prepared_runtime_call(&db, prepared.clone(), 1)
            .await
            .unwrap();
        assert_eq!(
            get_prepared_runtime_call(&db, &admission.request_id)
                .await
                .unwrap(),
            Some(prepared)
        );
        assert_eq!(
            get_admission_request_by_launch_key(&db, "w", "one")
                .await
                .unwrap()
                .unwrap()
                .request_id,
            admission.request_id
        );
    }

    #[tokio::test]
    async fn expired_generation_is_reaped_and_stale_callbacks_are_fenced() {
        let db = crate::storage::migrated_test_db("admission-reaping.db").await;
        enqueue_admission_request(&db, request("one", false, 1))
            .await
            .unwrap();
        let first = lease_next_admission(&db, "w", "process", "old", 2, 10)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reap_expired_admissions(&db, 11).await.unwrap(), 1);
        let second = lease_next_admission(&db, "w", "process", "new", 12, 20)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(second.lease_generation, first.lease_generation + 1);
        assert!(
            !heartbeat_admission(&db, "one", "old", first.lease_generation, 13, 30)
                .await
                .unwrap()
        );
        assert!(
            heartbeat_admission(&db, "one", "new", second.lease_generation, 13, 30)
                .await
                .unwrap()
        );
        assert!(finish_admission_by_launch_key(
            &db,
            "w",
            "one",
            "new",
            second.lease_generation,
            "released",
            None,
            14
        )
        .await
        .unwrap());
        assert!(!finish_admission_by_launch_key(
            &db,
            "w",
            "one",
            "old",
            first.lease_generation,
            "failed",
            None,
            15
        )
        .await
        .unwrap());
    }

    #[tokio::test]
    async fn runner_restart_requeues_nonexpired_active_generation() {
        let db = crate::storage::migrated_test_db("admission-restart-reclaim.db").await;
        enqueue_admission_request(&db, request("one", false, 1))
            .await
            .unwrap();
        let first = lease_next_admission(&db, "w", "process", "old", 2, 10_000)
            .await
            .unwrap()
            .unwrap();
        assert!(transition_admission(
            &db,
            "one",
            "old",
            first.lease_generation,
            "leased",
            "active",
            3
        )
        .await
        .unwrap());

        assert_eq!(requeue_orphaned_admissions(&db, "w", 4).await.unwrap(), 1);
        let replacement = lease_next_admission(&db, "w", "process", "new", 5, 20_000)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(replacement.lease_generation, first.lease_generation + 1);
        assert!(
            !heartbeat_admission(&db, "one", "old", first.lease_generation, 6, 30_000)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn queued_cancellation_prevents_promotion_and_status_is_one_based() {
        let db = crate::storage::migrated_test_db("admission-cancel.db").await;
        enqueue_admission_request(&db, request("first", false, 1))
            .await
            .unwrap();
        enqueue_admission_request(&db, request("cancelled", false, 2))
            .await
            .unwrap();
        let status = admission_status(&db, "cancelled", 12)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (status.position, status.behind_count, status.enqueue_age_ms),
            (Some(2), Some(0), 10_000)
        );
        assert!(
            cancel_queued_admission(&db, "cancelled", Some("job stopped"), 13)
                .await
                .unwrap()
        );
        assert_eq!(
            lease_next_admission(&db, "w", "process", "runner", 15, 20)
                .await
                .unwrap()
                .unwrap()
                .request_id,
            "first"
        );
        assert!(lease_next_admission(&db, "w", "process", "runner", 15, 20)
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn enqueue_is_idempotent_by_launch_identity() {
        let db = crate::storage::migrated_test_db("admission-idempotency.db").await;
        let first = enqueue_admission_request(&db, request("one", false, 1))
            .await
            .unwrap();
        let mut duplicate = request("two", true, 2);
        duplicate.launch_key = "one".into();
        let second = enqueue_admission_request(&db, duplicate).await.unwrap();
        assert_eq!(second.request_id, first.request_id);
        assert_eq!(second.enqueue_seq, first.enqueue_seq);
    }

    #[tokio::test]
    async fn urgent_burst_is_bounded_and_lease_is_fenced() {
        let db = crate::storage::migrated_test_db("admission-fairness.db").await;
        enqueue_admission_request(&db, request("ordinary", false, 1))
            .await
            .unwrap();
        for n in 1..=4 {
            enqueue_admission_request(&db, request(&format!("urgent-{n}"), true, n + 1))
                .await
                .unwrap();
        }
        let mut leased = Vec::new();
        for n in 0..4 {
            leased.push(
                lease_next_admission(&db, "w", "process", "runner", 10 + n, 100 + n)
                    .await
                    .unwrap()
                    .unwrap(),
            );
        }
        assert_eq!(
            leased
                .iter()
                .map(|r| r.request_id.as_str())
                .collect::<Vec<_>>(),
            vec!["urgent-1", "urgent-2", "urgent-3", "ordinary"]
        );
        let first = &leased[0];
        assert!(!transition_admission(
            &db,
            &first.request_id,
            "runner",
            first.lease_generation + 1,
            "leased",
            "starting",
            20
        )
        .await
        .unwrap());
        assert!(transition_admission(
            &db,
            &first.request_id,
            "runner",
            first.lease_generation,
            "leased",
            "starting",
            20
        )
        .await
        .unwrap());
    }

    #[tokio::test]
    async fn unmetered_stateless_http_proceeds_while_ordinary_dedicated_process_waits() {
        let db = crate::storage::migrated_test_db("admission-capacity.db").await;
        let mut active = claimed_request("claude-active", false, 1, 1, None, 1);
        active.launch_key = "ordinary:run-active".into();
        let mut queued = claimed_request("claude-queued", false, 2, 1, None, 1);
        queued.launch_key = "ordinary:run-queued".into();
        let mut stateless = claimed_request("http-stateless", false, 3, 0, None, 1);
        stateless.launch_key = "ordinary:run-http".into();
        for request in [active, queued, stateless] {
            enqueue_admission_request(&db, request).await.unwrap();
        }

        let capacity = RuntimeResourceCapacity {
            resident_process_units: 1,
            logical_stream_units: u32::MAX,
        };
        let active =
            lease_next_admission_with_capacity(&db, "w", "process", "runner", 4, 30, capacity)
                .await
                .unwrap()
                .unwrap();
        assert_eq!(active.request_id, "claude-active");
        assert!(transition_admission(
            &db,
            &active.request_id,
            "runner",
            active.lease_generation,
            "leased",
            "active",
            5,
        )
        .await
        .unwrap());

        // The older dedicated request cannot consume a second process slot, but
        // that must not head-of-line block an explicitly zero-process HTTP launch.
        let bypass =
            lease_next_admission_with_capacity(&db, "w", "process", "runner", 6, 30, capacity)
                .await
                .unwrap()
                .unwrap();
        assert_eq!(bypass.request_id, "http-stateless");
        assert_eq!(
            admission_status(&db, "claude-queued", 6)
                .await
                .unwrap()
                .unwrap()
                .request
                .state,
            "queued"
        );
    }

    #[tokio::test]
    async fn ordinary_restart_preserves_queued_work_and_replaces_leased_generation() {
        let db = crate::storage::migrated_test_db("ordinary-restart-mixed.db").await;
        let mut queued = request("ordinary-queued", false, 1);
        queued.launch_key = "ordinary:run-queued".into();
        let mut leased = request("ordinary-leased", false, 2);
        leased.launch_key = "ordinary:run-leased".into();
        enqueue_admission_request(&db, queued).await.unwrap();
        enqueue_admission_request(&db, leased).await.unwrap();

        let first = lease_next_admission(&db, "w", "process", "old-host", 3, 30_000)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.request_id, "ordinary-queued");
        assert!(transition_admission(
            &db,
            &first.request_id,
            "old-host",
            first.lease_generation,
            "leased",
            "active",
            4,
        )
        .await
        .unwrap());

        assert_eq!(requeue_orphaned_admissions(&db, "w", 5).await.unwrap(), 1);
        let replacement = lease_next_admission(&db, "w", "process", "new-host", 6, 30_000)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(replacement.request_id, first.request_id);
        assert_eq!(replacement.lease_generation, first.lease_generation + 1);
        assert!(!heartbeat_admission(
            &db,
            &first.request_id,
            "old-host",
            first.lease_generation,
            7,
            30_000,
        )
        .await
        .unwrap());

        let still_queued = admission_status(&db, "ordinary-leased", 7)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(still_queued.request.state, "queued");
    }

    const MIB: u64 = 1024 * 1024;

    fn estimated(id: &str, now: i64, bytes: Option<u64>) -> NewAdmissionRequest {
        let mut request = request(id, false, now);
        request.resource_claim.estimated_memory_bytes = bytes;
        request
    }

    fn unbounded() -> RuntimeResourceCapacity {
        RuntimeResourceCapacity {
            resident_process_units: u32::MAX,
            logical_stream_units: u32::MAX,
        }
    }

    fn envelope(available_mib: u64, reserve_mib: u64) -> Option<HostMemoryEnvelope> {
        Some(HostMemoryEnvelope {
            available_bytes: available_mib * MIB,
            reserve_bytes: reserve_mib * MIB,
            unmeasured_claim_bytes: 1024 * MIB,
        })
    }

    async fn lease_within(
        db: &LocalDb,
        capacity: RuntimeResourceCapacity,
        memory: Option<HostMemoryEnvelope>,
    ) -> LeaseDecision {
        lease_next_admission_within(db, "w", "process", "runner", 10, 300, capacity, memory)
            .await
            .unwrap()
    }

    async fn state_of(db: &LocalDb, id: &str) -> String {
        get_admission_request(db, id).await.unwrap().unwrap().state
    }

    /// The review's mixed-backend burst: both lanes are occupied, and a 1 GiB
    /// Codex spawn is in flight with 1.2 GiB above the reserve. A 450 MiB Claude
    /// spawn must see the Codex reservation, or the two together overrun it.
    #[tokio::test]
    async fn in_flight_memory_is_charged_across_lanes_while_units_stay_lane_local() {
        let db = crate::storage::migrated_test_db("admission-cross-lane-memory.db").await;
        for (id, now, lane, bytes) in [
            ("codex-resident", 1, "codex", 1024 * MIB),
            ("codex-spawn", 2, "codex", 1024 * MIB),
            ("claude-resident", 3, "process", 450 * MIB),
            ("claude-next", 4, "process", 450 * MIB),
        ] {
            let mut request = estimated(id, now, Some(bytes));
            request.resource_lane = lane.into();
            enqueue_admission_request(&db, request).await.unwrap();
        }
        db.execute_script(
            "UPDATE runtime_admission_requests SET state='active' WHERE request_id IN ('codex-resident','claude-resident');
             UPDATE runtime_admission_requests SET state='leased',lease_owner='runner',lease_generation=1 WHERE request_id='codex-spawn';",
        )
        .await
        .unwrap();

        let decision = lease_within(&db, unbounded(), envelope(1_000 + 1_229, 1_000)).await;
        assert!(
            matches!(
                decision,
                LeaseDecision::Blocked {
                    shortfall: LaneShortfall::Memory { in_flight_bytes, .. },
                    ..
                } if in_flight_bytes == 1024 * MIB
            ),
            "the other lane's spawn in flight is charged: {decision:?}"
        );
        assert_eq!(state_of(&db, "claude-next").await, "queued");

        // Units are not shared: a ceiling of 1 per lane still sees one claude unit.
        let decision = lease_within(
            &db,
            RuntimeResourceCapacity {
                resident_process_units: 1,
                logical_stream_units: u32::MAX,
            },
            None,
        )
        .await;
        assert!(
            matches!(
                decision,
                LeaseDecision::Blocked {
                    shortfall: LaneShortfall::Units {
                        active: 1,
                        ceiling: 1
                    },
                    ..
                }
            ),
            "{decision:?}"
        );
    }

    /// A team mirror stays `queued` locally after this runner leases and
    /// launches it. Only a mirror with no journaled lease is awaiting launch.
    #[tokio::test]
    async fn a_leased_team_mirror_is_neither_a_launch_claim_nor_a_queued_launch() {
        let db = crate::storage::migrated_test_db("admission-team-mirror-phase.db").await;
        for run in ["run-waiting", "run-launched"] {
            db.execute_script(&format!(
                "INSERT INTO runtime_admission_requests(request_id,workspace_key,resource_lane,launch_key,claim_json,priority,enqueue_seq,state,lease_generation,created_at,updated_at)
                   VALUES('req-{run}','team:t','lane','ordinary:{run}','{{\"residentProcessUnits\":1,\"logicalStreamUnits\":1}}',0,{seq},'queued',0,1,1);
                 INSERT INTO prepared_ordinary_launches(request_id,job_id,run_id,session_id,turn_id,execution_id,launch_event_id,prompt,reason,session_start_json,model,agent_config_json,runtime_capability_json,created_at)
                   VALUES('req-{run}','j','{run}','s','t-{run}','x','e','p','resume','{{}}','','null','{{}}',1);",
                seq = if run == "run-waiting" { 1 } else { 2 },
            ))
            .await
            .unwrap();
        }
        persist_team_ordinary_lease(
            &db,
            TeamOrdinaryAuthority {
                request_id: "req-run-launched".into(),
                lease_owner: "runner".into(),
                lease_generation: 1,
                lease_host_id: "host".into(),
                pending_terminal_action: None,
                pending_terminal_detail: None,
                updated_at: 1,
            },
        )
        .await
        .unwrap();

        // Leased and journaled, not yet registered: still a launch in flight,
        // but no longer a queued launch a cancellation may withdraw.
        assert_eq!(
            state_of(&db, "req-run-launched").await,
            "leased",
            "journaling the lease moves the mirror's phase in the same write"
        );
        assert_eq!(
            pending_launch_run_ids(&db).await.unwrap(),
            HashSet::from(["run-waiting".to_string(), "run-launched".to_string()])
        );
        assert!(
            advance_team_mirror_phase(&db, "req-run-launched", "starting", 2)
                .await
                .unwrap()
        );
        assert!(
            advance_team_mirror_phase(&db, "req-run-launched", "active", 3)
                .await
                .unwrap()
        );
        // Registered: a missing process is now real staleness.
        assert_eq!(
            pending_launch_run_ids(&db).await.unwrap(),
            HashSet::from(["run-waiting".to_string()])
        );
        assert!(!retire_team_mirror(&db, "req-run-launched", "settled", 4)
            .await
            .unwrap());
        let queued: Vec<_> = list_queued_ordinary_launches(&db)
            .await
            .unwrap()
            .into_iter()
            .map(|launch| launch.run_id)
            .collect();
        assert_eq!(queued, vec!["run-waiting".to_string()]);
    }

    #[tokio::test]
    async fn memory_envelope_admits_while_headroom_holds_and_blocks_past_it() {
        let db = crate::storage::migrated_test_db("admission-memory-envelope.db").await;
        for (id, now) in [("one", 1), ("two", 2), ("three", 3)] {
            enqueue_admission_request(&db, estimated(id, now, Some(400 * MIB)))
                .await
                .unwrap();
        }
        let mut leased = Vec::new();
        for id in ["one", "two"] {
            let LeaseDecision::Leased(request) =
                lease_within(&db, unbounded(), envelope(2_000, 1_000)).await
            else {
                panic!("{id} fits inside the 1000 MiB of headroom");
            };
            assert_eq!(request.request_id, id);
            leased.push(request);
        }
        // Two 400 MiB spawns are in flight, invisible to the probe: 200 MiB of
        // headroom is left, so the third must wait rather than eat the reserve.
        match lease_within(&db, unbounded(), envelope(2_000, 1_000)).await {
            LeaseDecision::Blocked {
                head_request_id,
                shortfall,
                in_flight_requests,
            } => {
                assert_eq!(head_request_id, "three");
                assert_eq!(in_flight_requests, 2);
                assert_eq!(
                    shortfall,
                    LaneShortfall::Memory {
                        available_bytes: 2_000 * MIB,
                        reserve_bytes: 1_000 * MIB,
                        in_flight_bytes: 800 * MIB,
                        head_estimate_bytes: 400 * MIB,
                    }
                );
            }
            other => panic!("expected a memory shortfall, got {other:?}"),
        }

        // Once both are resident the probe sees them, so only the measured
        // available memory is charged and the third fits again.
        for request in &leased {
            assert!(transition_admission(
                &db,
                &request.request_id,
                "runner",
                request.lease_generation,
                "leased",
                "active",
                11,
            )
            .await
            .unwrap());
        }
        assert!(matches!(
            lease_within(&db, unbounded(), envelope(2_000, 1_000)).await,
            LeaseDecision::Leased(request) if request.request_id == "three"
        ));
        assert!(matches!(
            lease_within(&db, unbounded(), envelope(2_000, 1_000)).await,
            LeaseDecision::Empty
        ));
    }

    #[tokio::test]
    async fn an_undeclared_estimate_is_charged_the_unmeasured_default() {
        let db = crate::storage::migrated_test_db("admission-memory-unestimated.db").await;
        enqueue_admission_request(&db, estimated("first", 1, Some(10 * MIB)))
            .await
            .unwrap();
        enqueue_admission_request(&db, estimated("codex", 2, None))
            .await
            .unwrap();
        assert!(matches!(
            lease_within(&db, unbounded(), envelope(2_000, 1_000)).await,
            LeaseDecision::Leased(_)
        ));
        // 990 MiB of headroom is left; an unknown cost is charged 1 GiB.
        assert!(matches!(
            lease_within(&db, unbounded(), envelope(2_000, 1_000)).await,
            LeaseDecision::Blocked {
                shortfall: LaneShortfall::Memory { head_estimate_bytes, .. },
                ..
            } if head_estimate_bytes == 1024 * MIB
        ));
    }

    #[tokio::test]
    async fn an_empty_lane_admits_one_even_below_the_reserve() {
        let db = crate::storage::migrated_test_db("admission-memory-liveness.db").await;
        enqueue_admission_request(&db, estimated("one", 1, Some(400 * MIB)))
            .await
            .unwrap();
        enqueue_admission_request(&db, estimated("two", 2, Some(400 * MIB)))
            .await
            .unwrap();
        assert!(matches!(
            lease_within(&db, unbounded(), envelope(500, 1_000)).await,
            LeaseDecision::Leased(request) if request.request_id == "one"
        ));
        assert!(matches!(
            lease_within(&db, unbounded(), envelope(500, 1_000)).await,
            LeaseDecision::Blocked { .. }
        ));
    }

    #[tokio::test]
    async fn an_unmeasured_host_admits_on_memory_but_honours_an_explicit_ceiling() {
        let db = crate::storage::migrated_test_db("admission-memory-unmeasured.db").await;
        for (id, now) in [("one", 1), ("two", 2), ("three", 3)] {
            enqueue_admission_request(&db, estimated(id, now, Some(400 * MIB)))
                .await
                .unwrap();
        }
        assert!(matches!(
            lease_within(&db, unbounded(), None).await,
            LeaseDecision::Leased(_)
        ));
        assert!(matches!(
            lease_within(&db, unbounded(), None).await,
            LeaseDecision::Leased(_)
        ));
        let ceiling = RuntimeResourceCapacity {
            resident_process_units: 2,
            logical_stream_units: u32::MAX,
        };
        assert_eq!(
            lease_within(&db, ceiling, None).await,
            LeaseDecision::Blocked {
                head_request_id: "three".into(),
                shortfall: LaneShortfall::Units {
                    active: 2,
                    ceiling: 2
                },
                in_flight_requests: 2,
            }
        );
    }

    #[tokio::test]
    async fn a_memory_blocked_head_is_not_passed_by_a_later_process_claim() {
        let db = crate::storage::migrated_test_db("admission-memory-fifo.db").await;
        enqueue_admission_request(&db, estimated("running", 1, Some(100 * MIB)))
            .await
            .unwrap();
        enqueue_admission_request(&db, estimated("large", 2, Some(900 * MIB)))
            .await
            .unwrap();
        enqueue_admission_request(&db, estimated("small", 3, Some(10 * MIB)))
            .await
            .unwrap();
        let mut http = claimed_request("http", false, 4, 0, None, 1);
        http.resource_claim.estimated_memory_bytes = None;
        enqueue_admission_request(&db, http).await.unwrap();
        assert!(matches!(
            lease_within(&db, unbounded(), envelope(1_500, 1_000)).await,
            LeaseDecision::Leased(request) if request.request_id == "running"
        ));
        // The large head does not fit; the small claim must not jump it, but a
        // zero-process request costs no memory and still proceeds.
        assert!(matches!(
            lease_within(&db, unbounded(), envelope(1_500, 1_000)).await,
            LeaseDecision::Leased(request) if request.request_id == "http"
        ));
        assert_eq!(state_of(&db, "small").await, "queued");
        assert!(matches!(
            lease_within(&db, unbounded(), envelope(1_500, 1_000)).await,
            LeaseDecision::Blocked { head_request_id, .. } if head_request_id == "large"
        ));
    }

    #[tokio::test]
    async fn withdrawal_cancels_only_a_queued_row_and_pending_launches_exclude_active() {
        let db = crate::storage::migrated_test_db("admission-withdraw.db").await;
        db.execute_script(
            "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES('p','w','P','p','/tmp/p',1,1);
             INSERT INTO executions(id,recipe_id,project_id,status,started_at) VALUES('x','recipe','p','running',1);
             INSERT INTO jobs(id,project_id,execution_id,status,created_at,updated_at) VALUES('j','p','x','running',1,1);
             INSERT INTO sessions(id,job_id,backend,status,created_at,updated_at) VALUES('s','j','claude','open',1,1);",
        )
        .await
        .unwrap();
        for (seq, (state, run)) in [
            ("queued", "run-queued"),
            ("leased", "run-leased"),
            ("starting", "run-starting"),
            ("active", "run-active"),
        ]
        .into_iter()
        .enumerate()
        {
            db.execute_script(&format!(
                "INSERT INTO runtime_admission_requests(request_id,workspace_key,resource_lane,launch_key,claim_json,priority,enqueue_seq,state,lease_owner,lease_generation,created_at,updated_at)
                   VALUES('req-{run}','private-runtime','lane','ordinary:{run}','{{\"residentProcessUnits\":1,\"logicalStreamUnits\":1}}',0,{seq},'{state}','owner',1,1,1);
                 INSERT INTO prepared_ordinary_launches(request_id,job_id,run_id,session_id,turn_id,execution_id,launch_event_id,prompt,reason,session_start_json,model,agent_config_json,runtime_capability_json,created_at)
                   VALUES('req-{run}','j','{run}','s','t-{run}','x','e','p','resume','{{}}','','null','{{}}',1);"
            ))
            .await
            .unwrap();
        }
        let pending = pending_launch_run_ids(&db).await.unwrap();
        assert_eq!(
            pending,
            ["run-queued", "run-leased", "run-starting"]
                .into_iter()
                .map(String::from)
                .collect::<HashSet<_>>()
        );

        for run in ["run-queued", "run-leased", "run-starting", "run-active"] {
            let expected = (run == "run-queued").then(|| "req-run-queued".to_string());
            let run = run.to_string();
            let withdrawn = db
                .write(move |conn| {
                    let run = run.clone();
                    Box::pin(async move {
                        withdraw_queued_ordinary_admission(conn, &run, "run settled", 5).await
                    })
                })
                .await
                .unwrap();
            if let Some(withdrawn) = &withdrawn {
                assert_eq!(withdrawn.turn_id.as_deref(), Some("t-run-queued"));
            }
            assert_eq!(withdrawn.map(|withdrawn| withdrawn.request_id), expected);
        }
        assert_eq!(state_of(&db, "req-run-queued").await, "cancelled");
        assert_eq!(state_of(&db, "req-run-leased").await, "leased");
        assert_eq!(state_of(&db, "req-run-starting").await, "starting");
        assert_eq!(state_of(&db, "req-run-active").await, "active");
        assert_eq!(
            get_admission_request(&db, "req-run-queued")
                .await
                .unwrap()
                .unwrap()
                .failure_detail
                .as_deref(),
            Some("run settled")
        );

        let occupancy = lane_occupancy(&db, "private-runtime").await.unwrap();
        assert_eq!(occupancy.len(), 1);
        assert_eq!(occupancy[0].active_units, 3);
        assert_eq!(occupancy[0].in_flight_requests, 2);
        assert_eq!(occupancy[0].in_flight_unestimated, 2);
        assert_eq!(occupancy[0].queued, 0);
    }

    #[test]
    fn lane_budget_expresses_headroom_as_units_under_the_ceiling() {
        let budget = LaneBudget {
            lane: "claude-cli".into(),
            active_units: 9,
            queued: 0,
            in_flight_bytes: 0,
            ceiling: None,
            memory: envelope(13_000, 1_000),
            measured_at: Some(1),
            per_process_bytes: 450 * MIB,
            shortfall: None,
        };
        assert_eq!(budget.resident_process_units(), 9 + 26);
        assert_eq!(
            LaneBudget {
                ceiling: Some(12),
                ..budget.clone()
            }
            .resident_process_units(),
            12
        );
        assert_eq!(
            LaneBudget {
                memory: None,
                ..budget
            }
            .resident_process_units(),
            u32::MAX
        );
    }

    #[tokio::test]
    async fn multi_unit_claim_is_charged_at_its_declared_cost() {
        let db = crate::storage::migrated_test_db("admission-multi-unit.db").await;
        enqueue_admission_request(&db, claimed_request("large", false, 1, 2, None, 3))
            .await
            .unwrap();
        let insufficient = RuntimeResourceCapacity {
            resident_process_units: 1,
            logical_stream_units: 3,
        };
        assert!(lease_next_admission_with_capacity(
            &db,
            "w",
            "process",
            "runner",
            2,
            30,
            insufficient,
        )
        .await
        .unwrap()
        .is_none());
    }

    #[tokio::test]
    async fn pooled_server_reuse_deduplicates_process_but_charges_each_stream() {
        let db = crate::storage::migrated_test_db("admission-pooled-reuse.db").await;
        for (id, now) in [("first", 1), ("second", 2), ("third", 3)] {
            enqueue_admission_request(&db, claimed_request(id, false, now, 1, Some("pool-a"), 1))
                .await
                .unwrap();
        }
        let capacity = RuntimeResourceCapacity {
            resident_process_units: 1,
            logical_stream_units: 2,
        };
        for expected in ["first", "second"] {
            let leased =
                lease_next_admission_with_capacity(&db, "w", "process", "runner", 10, 30, capacity)
                    .await
                    .unwrap()
                    .unwrap();
            assert_eq!(leased.request_id, expected);
        }
        assert!(lease_next_admission_with_capacity(
            &db, "w", "process", "runner", 11, 30, capacity,
        )
        .await
        .unwrap()
        .is_none());
    }
}
