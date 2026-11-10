// Runtime-admission authority and the runner-authenticated team API client.
//
// Team admission is coordinated by the cloud API's transactional Postgres
// repository. Private admission remains local. These authorities are never
// interchangeable: failure to reach team authority is an admission failure,
// not permission to consume uncoordinated local capacity.

pub use crate::storage::RuntimeResourceCapacity;
use async_trait::async_trait;
use reqwest::{Method, StatusCode};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;

use crate::api::ApiConfig;
use crate::storage::LocalDb;

pub const PROVIDER_ADMISSION_PROJECTION: &str = "provider_admission";
pub(crate) const PROVIDER_ADMISSION_KEY: &str = "runner";
pub(crate) type ProviderAdmissionCache =
    crate::orchestrator::generation_cache::CadencedCache<EncodedProviderAdmissionSnapshot>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ProviderAdmissionAuthority {
    pub key: String,
    pub kind: String,
    pub workspace_id: String,
    pub availability: String,
    pub observed_at_unix_ms: Option<i64>,
    pub detail: Option<String>,
}

fn project_retained_warm_state(
    state: Option<crate::storage::RuntimeAdmissionState>,
    warm: bool,
) -> Option<crate::storage::RuntimeAdmissionState> {
    if warm && state == Some(crate::storage::RuntimeAdmissionState::Active) {
        Some(crate::storage::RuntimeAdmissionState::RetainedWarm)
    } else {
        state
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ProviderAdmissionSnapshot {
    pub captured_at_unix_ms: i64,
    pub revision: String,
    pub authorities: Vec<ProviderAdmissionAuthority>,
    pub records: Vec<crate::storage::ProviderAdmissionRecord>,
    /// The identity this runner stamps on the hosts it runs, so a surface can
    /// tell its own team-admitted processes from another runner's. Fixed for
    /// the life of the process; `None` where the runner has no identity.
    #[serde(default)]
    pub local_runner_id: Option<String>,
}

/// Equality excludes observation clocks: refreshing an unchanged authority must
/// not announce another snapshot just because time passed.
#[derive(Clone)]
pub struct EncodedProviderAdmissionSnapshot {
    pub snapshot: Arc<ProviderAdmissionSnapshot>,
    json: Arc<serde_json::value::RawValue>,
    content: Arc<str>,
}

impl PartialEq for EncodedProviderAdmissionSnapshot {
    fn eq(&self, other: &Self) -> bool {
        self.content == other.content
    }
}

impl EncodedProviderAdmissionSnapshot {
    pub fn json(&self) -> Arc<serde_json::value::RawValue> {
        self.json.clone()
    }

    fn encode(mut snapshot: ProviderAdmissionSnapshot) -> Result<Self, String> {
        use std::hash::{Hash, Hasher};
        let mut content = snapshot.clone();
        content.captured_at_unix_ms = 0;
        content.revision.clear();
        for authority in &mut content.authorities {
            authority.observed_at_unix_ms = None;
        }
        let content = serde_json::to_string(&content).map_err(|e| e.to_string())?;
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        content.hash(&mut hasher);
        snapshot.revision = format!("{:016x}", hasher.finish());
        let json = serde_json::value::to_raw_value(&snapshot).map_err(|e| e.to_string())?;
        Ok(Self {
            snapshot: Arc::new(snapshot),
            json: Arc::from(json),
            content: Arc::from(content),
        })
    }
}

fn team_projection_from_status(
    mut local: crate::storage::ProviderAdmissionRecord,
    status: AdmissionStatus,
) -> Result<crate::storage::ProviderAdmissionRecord, AdmissionClientError> {
    if status.request.id != local.request_id
        || status.request.backend_lane != local.resource_lane
        || Some(status.request.workspace_id.as_str()) != local.workspace_id.strip_prefix("team:")
    {
        return Err(AdmissionClientError::InvalidResponse(
            "team admission identity mismatch".into(),
        ));
    }
    let parse = |error: serde_json::Error| AdmissionClientError::InvalidResponse(error.to_string());
    local.state = Some(serde_json::from_value(Value::String(status.request.state)).map_err(parse)?);
    local.reason = status
        .reason
        .map(|value| serde_json::from_value(Value::String(value)))
        .transpose()
        .map_err(parse)?;
    local.position = status.position;
    local.behind_count = status.behind_count;
    // A runner mirror's local sequence is NOT the team scheduler's sequence.
    local.enqueue_sequence = status.enqueue_sequence;
    local.enqueued_at_unix_ms = status.enqueued_at_unix_ms;
    local.lane_order = status.position.or(status.enqueue_sequence);
    local.runtime_class =
        serde_json::from_value(Value::String(status.request.runtime_class)).map_err(parse)?;
    local.effective_claim = Some(serde_json::from_value(status.effective_claim).map_err(parse)?);
    local.effective_capacity = status
        .effective_capacity
        .map(serde_json::from_value)
        .transpose()
        .map_err(parse)?;
    local.runtime_host = status
        .runtime_host
        .map(|id| crate::storage::RuntimeHostIdentity {
            kind: crate::storage::PreparedRuntimeHostKind::Runner,
            id,
        });
    local.host_constraint = match status.host_constraint.as_deref() {
        Some("runner_host") => Some(crate::storage::PreparedRuntimeHostRequirement::ResidentOn(
            crate::storage::PreparedRuntimeHostKind::Runner,
        )),
        None => None,
        Some(_) => {
            return Err(AdmissionClientError::InvalidResponse(
                "invalid team host constraint".into(),
            ))
        }
    };
    Ok(local)
}

fn unavailable_projection(
    mut local: crate::storage::ProviderAdmissionRecord,
) -> crate::storage::ProviderAdmissionRecord {
    local.state = None;
    local.reason = Some(crate::storage::RuntimeAdmissionReason::AdmissionAuthorityUnavailable);
    local.position = None;
    local.behind_count = None;
    local.lane_order = None;
    local.enqueue_sequence = None;
    local.enqueued_at_unix_ms = None;
    local.effective_claim = None;
    local.effective_capacity = None;
    local.runtime_host = None;
    local.host_constraint = None;
    local
}

async fn team_snapshot_projection(
    client: &dyn RuntimeAdmissionClient,
    workspace: &str,
    group: &[crate::storage::ProviderAdmissionRecord],
) -> Result<Vec<crate::storage::ProviderAdmissionRecord>, AdmissionClientError> {
    let ids: Vec<_> = group
        .iter()
        .map(|record| record.request_id.clone())
        .collect();
    let statuses = tokio::time::timeout(Duration::from_secs(5), client.snapshot(workspace, &ids))
        .await
        .map_err(|_| AdmissionClientError::Unavailable("snapshot deadline exceeded".into()))??;
    if statuses.len() != group.len() {
        return Err(AdmissionClientError::InvalidResponse(
            "incomplete admission snapshot".into(),
        ));
    }
    let mut by_id = std::collections::HashMap::new();
    for status in statuses {
        if by_id.insert(status.request.id.clone(), status).is_some() {
            return Err(AdmissionClientError::InvalidResponse(
                "duplicate admission snapshot identity".into(),
            ));
        }
    }
    group
        .iter()
        .map(|record| {
            let status = by_id.remove(&record.request_id).ok_or_else(|| {
                AdmissionClientError::InvalidResponse("missing admission snapshot identity".into())
            })?;
            team_projection_from_status(record.clone(), status)
        })
        .collect()
}

/// Compose provider facts at the authority boundary. Authority groups fail as a
/// unit: a partial HTTP batch must not present some lane ranks as coherent with
/// unknown siblings. The bounded deadline also prevents one outage starving
/// every subsequent publication.
pub async fn provider_admission_snapshot(
    db: &LocalDb,
    now_ms: i64,
    private_budgets: &[crate::storage::LaneBudget],
    team_client: Option<&dyn RuntimeAdmissionClient>,
) -> crate::storage::DbResult<ProviderAdmissionSnapshot> {
    let local = crate::storage::provider_admission_records(db, private_budgets).await?;
    Ok(compose_provider_admission_snapshot(local, now_ms, team_client).await)
}

async fn compose_provider_admission_snapshot(
    local: Vec<crate::storage::ProviderAdmissionRecord>,
    now_ms: i64,
    team_client: Option<&dyn RuntimeAdmissionClient>,
) -> ProviderAdmissionSnapshot {
    let mut groups = std::collections::BTreeMap::<String, Vec<_>>::new();
    for record in local {
        groups
            .entry(record.workspace_id.clone())
            .or_default()
            .push(record);
    }
    let mut authorities = Vec::new();
    let mut records = Vec::new();
    for (workspace, group) in groups {
        let team = workspace.starts_with("team:");
        let mut available = true;
        let mut resolved = Vec::new();
        if team {
            if let Some(client) = team_client {
                match team_snapshot_projection(client, &workspace[5..], &group).await {
                    Ok(results) => resolved = results,
                    Err(_) => available = false,
                }
            } else {
                available = false;
            }
        } else {
            resolved = group.clone();
        }
        if !available {
            resolved = group.into_iter().map(unavailable_projection).collect();
        }
        authorities.push(ProviderAdmissionAuthority {
            key: if team {
                workspace.clone()
            } else {
                format!("private:{workspace}")
            },
            kind: if team {
                "team".into()
            } else {
                "private".into()
            },
            workspace_id: workspace,
            availability: if available {
                "available".into()
            } else {
                "unavailable".into()
            },
            observed_at_unix_ms: available.then_some(now_ms),
            detail: (!available)
                .then(|| "Provider admission authority unavailable; live state is unknown.".into()),
        });
        records.extend(resolved.into_iter().filter(|record| {
            !matches!(
                record.state,
                Some(
                    crate::storage::RuntimeAdmissionState::Released
                        | crate::storage::RuntimeAdmissionState::Cancelled
                        | crate::storage::RuntimeAdmissionState::Failed
                )
            )
        }));
    }
    records.sort_by(|a, b| {
        (
            &a.authority_key,
            &a.workspace_id,
            &a.resource_lane,
            a.state != Some(crate::storage::RuntimeAdmissionState::Queued),
            a.lane_order,
            &a.request_id,
        )
            .cmp(&(
                &b.authority_key,
                &b.workspace_id,
                &b.resource_lane,
                b.state != Some(crate::storage::RuntimeAdmissionState::Queued),
                b.lane_order,
                &b.request_id,
            ))
    });
    ProviderAdmissionSnapshot {
        captured_at_unix_ms: now_ms,
        revision: String::new(),
        authorities,
        records,
        local_runner_id: None,
    }
}

pub async fn published_provider_admission(
    orch: &crate::orchestrator::Orchestrator,
) -> Result<EncodedProviderAdmissionSnapshot, String> {
    orch.provider_admission_cache
        .get(PROVIDER_ADMISSION_KEY)
        .await
}

/// Rebuild the authority snapshot and hydrate only unresolved immutable owners.
/// The same path serves cold publication and subsequent cache refreshes.
async fn rebuild_provider_admission_snapshot(
    dbs: &crate::db::DbState,
    now_ms: i64,
    private_budgets: &[crate::storage::LaneBudget],
    team_client: Option<&dyn RuntimeAdmissionClient>,
    previous: Option<&ProviderAdmissionSnapshot>,
    owner_query: &mut (impl FnMut(&cairn_common::ids::RouteScope) + Send),
) -> crate::storage::DbResult<ProviderAdmissionSnapshot> {
    use crate::storage::RowExt;
    use cairn_common::ids::{parse_route_scope, RouteScope};
    use std::collections::HashMap;

    let mut snapshot =
        provider_admission_snapshot(&dbs.local, now_ms, private_budgets, team_client).await?;
    let previous: HashMap<_, _> = previous
        .into_iter()
        .flat_map(|snapshot| &snapshot.records)
        .map(|record| (record.request_id.as_str(), record))
        .collect();
    let mut unresolved: HashMap<RouteScope, HashMap<String, Vec<usize>>> = HashMap::new();
    for (index, record) in snapshot.records.iter_mut().enumerate() {
        let mut owner_resolved = false;
        if let Some(old) = previous.get(record.request_id.as_str()).filter(|old| {
            old.authority_key == record.authority_key
                && old.workspace_id == record.workspace_id
                && old.resource_lane == record.resource_lane
                && old.owner.job_id == record.owner.job_id
                && old.owner.run_id == record.owner.run_id
                && old.owner.execution_id == record.owner.execution_id
        }) {
            owner_resolved = old.owner.project_id.is_some();
            record.owner.thread_name = old.owner.thread_name.clone();
            record.owner.project_key = old.owner.project_key.clone();
            if record.owner.project_id.is_none() {
                record.owner.project_id = old.owner.project_id.clone();
                record.owner.issue_id = old.owner.issue_id.clone();
                record.owner.thread_id = old.owner.thread_id.clone();
                record.owner.issue_number = old.owner.issue_number;
                record.owner.node_kind = old.owner.node_kind.clone();
            }
            if record.state.is_none() {
                record.enqueue_sequence = old.enqueue_sequence;
                record.enqueued_at_unix_ms = old.enqueued_at_unix_ms;
            }
        }
        if !owner_resolved {
            if let Some(job_id) = &record.owner.job_id {
                if let Ok(scope) = parse_route_scope(job_id) {
                    // Local projections already know this ID. Only a successful
                    // owner query may mark enrichment resolved for later refreshes.
                    record.owner.project_id = None;
                    unresolved
                        .entry(scope)
                        .or_default()
                        .entry(job_id.clone())
                        .or_default()
                        .push(index);
                }
            }
        }
    }
    for (scope, jobs) in unresolved {
        let db = match &scope {
            RouteScope::Local => Some(dbs.local.clone()),
            RouteScope::Team(team) => dbs.team_db(team).await,
        };
        // A closed replica or an absent row remains unknown and is retried on
        // the next refresh; neither may fall back to private ownership.
        let Some(db) = db else { continue };
        let ids =
            serde_json::to_string(&jobs.keys().collect::<Vec<_>>()).expect("job ids serialize");
        owner_query(&scope);
        let owners = db.query_all(
            "SELECT j.id,j.project_id,j.issue_id,j.thread_id,i.number,j.node_name,t.name,p.key FROM jobs j LEFT JOIN issues i ON i.id=j.issue_id LEFT JOIN threads t ON t.id=j.thread_id LEFT JOIN projects p ON p.id=t.project_id WHERE j.id IN (SELECT value FROM json_each(?1))",
            (ids,),
            |row| Ok((
                row.text(0)?,
                row.opt_text(1)?,
                row.opt_text(2)?,
                row.opt_text(3)?,
                row.opt_i64(4)?.and_then(|number| i32::try_from(number).ok()),
                row.opt_text(5)?.filter(|kind| !kind.is_empty()),
                row.opt_text(6)?,
                row.opt_text(7)?,
            )),
        ).await;
        match owners {
            Ok(owners) => {
                for (
                    job,
                    project,
                    issue,
                    thread,
                    issue_number,
                    node_kind,
                    thread_name,
                    project_key,
                ) in owners
                {
                    for &index in &jobs[&job] {
                        let owner = &mut snapshot.records[index].owner;
                        owner.project_id = project.clone();
                        owner.issue_id = issue.clone();
                        owner.thread_id = thread.clone();
                        owner.issue_number = issue_number;
                        owner.node_kind = node_kind.clone();
                        owner.thread_name = thread_name.clone();
                        owner.project_key = project_key.clone();
                    }
                }
            }
            Err(error) => log::warn!("provider admission owner enrichment for {scope:?}: {error}"),
        }
    }
    Ok(snapshot)
}

pub(crate) fn provider_admission_cache(
    dbs: &Arc<crate::db::DbState>,
    ledger: &Arc<crate::execution::jobs::PrivateLaneLedger>,
    process_state: &Arc<crate::agent_process::process::AgentProcessState>,
    config_dir: std::path::PathBuf,
    boot_at: i64,
    api: ApiConfig,
    emitter: Arc<dyn crate::services::EventEmitter>,
) -> Arc<ProviderAdmissionCache> {
    use crate::orchestrator::generation_cache::{rebuild_with, STATUS_ROLLUP_FLOOR};
    let slot = Arc::new(std::sync::OnceLock::<std::sync::Weak<ProviderAdmissionCache>>::new());
    let weak_dbs = Arc::downgrade(dbs);
    let weak_ledger = Arc::downgrade(ledger);
    let weak_process_state = Arc::downgrade(process_state);
    // The runner credential is process-scoped environment state. Resolve one
    // authority handle with the cache; its requests use the process-wide TLS
    // client rather than loading platform roots on a runtime worker.
    let team_client = crate::api::runner_credential()
        .ok()
        .and_then(|credential| {
            TeamAdmissionClient::with_api(api, credential.token().to_string()).ok()
        })
        .map(Arc::new);
    let refresh_slot = slot.clone();
    let cache = ProviderAdmissionCache::new(
        "provider admission",
        STATUS_ROLLUP_FLOOR,
        rebuild_with(move |_| {
            let (dbs, ledger, process_state, config_dir, team_client, slot) = (
                weak_dbs.clone(),
                weak_ledger.clone(),
                weak_process_state.clone(),
                config_dir.clone(),
                team_client.clone(),
                refresh_slot.clone(),
            );
            async move {
                let dbs = dbs.upgrade().ok_or("provider admission runner gone")?;
                let ledger = ledger.upgrade().ok_or("provider admission ledger gone")?;
                let process_state = process_state
                    .upgrade()
                    .ok_or("provider admission process registry gone")?;
                let budgets = crate::execution::jobs::private_lane_budgets_from(
                    &dbs.local,
                    &ledger,
                    crate::config::settings::load_settings(&config_dir)
                        .private_dedicated_process_capacity,
                    boot_at,
                )
                .await
                .map_err(|e| e.to_string())?;
                let previous = slot
                    .get()
                    .and_then(std::sync::Weak::upgrade)
                    .and_then(|cache| cache.published(PROVIDER_ADMISSION_KEY));
                let mut snapshot = rebuild_provider_admission_snapshot(
                    &dbs,
                    chrono::Utc::now().timestamp_millis(),
                    &budgets,
                    team_client
                        .as_deref()
                        .map(|client| client as &dyn RuntimeAdmissionClient),
                    previous
                        .as_ref()
                        .map(|published| published.snapshot.as_ref()),
                    &mut |_| {},
                )
                .await
                .map_err(|e| e.to_string())?;
                let calls: std::collections::HashMap<_, _> =
                    crate::storage::list_unsettled_call_admissions(&dbs.local)
                        .await
                        .map_err(|e| e.to_string())?
                        .into_iter()
                        .map(|call| (call.request_id, call.prepared_json))
                        .collect();
                let runner_id = std::env::var("CAIRN_RUNNER_ID").ok();
                snapshot.local_runner_id = runner_id.clone();
                for record in &mut snapshot.records {
                    if let Some(prepared_json) = calls.get(&record.request_id) {
                        if let Some((capability, capacity)) =
                            crate::execution::jobs::prepared_call_provider_metadata(
                                prepared_json,
                                record
                                    .effective_claim
                                    .as_ref()
                                    .and_then(|claim| claim.server_instance_key.clone()),
                            )
                        {
                            if capability.lane == record.resource_lane {
                                record.runtime_class = capability.class;
                                record.host_constraint = Some(capability.host_requirement);
                                record.effective_capacity = Some(capacity);
                                record.provider = capability.provider;
                            }
                        }
                    }
                    if !record.authority_key.starts_with("team:")
                        && matches!(
                            record.state,
                            Some(
                                crate::storage::RuntimeAdmissionState::Leased
                                    | crate::storage::RuntimeAdmissionState::Starting
                                    | crate::storage::RuntimeAdmissionState::Active
                            )
                        )
                    {
                        record.runtime_host =
                            runner_id
                                .clone()
                                .map(|id| crate::storage::RuntimeHostIdentity {
                                    kind: crate::storage::PreparedRuntimeHostKind::Runner,
                                    id,
                                });
                    }
                    let warm = record
                        .owner
                        .run_id
                        .as_deref()
                        .is_some_and(|run_id| process_state.is_warm(run_id));
                    record.state = project_retained_warm_state(record.state, warm);
                }
                // Team state can move outside this runner. Queued private work
                // also depends on measured memory and the boot-ramp deadline.
                if snapshot.records.iter().any(|record| {
                    record.authority_key.starts_with("team:")
                        || matches!(
                            record.state,
                            Some(
                                crate::storage::RuntimeAdmissionState::Queued
                                    | crate::storage::RuntimeAdmissionState::Active
                                    | crate::storage::RuntimeAdmissionState::RetainedWarm
                            )
                        )
                }) {
                    if let Some(cache) = slot.get().and_then(std::sync::Weak::upgrade) {
                        cache.invalidate_after(
                            PROVIDER_ADMISSION_KEY,
                            Duration::from_secs(5),
                            "provider-authority-refresh",
                        );
                    }
                }
                EncodedProviderAdmissionSnapshot::encode(snapshot)
            }
        }),
        Arc::new(move |_| {
            let _ = emitter.emit(
                "db-change",
                crate::notify::projection_published_db_change(
                    PROVIDER_ADMISSION_PROJECTION,
                    PROVIDER_ADMISSION_KEY,
                ),
            );
        }),
    );
    let _ = slot.set(Arc::downgrade(&cache));
    cache
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkspaceAdmissionAuthority {
    Private,
    Team,
}

/// Private-workspace implementation of the same authority contract used by
/// team admission. Launch code selects this adapter once before enqueueing;
/// promotion therefore cannot silently switch authorities after a failure.
pub struct LocalAdmissionClient {
    db: Arc<LocalDb>,
    capacity: RuntimeResourceCapacity,
}

impl LocalAdmissionClient {
    pub fn new(db: Arc<LocalDb>, capacity: RuntimeResourceCapacity) -> Self {
        Self { db, capacity }
    }

    fn now() -> i64 {
        chrono::Utc::now().timestamp()
    }

    fn map_request(request: crate::storage::AdmissionRequest) -> AdmissionRequest {
        let runtime_class = if request.resource_claim.resident_process_units == 0 {
            "stateless_http"
        } else if request.resource_claim.server_instance_key.is_some() {
            "multiplexed_server"
        } else {
            "dedicated_process"
        };
        AdmissionRequest {
            id: request.request_id,
            launch_key: request.launch_key,
            workspace_id: request.workspace_key,
            backend_lane: request.resource_lane,
            runtime_class: runtime_class.into(),
            resource_claim: serde_json::to_value(request.resource_claim)
                .expect("runtime resource claims are serializable"),
            priority: if request.urgent {
                AdmissionPriority::Urgent
            } else {
                AdmissionPriority::Ordinary
            },
            state: request.state,
            lease_generation: request.lease_generation,
            lease_owner: request.lease_owner,
            lease_runner_id: None,
            lease_host_id: None,
            lease_expires_at: request.lease_expires_at.map(|value| value.to_string()),
            admission_policy_revision: None,
            admission_capacity: None,
        }
    }

    fn unavailable(error: impl std::fmt::Display) -> AdmissionClientError {
        AdmissionClientError::Unavailable(error.to_string())
    }

    async fn load(
        &self,
        request_id: &str,
    ) -> Result<crate::storage::AdmissionRequest, AdmissionClientError> {
        crate::storage::get_admission_request(&self.db, request_id)
            .await
            .map_err(Self::unavailable)?
            .ok_or_else(|| {
                AdmissionClientError::InvalidResponse("local admission request not found".into())
            })
    }
}

#[async_trait]
impl RuntimeAdmissionClient for LocalAdmissionClient {
    async fn enqueue(
        &self,
        request: &EnqueueAdmissionRequest,
    ) -> Result<AdmissionRequest, AdmissionClientError> {
        let resource_claim =
            serde_json::from_value(request.resource_claim.clone()).map_err(|error| {
                AdmissionClientError::InvalidResponse(format!(
                    "invalid local resource claim: {error}"
                ))
            })?;
        crate::storage::enqueue_admission_request(
            &self.db,
            crate::storage::NewAdmissionRequest {
                request_id: request.id.clone(),
                workspace_key: request.workspace_id.clone(),
                resource_lane: request.backend_lane.clone(),
                launch_key: request.launch_key.clone(),
                resource_claim,
                urgent: request.priority == AdmissionPriority::Urgent,
                now: Self::now(),
            },
        )
        .await
        .map(Self::map_request)
        .map_err(Self::unavailable)
    }

    async fn lease(
        &self,
        request: &LeaseAdmissionRequest,
    ) -> Result<Option<AdmissionRequest>, AdmissionClientError> {
        let now = Self::now();
        let lease_seconds = i64::try_from(request.lease_ms.div_ceil(1_000)).unwrap_or(i64::MAX);
        crate::storage::lease_next_admission_with_capacity(
            &self.db,
            &request.workspace_id,
            &request.backend_lane,
            &request.host_id,
            now,
            now.saturating_add(lease_seconds),
            self.capacity,
        )
        .await
        .map(|value| value.map(Self::map_request))
        .map_err(Self::unavailable)
    }

    async fn heartbeat(
        &self,
        request_id: &str,
        request: &HeartbeatAdmissionRequest,
    ) -> Result<AdmissionRequest, AdmissionClientError> {
        let current = self.load(request_id).await?;
        let owner = current.lease_owner.as_deref().ok_or_else(|| {
            AdmissionClientError::InvalidResponse(
                "local admission request has no lease owner".into(),
            )
        })?;
        let now = Self::now();
        let lease_seconds = i64::try_from(request.lease_ms.div_ceil(1_000)).unwrap_or(i64::MAX);
        crate::storage::heartbeat_admission(
            &self.db,
            request_id,
            owner,
            request.generation,
            now,
            now.saturating_add(lease_seconds),
        )
        .await
        .map_err(Self::unavailable)?;
        self.load(request_id).await.map(Self::map_request)
    }

    async fn finish(
        &self,
        request_id: &str,
        request: &FinishAdmissionRequest,
    ) -> Result<AdmissionRequest, AdmissionClientError> {
        let current = self.load(request_id).await?;
        let owner = current.lease_owner.as_deref().ok_or_else(|| {
            AdmissionClientError::InvalidResponse(
                "local admission request has no lease owner".into(),
            )
        })?;
        let terminal = match request.action {
            AdmissionTerminalAction::Released => "released",
            AdmissionTerminalAction::Cancelled => "cancelled",
            AdmissionTerminalAction::Failed => "failed",
        };
        crate::storage::finish_admission_by_launch_key(
            &self.db,
            &current.workspace_key,
            &current.launch_key,
            owner,
            request.generation,
            terminal,
            request.detail.as_deref().or(request.reason.as_deref()),
            Self::now(),
        )
        .await
        .map_err(Self::unavailable)?;
        self.load(request_id).await.map(Self::map_request)
    }

    async fn cancel(
        &self,
        request_id: &str,
        reason: Option<&str>,
    ) -> Result<AdmissionRequest, AdmissionClientError> {
        crate::storage::cancel_queued_admission(&self.db, request_id, reason, Self::now())
            .await
            .map_err(Self::unavailable)?;
        self.load(request_id).await.map(Self::map_request)
    }

    async fn status(&self, request_id: &str) -> Result<AdmissionStatus, AdmissionClientError> {
        let status = crate::storage::admission_status(&self.db, request_id, Self::now())
            .await
            .map_err(Self::unavailable)?
            .ok_or_else(|| {
                AdmissionClientError::InvalidResponse("local admission request not found".into())
            })?;
        let effective_claim = serde_json::to_value(&status.request.resource_claim)
            .map_err(|error| AdmissionClientError::InvalidResponse(error.to_string()))?;
        Ok(AdmissionStatus {
            enqueue_sequence: Some(status.request.enqueue_seq),
            enqueued_at_unix_ms: Some(status.request.created_at.saturating_mul(1_000)),
            request: Self::map_request(status.request),
            reason: None,
            position: status.position,
            behind_count: status.behind_count,
            enqueue_age_ms: status.enqueue_age_ms,
            effective_claim,
            effective_capacity: Some(serde_json::json!({
                "resident_process_units": self.capacity.resident_process_units,
                "logical_stream_units": self.capacity.logical_stream_units,
            })),
            runtime_host: None,
            host_constraint: None,
        })
    }

    async fn reap(&self) -> Result<Vec<AdmissionRequest>, AdmissionClientError> {
        crate::storage::reap_expired_admissions(&self.db, Self::now())
            .await
            .map_err(Self::unavailable)?;
        Ok(Vec::new())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeAdmissionRoute {
    /// Stateless HTTP creates no resident runtime and does not enter a process lane.
    Bypass,
    PrivateLocal,
    TeamPostgres,
}

/// Resolve the singular authority before enqueueing. `stateless_http` is the
/// only class allowed to bypass an unavailable authority.
pub fn admission_route(
    authority: WorkspaceAdmissionAuthority,
    runtime_class: &str,
) -> RuntimeAdmissionRoute {
    if runtime_class == "stateless_http" {
        RuntimeAdmissionRoute::Bypass
    } else {
        match authority {
            WorkspaceAdmissionAuthority::Private => RuntimeAdmissionRoute::PrivateLocal,
            WorkspaceAdmissionAuthority::Team => RuntimeAdmissionRoute::TeamPostgres,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AdmissionPriority {
    Ordinary,
    Urgent,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AdmissionTerminalAction {
    Released,
    Cancelled,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct EnqueueAdmissionRequest {
    pub id: String,
    pub launch_key: String,
    pub workspace_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub job_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    pub backend_lane: String,
    pub runtime_class: String,
    pub resource_claim: Value,
    pub priority: AdmissionPriority,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LeaseAdmissionRequest {
    pub request_id: String,
    pub workspace_id: String,
    pub backend_lane: String,
    pub host_id: String,
    pub lease_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct HeartbeatAdmissionRequest {
    pub generation: i64,
    pub lease_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct FinishAdmissionRequest {
    pub generation: i64,
    pub action: AdmissionTerminalAction,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AdmissionRequest {
    pub id: String,
    #[serde(alias = "launch_key")]
    pub launch_key: String,
    #[serde(alias = "workspace_id")]
    pub workspace_id: String,
    #[serde(alias = "backend_lane")]
    pub backend_lane: String,
    #[serde(alias = "runtime_class")]
    pub runtime_class: String,
    #[serde(alias = "resource_claim")]
    pub resource_claim: Value,
    pub priority: AdmissionPriority,
    pub state: String,
    #[serde(default, alias = "lease_generation")]
    pub lease_generation: i64,
    #[serde(default, alias = "lease_owner")]
    pub lease_owner: Option<String>,
    #[serde(default, alias = "lease_runner_id")]
    pub lease_runner_id: Option<String>,
    #[serde(default, alias = "lease_host_id")]
    pub lease_host_id: Option<String>,
    #[serde(default, alias = "lease_expires_at")]
    pub lease_expires_at: Option<String>,
    #[serde(default, alias = "admission_policy_revision")]
    pub admission_policy_revision: Option<i64>,
    #[serde(default, alias = "admission_capacity")]
    pub admission_capacity: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AdmissionStatus {
    #[serde(default)]
    pub enqueue_sequence: Option<i64>,
    #[serde(default)]
    pub enqueued_at_unix_ms: Option<i64>,
    #[serde(flatten)]
    pub request: AdmissionRequest,
    pub reason: Option<String>,
    pub position: Option<i64>,
    pub behind_count: Option<i64>,
    pub enqueue_age_ms: i64,
    pub effective_claim: Value,
    pub effective_capacity: Option<Value>,
    pub runtime_host: Option<String>,
    pub host_constraint: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum AdmissionClientError {
    #[error("team runtime admission authority is unavailable: {0}")]
    Unavailable(String),
    #[error("team runtime admission rejected the request ({status}): {message}")]
    Rejected { status: StatusCode, message: String },
    #[error("team runtime admission returned an invalid response: {0}")]
    InvalidResponse(String),
}

#[allow(clippy::double_must_use)]
#[async_trait]
pub trait RuntimeAdmissionClient: Send + Sync {
    async fn enqueue(
        &self,
        request: &EnqueueAdmissionRequest,
    ) -> Result<AdmissionRequest, AdmissionClientError>;
    async fn lease(
        &self,
        request: &LeaseAdmissionRequest,
    ) -> Result<Option<AdmissionRequest>, AdmissionClientError>;
    async fn heartbeat(
        &self,
        request_id: &str,
        request: &HeartbeatAdmissionRequest,
    ) -> Result<AdmissionRequest, AdmissionClientError>;
    async fn finish(
        &self,
        request_id: &str,
        request: &FinishAdmissionRequest,
    ) -> Result<AdmissionRequest, AdmissionClientError>;
    async fn cancel(
        &self,
        request_id: &str,
        reason: Option<&str>,
    ) -> Result<AdmissionRequest, AdmissionClientError>;
    async fn status(&self, request_id: &str) -> Result<AdmissionStatus, AdmissionClientError>;
    /// One coherent authority read, never synthesized from individual status calls.
    /// Private projections read LocalDb directly rather than this team transport.
    async fn snapshot(
        &self,
        _workspace_id: &str,
        _request_ids: &[String],
    ) -> Result<Vec<AdmissionStatus>, AdmissionClientError> {
        Err(AdmissionClientError::Unavailable(
            "authority does not support snapshots".into(),
        ))
    }
    async fn reap(&self) -> Result<Vec<AdmissionRequest>, AdmissionClientError>;
}

/// HTTP implementation for team workspaces. The credential is the scoped token
/// returned by `POST /runners/register`, not a device or organization token.
pub struct TeamAdmissionClient {
    api: ApiConfig,
    runner_token: String,
}

/// The HTTP client every admission request goes through, one per process.
/// Admission builds a `TeamAdmissionClient` per decision, including from
/// synchronous boot recovery, so the client is resolved here, on the async
/// request path, where waiting for the root load holds no worker.
async fn admission_http() -> Result<&'static reqwest::Client, AdmissionClientError> {
    static HTTP: std::sync::OnceLock<Result<reqwest::Client, String>> = std::sync::OnceLock::new();
    if HTTP.get().is_none() {
        cairn_tls::ready().await;
    }
    HTTP.get_or_init(|| {
        cairn_tls::client_builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|error| error.to_string())
    })
    .as_ref()
    .map_err(|error| AdmissionClientError::Unavailable(error.clone()))
}

impl TeamAdmissionClient {
    pub fn new(runner_token: impl Into<String>) -> Result<Self, AdmissionClientError> {
        Self::with_api(ApiConfig::default(), runner_token)
    }

    pub fn with_api(
        api: ApiConfig,
        runner_token: impl Into<String>,
    ) -> Result<Self, AdmissionClientError> {
        let runner_token = runner_token.into();
        if runner_token.trim().is_empty() {
            return Err(AdmissionClientError::Unavailable(
                "runner credential is missing".into(),
            ));
        }
        Ok(Self { api, runner_token })
    }

    async fn send<T: DeserializeOwned>(
        &self,
        method: Method,
        url: String,
        body: Option<Value>,
    ) -> Result<Option<T>, AdmissionClientError> {
        let mut request = admission_http()
            .await?
            .request(method, url)
            .bearer_auth(&self.runner_token);
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request
            .send()
            .await
            .map_err(|error| AdmissionClientError::Unavailable(error.to_string()))?;
        if response.status() == StatusCode::NO_CONTENT {
            return Ok(None);
        }
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            let message = serde_json::from_str::<Value>(&body)
                .ok()
                .and_then(|value| {
                    value
                        .get("error")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                })
                .unwrap_or(body);
            return Err(AdmissionClientError::Rejected { status, message });
        }
        response
            .json::<T>()
            .await
            .map(Some)
            .map_err(|error| AdmissionClientError::InvalidResponse(error.to_string()))
    }

    fn request_url(&self, request_id: &str, operation: Option<&str>) -> String {
        let base = self.api.runner_admission_request_url(request_id);
        operation.map_or(base.clone(), |operation| format!("{base}/{operation}"))
    }
}

#[derive(Deserialize)]
struct ReapResponse {
    requeued: usize,
    requests: Vec<AdmissionRequest>,
}

#[async_trait]
impl RuntimeAdmissionClient for TeamAdmissionClient {
    async fn enqueue(
        &self,
        request: &EnqueueAdmissionRequest,
    ) -> Result<AdmissionRequest, AdmissionClientError> {
        self.send(
            Method::POST,
            self.api.runner_admission_url("enqueue"),
            Some(serde_json::to_value(request).unwrap()),
        )
        .await?
        .ok_or_else(|| AdmissionClientError::InvalidResponse("enqueue returned no body".into()))
    }
    async fn lease(
        &self,
        request: &LeaseAdmissionRequest,
    ) -> Result<Option<AdmissionRequest>, AdmissionClientError> {
        self.send(
            Method::POST,
            self.api.runner_admission_url("lease"),
            Some(serde_json::to_value(request).unwrap()),
        )
        .await
    }
    async fn heartbeat(
        &self,
        id: &str,
        request: &HeartbeatAdmissionRequest,
    ) -> Result<AdmissionRequest, AdmissionClientError> {
        self.send(
            Method::PATCH,
            self.request_url(id, Some("heartbeat")),
            Some(serde_json::to_value(request).unwrap()),
        )
        .await?
        .ok_or_else(|| AdmissionClientError::InvalidResponse("heartbeat returned no body".into()))
    }
    async fn finish(
        &self,
        id: &str,
        request: &FinishAdmissionRequest,
    ) -> Result<AdmissionRequest, AdmissionClientError> {
        self.send(
            Method::POST,
            self.request_url(id, Some("finish")),
            Some(serde_json::to_value(request).unwrap()),
        )
        .await?
        .ok_or_else(|| AdmissionClientError::InvalidResponse("finish returned no body".into()))
    }
    async fn cancel(
        &self,
        id: &str,
        reason: Option<&str>,
    ) -> Result<AdmissionRequest, AdmissionClientError> {
        self.send(
            Method::POST,
            self.request_url(id, Some("cancel")),
            Some(serde_json::json!({"reason": reason})),
        )
        .await?
        .ok_or_else(|| AdmissionClientError::InvalidResponse("cancel returned no body".into()))
    }
    async fn snapshot(
        &self,
        workspace_id: &str,
        request_ids: &[String],
    ) -> Result<Vec<AdmissionStatus>, AdmissionClientError> {
        #[derive(Deserialize)]
        struct SnapshotResponse {
            records: Vec<AdmissionStatus>,
        }
        let response: SnapshotResponse = self
            .send(
                Method::POST,
                self.api.runner_admission_url("snapshot"),
                Some(serde_json::json!({"workspaceId": workspace_id, "requestIds": request_ids})),
            )
            .await?
            .ok_or_else(|| {
                AdmissionClientError::InvalidResponse("snapshot returned no body".into())
            })?;
        Ok(response.records)
    }

    async fn status(&self, id: &str) -> Result<AdmissionStatus, AdmissionClientError> {
        self.send(Method::GET, self.request_url(id, None), None)
            .await?
            .ok_or_else(|| AdmissionClientError::InvalidResponse("status returned no body".into()))
    }
    async fn reap(&self) -> Result<Vec<AdmissionRequest>, AdmissionClientError> {
        let response: ReapResponse = self
            .send(Method::POST, self.api.runner_admission_url("reap"), None)
            .await?
            .ok_or_else(|| AdmissionClientError::InvalidResponse("reap returned no body".into()))?;
        if response.requeued != response.requests.len() {
            return Err(AdmissionClientError::InvalidResponse(
                "reap count did not match requests".into(),
            ));
        }
        Ok(response.requests)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn warm_processes_keep_their_request_identity_in_a_distinct_state() {
        let active = Some(crate::storage::RuntimeAdmissionState::Active);
        assert_eq!(
            project_retained_warm_state(active, true),
            Some(crate::storage::RuntimeAdmissionState::RetainedWarm)
        );
        assert_eq!(project_retained_warm_state(active, false), active);
        assert_eq!(
            project_retained_warm_state(Some(crate::storage::RuntimeAdmissionState::Queued), true),
            Some(crate::storage::RuntimeAdmissionState::Queued)
        );
    }

    fn projection() -> crate::storage::ProviderAdmissionRecord {
        crate::storage::ProviderAdmissionRecord {
            authority_key: "team:one".into(),
            workspace_id: "team:one".into(),
            resource_lane: "codex".into(),
            request_id: "req".into(),
            owner: crate::storage::ProviderAdmissionOwner {
                job_id: Some("job".into()),
                run_id: Some("run".into()),
                ..Default::default()
            },
            enqueue_sequence: Some(99),
            enqueued_at_unix_ms: Some(1),
            lane_order: Some(99),
            state: Some(crate::storage::RuntimeAdmissionState::Queued),
            reason: Some(crate::storage::RuntimeAdmissionReason::WaitingForRuntimeCapacity),
            position: Some(99),
            behind_count: Some(98),
            runtime_class: crate::storage::PreparedRuntimeClass::DedicatedProcess,
            effective_claim: Some(
                serde_json::from_value(
                    serde_json::json!({"residentProcessUnits":9,"logicalStreamUnits":9}),
                )
                .unwrap(),
            ),
            effective_capacity: Some(RuntimeResourceCapacity {
                resident_process_units: 9,
                logical_stream_units: 9,
            }),
            runtime_host: None,
            host_constraint: Some(crate::storage::PreparedRuntimeHostRequirement::None),
            session_id: Some("session".into()),
            model: Some("gpt-5.6-sol".into()),
            account_id: Some("account".into()),
            provider: Some("codex".into()),
        }
    }

    fn status(state: &str) -> AdmissionStatus {
        AdmissionStatus {
            enqueue_sequence: Some(7),
            enqueued_at_unix_ms: Some(1_000),
            request: AdmissionRequest {
                id: "req".into(),
                launch_key: "ordinary:run".into(),
                workspace_id: "one".into(),
                backend_lane: "codex".into(),
                runtime_class: "multiplexed_server".into(),
                resource_claim: serde_json::json!({}),
                priority: AdmissionPriority::Ordinary,
                state: state.into(),
                lease_generation: 2,
                lease_owner: None,
                lease_runner_id: None,
                lease_host_id: None,
                lease_expires_at: None,
                admission_policy_revision: Some(4),
                admission_capacity: None,
            },
            reason: (state == "queued").then(|| "waiting_for_runtime_capacity".into()),
            position: (state == "queued").then_some(2),
            behind_count: (state == "queued").then_some(4),
            enqueue_age_ms: 12345,
            effective_claim: serde_json::json!({"residentProcessUnits":1,"serverInstanceKey":"server","logicalStreamUnits":2}),
            effective_capacity: Some(
                serde_json::json!({"residentProcessUnits":3,"logicalStreamUnits":8}),
            ),
            runtime_host: (state == "active").then(|| "host-1".into()),
            host_constraint: (state == "active").then(|| "runner_host".into()),
        }
    }

    #[test]
    fn queued_team_projection_replaces_fabricated_local_values() {
        let value = team_projection_from_status(projection(), status("queued")).unwrap();
        assert_eq!(
            (
                value.position,
                value.behind_count,
                value.enqueued_at_unix_ms
            ),
            (Some(2), Some(4), Some(1_000))
        );
        assert_eq!(
            value.runtime_class,
            crate::storage::PreparedRuntimeClass::MultiplexedServer
        );
        assert_eq!(value.effective_capacity.unwrap().resident_process_units, 3);
        assert_eq!(
            value
                .effective_claim
                .unwrap()
                .server_instance_key
                .as_deref(),
            Some("server")
        );
    }

    #[test]
    fn active_team_projection_uses_authoritative_host_and_constraint() {
        let value = team_projection_from_status(projection(), status("active")).unwrap();
        assert_eq!(
            value.state,
            Some(crate::storage::RuntimeAdmissionState::Active)
        );
        assert_eq!(value.position, None);
        assert_eq!(value.runtime_host.unwrap().id, "host-1");
        assert_eq!(
            value.host_constraint,
            Some(crate::storage::PreparedRuntimeHostRequirement::ResidentOn(
                crate::storage::PreparedRuntimeHostKind::Runner
            ))
        );
    }

    #[test]
    fn team_outage_keeps_pending_work_visible_without_fabricated_authority() {
        let value = unavailable_projection(projection());
        assert_eq!(value.state, None);
        assert_eq!(value.lane_order, None);
        assert_eq!(value.enqueue_sequence, None);
        assert_eq!(value.enqueued_at_unix_ms, None);
        assert_eq!(value.host_constraint, None);
        assert_eq!(value.owner.job_id.as_deref(), Some("job"));
        assert_eq!(
            value.reason,
            Some(crate::storage::RuntimeAdmissionReason::AdmissionAuthorityUnavailable)
        );
        assert_eq!((value.position, value.behind_count), (None, None));
        assert_eq!(
            (value.effective_claim, value.effective_capacity),
            (None, None)
        );
        assert_eq!(value.runtime_host, None);
    }

    #[test]
    fn team_resident_work_never_falls_back_to_local_authority() {
        assert_eq!(
            admission_route(WorkspaceAdmissionAuthority::Team, "dedicated_process"),
            RuntimeAdmissionRoute::TeamPostgres
        );
        assert_eq!(
            admission_route(WorkspaceAdmissionAuthority::Team, "multiplexed_server"),
            RuntimeAdmissionRoute::TeamPostgres
        );
        assert_eq!(
            admission_route(WorkspaceAdmissionAuthority::Private, "dedicated_process"),
            RuntimeAdmissionRoute::PrivateLocal
        );
    }

    #[test]
    fn stateless_http_explicitly_bypasses_both_authorities() {
        assert_eq!(
            admission_route(WorkspaceAdmissionAuthority::Team, "stateless_http"),
            RuntimeAdmissionRoute::Bypass
        );
        assert_eq!(
            admission_route(WorkspaceAdmissionAuthority::Private, "stateless_http"),
            RuntimeAdmissionRoute::Bypass
        );
    }

    #[test]
    fn status_accepts_postgres_column_names_without_losing_identity() {
        let status: AdmissionStatus = serde_json::from_value(serde_json::json!({
            "id":"req","launch_key":"ordinary:run","workspace_id":"one",
            "backend_lane":"codex","runtime_class":"dedicated_process",
            "resource_claim":{"residentProcessUnits":1,"logicalStreamUnits":1},
            "priority":"ordinary","state":"queued","lease_generation":0,
            "enqueue_sequence":7,"enqueued_at_unix_ms":1000,
            "reason":"waiting_for_runtime_capacity","position":2,"behind_count":4,
            "enqueue_age_ms":100,"effective_claim":{"residentProcessUnits":1,"logicalStreamUnits":1},
            "effective_capacity":{"residentProcessUnits":3,"logicalStreamUnits":8},
            "runtime_host":null,"host_constraint":null
        })).unwrap();
        let record = team_projection_from_status(projection(), status).unwrap();
        assert_eq!(record.request_id, "req");
        assert_eq!(record.enqueue_sequence, Some(7));
        assert_eq!(record.enqueued_at_unix_ms, Some(1000));
        assert_eq!(record.lane_order, Some(2));
    }

    #[test]
    fn request_models_match_runner_api_casing() {
        let request = LeaseAdmissionRequest {
            request_id: "request-1".into(),
            workspace_id: "ws".into(),
            backend_lane: "codex".into(),
            host_id: "host".into(),
            lease_ms: 30_000,
        };
        assert_eq!(
            serde_json::to_value(request).unwrap(),
            serde_json::json!({"requestId":"request-1","workspaceId":"ws","backendLane":"codex","hostId":"host","leaseMs":30000})
        );
    }

    #[test]
    fn provider_revision_and_encoding_ignore_observation_clock_only() {
        let snapshot = ProviderAdmissionSnapshot {
            local_runner_id: None,
            captured_at_unix_ms: 1,
            revision: String::new(),
            authorities: vec![ProviderAdmissionAuthority {
                key: "team:one".into(),
                kind: "team".into(),
                workspace_id: "team:one".into(),
                availability: "available".into(),
                observed_at_unix_ms: Some(1),
                detail: None,
            }],
            records: vec![projection()],
        };
        let first = EncodedProviderAdmissionSnapshot::encode(snapshot.clone()).unwrap();
        let mut later = snapshot;
        later.captured_at_unix_ms = 2;
        later.authorities[0].observed_at_unix_ms = Some(2);
        let equal = EncodedProviderAdmissionSnapshot::encode(later.clone()).unwrap();
        assert!(first == equal);
        assert_eq!(first.snapshot.revision, equal.snapshot.revision);
        let decoded: ProviderAdmissionSnapshot = serde_json::from_str(first.json().get()).unwrap();
        assert_eq!(decoded, *first.snapshot);
        later.records[0].state = Some(crate::storage::RuntimeAdmissionState::Active);
        let changed = EncodedProviderAdmissionSnapshot::encode(later).unwrap();
        assert!(changed != first);
        assert_ne!(changed.snapshot.revision, first.snapshot.revision);
    }

    #[tokio::test]
    async fn provider_encoding_excludes_host_budget_telemetry_but_tracks_capacity_and_reasons() {
        use crate::storage::{HostMemoryEnvelope, LaneBudget, LaneShortfall};
        let db = crate::storage::migrated_test_db("provider-budget-envelope.db").await;
        db.execute_script(
            r#"INSERT INTO runtime_admission_requests(request_id,workspace_key,resource_lane,launch_key,claim_json,enqueue_seq,state,created_at,updated_at)
               VALUES('request','private-runtime','codex','call:one','{"residentProcessUnits":1,"logicalStreamUnits":1}',1,'queued',1,1);
               INSERT INTO prepared_runtime_calls(request_id,run_id,prepared_json,created_at)
               VALUES('request','run','{}',1);"#,
        ).await.unwrap();
        let mut budgets = [LaneBudget {
            lane: "codex".into(),
            active_units: 0,
            queued: 1,
            in_flight_bytes: 0,
            ceiling: Some(2),
            memory: None,
            measured_at: None,
            per_process_bytes: 1024,
            shortfall: None,
            boot_ramp: None,
        }];
        let first = EncodedProviderAdmissionSnapshot::encode(
            provider_admission_snapshot(&db, 1, &budgets, None)
                .await
                .unwrap(),
        )
        .unwrap();
        budgets[0].memory = Some(HostMemoryEnvelope {
            available_bytes: 100_000,
            reserve_bytes: 10_000,
            unmeasured_claim_bytes: 1024,
        });
        budgets[0].measured_at = Some(2);
        budgets[0].in_flight_bytes = 2048;
        let refreshed = EncodedProviderAdmissionSnapshot::encode(
            provider_admission_snapshot(&db, 2, &budgets, None)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(first.snapshot.revision, refreshed.snapshot.revision);
        assert!(
            first == refreshed,
            "host telemetry alone must not republish"
        );
        let wire: Value = serde_json::from_str(refreshed.json().get()).unwrap();
        assert_eq!(
            wire["records"][0]["effectiveCapacity"]["residentProcessUnits"],
            2
        );
        for field in [
            "laneBudget",
            "memory",
            "availableBytes",
            "reserveBytes",
            "inFlightBytes",
            "shortfall",
            "measuredAt",
        ] {
            assert!(!refreshed.json().get().contains(field), "{field}");
        }
        budgets[0].shortfall = Some(LaneShortfall::StartupRecovery);
        let recovering = EncodedProviderAdmissionSnapshot::encode(
            provider_admission_snapshot(&db, 3, &budgets, None)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            recovering.snapshot.records[0].reason,
            Some(crate::storage::RuntimeAdmissionReason::WaitingForStartupRecovery)
        );
        assert_ne!(
            refreshed.snapshot.revision, recovering.snapshot.revision,
            "a changed typed reason must publish"
        );
        budgets[0].ceiling = Some(3);
        let changed = EncodedProviderAdmissionSnapshot::encode(
            provider_admission_snapshot(&db, 4, &budgets, None)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_ne!(recovering.snapshot.revision, changed.snapshot.revision);
    }

    #[tokio::test]
    async fn missing_team_client_is_unknown_not_empty() {
        let db = crate::storage::migrated_test_db("provider-outage.db").await;
        crate::storage::enqueue_admission_request(
            &db,
            crate::storage::NewAdmissionRequest {
                request_id: "req".into(),
                workspace_key: "team:one".into(),
                resource_lane: "codex".into(),
                launch_key: "call:run".into(),
                resource_claim: projection().effective_claim.unwrap(),
                urgent: false,
                now: 1,
            },
        )
        .await
        .unwrap();
        crate::storage::persist_prepared_runtime_call(
            &db,
            crate::storage::PreparedRuntimeCall {
                request_id: "req".into(),
                run_id: "run".into(),
                prepared_json:
                    serde_json::json!({"job_id":"job","execution_id":"exec","prompt":"secret"})
                        .to_string(),
            },
            1,
        )
        .await
        .unwrap();
        let snapshot = provider_admission_snapshot(&db, 2_000, &[], None)
            .await
            .unwrap();
        assert_eq!(snapshot.records.len(), 1);
        assert_eq!(snapshot.authorities[0].availability, "unavailable");
        assert_eq!(snapshot.authorities[0].observed_at_unix_ms, None);
        assert_eq!(snapshot.records[0].state, None);
        assert_eq!(snapshot.records[0].owner.job_id.as_deref(), Some("job"));
        assert_eq!(snapshot.records[0].position, None);
        assert_eq!(snapshot.records[0].effective_capacity, None);
        assert!(!serde_json::to_string(&snapshot).unwrap().contains("secret"));
    }

    async fn snapshot_server(
        records: Vec<AdmissionStatus>,
        delay: Duration,
        status_code: StatusCode,
    ) -> (
        TeamAdmissionClient,
        Arc<std::sync::atomic::AtomicUsize>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        use axum::{routing::post, Json, Router};
        use std::sync::atomic::{AtomicUsize, Ordering};
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = calls.clone();
        let app = Router::new().route(
            "/runners/admission/snapshot",
            post(
                move |headers: axum::http::HeaderMap, Json(body): Json<Value>| {
                    let records = records.clone();
                    let counted = counted.clone();
                    async move {
                        counted.fetch_add(1, Ordering::SeqCst);
                        assert_eq!(headers["authorization"], "Bearer test-runner");
                        assert_eq!(body["workspaceId"], "one");
                        assert!(body["requestIds"].as_array().is_some());
                        tokio::time::sleep(delay).await;
                        (status_code, Json(serde_json::json!({"records": records})))
                    }
                },
            ),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, stopped) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = stopped.await;
                })
                .await
                .unwrap();
        });
        let client = TeamAdmissionClient::with_api(
            ApiConfig {
                base_url: format!("http://{address}"),
            },
            "test-runner",
        )
        .unwrap();
        (client, calls, stop)
    }

    #[tokio::test]
    async fn five_hundred_delayed_team_records_use_one_coherent_snapshot() {
        use std::sync::atomic::Ordering;
        let mut local = Vec::new();
        let mut statuses = Vec::new();
        for i in 0..500 {
            let id = format!("request-{i}");
            let mut record = projection();
            record.request_id = id.clone();
            local.push(record);
            let mut value = status("queued");
            value.request.id = id;
            value.position = Some(i + 1);
            value.behind_count = Some(499 - i);
            value.enqueue_sequence = Some(i + 1);
            statuses.push(value);
        }
        // Reversed wire order ensures identity, not array position, drives joining.
        statuses.reverse();
        let (client, calls, stop) =
            snapshot_server(statuses, Duration::from_millis(200), StatusCode::OK).await;
        let snapshot = compose_provider_admission_snapshot(local, 2_000, Some(&client)).await;
        let _ = stop.send(());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(snapshot.authorities[0].availability, "available");
        assert_eq!(snapshot.records.len(), 500);
        for (i, record) in snapshot.records.iter().enumerate() {
            assert_eq!(record.request_id, format!("request-{i}"));
            assert_eq!(
                record.state,
                Some(crate::storage::RuntimeAdmissionState::Queued)
            );
            assert_eq!(record.position, Some(i as i64 + 1));
            assert_eq!(record.behind_count, Some(499 - i as i64));
        }
    }

    #[tokio::test]
    async fn thread_owner_labels_are_enriched_once_and_renames_do_not_republish() {
        use crate::db::DbState;
        use crate::storage::SearchIndex;
        let local = Arc::new(crate::storage::migrated_test_db("provider-thread-owners.db").await);
        let index_dir = tempfile::tempdir().unwrap();
        let dbs = DbState::new(
            local.clone(),
            Arc::new(SearchIndex::open_or_create(index_dir.path()).unwrap()),
        );
        let thread_job = "00000000-0000-4000-8000-000000000001";
        let plain_job = "00000000-0000-4000-8000-000000000002";
        local
            .execute_script(format!(
                "INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at)
             VALUES('project','default','Cairn','cairn','/tmp/cairn',1,1);
             INSERT INTO threads(id,project_id,name,created_at,updated_at)
             VALUES('thread','project','providers',1,1);
             INSERT INTO jobs(id,project_id,thread_id,node_name,status,created_at,updated_at)
             VALUES('{thread_job}','project','thread','thread','running',1,1),
                   ('{plain_job}','project',NULL,'builder','running',1,1);"
            ))
            .await
            .unwrap();
        for (index, job) in [thread_job, plain_job].iter().enumerate() {
            let prepared = serde_json::json!({"job_id": job, "execution_id": "execution"});
            local.execute_script(format!(
                "INSERT INTO runtime_admission_requests(request_id,workspace_key,resource_lane,launch_key,claim_json,enqueue_seq,state,created_at,updated_at)
                 VALUES('request-{index}','private-runtime','codex','call:{index}','{{\"residentProcessUnits\":1,\"logicalStreamUnits\":1}}',{index},'queued',1,1);
                 INSERT INTO prepared_runtime_calls(request_id,run_id,prepared_json,created_at)
                 VALUES('request-{index}','run-{index}','{prepared}',1);"
            )).await.unwrap();
        }
        let seed = provider_admission_snapshot(&local, 500, &[], None)
            .await
            .unwrap();
        assert_eq!(seed.records[0].owner.project_id.as_deref(), Some("project"));
        local
            .execute_script("ALTER TABLE threads RENAME TO unavailable_threads;")
            .await
            .unwrap();
        let mut queries = 0;
        let failed =
            rebuild_provider_admission_snapshot(&dbs, 750, &[], None, None, &mut |_| queries += 1)
                .await
                .unwrap();
        assert_eq!(queries, 1);
        assert_eq!(failed.records[0].owner.thread_name, None);
        assert_eq!(failed.records[0].owner.project_key, None);
        local
            .execute_script("ALTER TABLE unavailable_threads RENAME TO threads;")
            .await
            .unwrap();
        queries = 0;
        let first =
            rebuild_provider_admission_snapshot(&dbs, 1_000, &[], None, Some(&failed), &mut |_| {
                queries += 1
            })
            .await
            .unwrap();
        assert_eq!(queries, 1, "one bulk owner query for cold local records");
        let owner = &first.records[0].owner;
        assert_eq!(owner.thread_name.as_deref(), Some("providers"));
        assert_eq!(owner.project_key.as_deref(), Some("cairn"));
        assert_eq!(first.records[1].owner.thread_name, None);
        assert_eq!(first.records[1].owner.project_key, None);
        local
            .execute_script(
                "UPDATE threads SET name='renamed' WHERE id='thread';
             UPDATE projects SET key='renamed-project' WHERE id='project';",
            )
            .await
            .unwrap();
        queries = 0;
        let refreshed =
            rebuild_provider_admission_snapshot(&dbs, 2_000, &[], None, Some(&first), &mut |_| {
                queries += 1
            })
            .await
            .unwrap();
        assert_eq!(queries, 0, "unchanged records need no owner query");
        let first = EncodedProviderAdmissionSnapshot::encode(first).unwrap();
        let refreshed = EncodedProviderAdmissionSnapshot::encode(refreshed).unwrap();
        assert!(
            first == refreshed,
            "owner label renames alone must not republish"
        );
        let cold = rebuild_provider_admission_snapshot(&dbs, 3_000, &[], None, None, &mut |_| {
            queries += 1
        })
        .await
        .unwrap();
        assert_eq!(queries, 1);
        assert_eq!(
            cold.records[0].owner.thread_name.as_deref(),
            Some("renamed")
        );
        assert_eq!(
            cold.records[0].owner.project_key.as_deref(),
            Some("renamed-project")
        );
        local.execute(
            "UPDATE prepared_runtime_calls SET prepared_json=?1 WHERE request_id='request-0'",
            (serde_json::json!({"job_id": "00000000-0000-4000-8000-000000000003", "execution_id": "execution"}).to_string(),),
        ).await.unwrap();
        let missing =
            rebuild_provider_admission_snapshot(&dbs, 4_000, &[], None, Some(&cold), &mut |_| {})
                .await
                .unwrap();
        assert_eq!(missing.records[0].owner.thread_name, None);
        assert_eq!(missing.records[0].owner.project_key, None);
    }

    #[tokio::test]
    async fn five_hundred_record_cache_rebuild_bounds_owner_queries_by_replica() {
        use crate::db::DbState;
        use crate::orchestrator::generation_cache::rebuild_with;
        use crate::storage::SearchIndex;
        use std::sync::atomic::{AtomicUsize, Ordering};
        let local = Arc::new(crate::storage::migrated_test_db("provider-owners-local.db").await);
        let index_dir = tempfile::tempdir().unwrap();
        let dbs = Arc::new(DbState::new(
            local.clone(),
            Arc::new(SearchIndex::open_or_create(index_dir.path()).unwrap()),
        ));
        for team in ["teamA", "teamB"] {
            let db = Arc::new(
                crate::storage::migrated_test_db(&format!("provider-owners-{team}.db")).await,
            );
            db.execute_script(format!(
                "INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at)
                 VALUES('{team}-project','default','P','p','/tmp/p',1,1);
                 INSERT INTO issues(id,project_id,number,title,created_at,updated_at)
                 VALUES('{team}-issue','{team}-project',1,'Issue',1,1);"
            ))
            .await
            .unwrap();
            let mut sql = String::new();
            for i in 0..500 {
                if (i % 2 == 0) != (team == "teamA") {
                    continue;
                }
                sql.push_str(&format!(
                    "INSERT INTO jobs(id,project_id,issue_id,node_name,status,created_at,updated_at)
                     VALUES('{team}~00000000-0000-4000-8000-{i:012}','{team}-project','{team}-issue','builder','running',1,1);"
                ));
            }
            db.execute_script(&sql).await.unwrap();
            dbs.register_team_db_for_test(team.into(), db).await;
        }
        let mut sql = String::new();
        let mut statuses = Vec::new();
        for i in 0..500 {
            let team = if i % 2 == 0 { "teamA" } else { "teamB" };
            let job = format!("{team}~00000000-0000-4000-8000-{i:012}");
            let prepared =
                serde_json::json!({"job_id": job, "execution_id": "execution"}).to_string();
            sql.push_str(&format!(
                "INSERT INTO runtime_admission_requests(request_id,workspace_key,resource_lane,launch_key,claim_json,enqueue_seq,state,created_at,updated_at)
                 VALUES('request-{i}','team:one','codex','call:{i}','{{\"residentProcessUnits\":1,\"logicalStreamUnits\":1}}',{i},'queued',1,1);
                 INSERT INTO prepared_runtime_calls(request_id,run_id,prepared_json,created_at)
                 VALUES('request-{i}','run-{i}','{prepared}',1);"
            ));
            let mut value = status("queued");
            value.request.id = format!("request-{i}");
            value.position = Some(i + 1);
            value.enqueue_sequence = Some(i);
            statuses.push(value);
        }
        local.execute_script(&sql).await.unwrap();
        let (client, http_calls, stop) =
            snapshot_server(statuses, Duration::ZERO, StatusCode::OK).await;
        let client = Arc::new(client);
        let queries = Arc::new(AtomicUsize::new(0));
        let slot = Arc::new(std::sync::OnceLock::<std::sync::Weak<ProviderAdmissionCache>>::new());
        let cache = ProviderAdmissionCache::new(
            "provider-owner-test",
            Duration::ZERO,
            rebuild_with({
                let (dbs, client, queries, slot) =
                    (dbs.clone(), client.clone(), queries.clone(), slot.clone());
                move |_| {
                    let (dbs, client, queries, slot) =
                        (dbs.clone(), client.clone(), queries.clone(), slot.clone());
                    async move {
                        let previous = slot
                            .get()
                            .and_then(std::sync::Weak::upgrade)
                            .and_then(|cache| cache.published(PROVIDER_ADMISSION_KEY));
                        let snapshot = rebuild_provider_admission_snapshot(
                            &dbs,
                            2_000,
                            &[],
                            Some(client.as_ref()),
                            previous.as_ref().map(|value| value.snapshot.as_ref()),
                            &mut |_| {
                                queries.fetch_add(1, Ordering::SeqCst);
                            },
                        )
                        .await
                        .map_err(|e| e.to_string())?;
                        EncodedProviderAdmissionSnapshot::encode(snapshot)
                    }
                }
            }),
            Arc::new(|_| {}),
        );
        slot.set(Arc::downgrade(&cache)).unwrap();
        let first = cache.get(PROVIDER_ADMISSION_KEY).await.unwrap();
        assert_eq!(
            queries.swap(0, Ordering::SeqCst),
            2,
            "one owner SELECT per replica, not per record"
        );
        assert_eq!(first.snapshot.records.len(), 500);
        for record in &first.snapshot.records {
            let team = record
                .owner
                .job_id
                .as_ref()
                .unwrap()
                .split('~')
                .next()
                .unwrap();
            assert_eq!(record.owner.project_id, Some(format!("{team}-project")));
            assert_eq!(record.owner.issue_id, Some(format!("{team}-issue")));
            assert_eq!(record.owner.issue_number, Some(1));
            assert_eq!(record.owner.node_kind.as_deref(), Some("builder"));
            assert_eq!(record.owner.execution_id.as_deref(), Some("execution"));
        }
        cache.invalidate(PROVIDER_ADMISSION_KEY, "test-refresh");
        cache.settle(PROVIDER_ADMISSION_KEY).await;
        let refreshed = cache.get(PROVIDER_ADMISSION_KEY).await.unwrap();
        assert_eq!(
            queries.load(Ordering::SeqCst),
            0,
            "unchanged immutable owners need no replica queries"
        );
        assert_eq!(
            http_calls.load(Ordering::SeqCst),
            2,
            "refresh still obtains authoritative mutable facts"
        );
        assert_eq!(first.snapshot.records, refreshed.snapshot.records);

        // A mismatched prior owner must not suppress lookup or replace the
        // current reconstruction identity.
        let mut mismatched = (*first.snapshot).clone();
        mismatched.records[0].owner.job_id = Some("unrelated".into());
        mismatched.records[0].owner.project_id = Some("wrong".into());
        let rebuilt = rebuild_provider_admission_snapshot(
            &dbs,
            3_000,
            &[],
            Some(client.as_ref()),
            Some(&mismatched),
            &mut |_| {
                queries.fetch_add(1, Ordering::SeqCst);
            },
        )
        .await
        .unwrap();
        assert_eq!(queries.swap(0, Ordering::SeqCst), 1);
        assert_eq!(rebuilt.records, first.snapshot.records);

        // Closed replicas stay unknown and misses are retried rather than
        // negatively cached forever.
        let closed_job = "closed~00000000-0000-4000-8000-000000000001";
        local
            .execute(
                "UPDATE prepared_runtime_calls SET prepared_json=?1 WHERE request_id='request-0'",
                (
                    serde_json::json!({"job_id": closed_job, "execution_id": "execution"})
                        .to_string(),
                ),
            )
            .await
            .unwrap();
        let missing = rebuild_provider_admission_snapshot(
            &dbs,
            4_000,
            &[],
            Some(client.as_ref()),
            Some(&first.snapshot),
            &mut |_| {
                queries.fetch_add(1, Ordering::SeqCst);
            },
        )
        .await
        .unwrap();
        assert_eq!(missing.records[0].owner.project_id, None);
        assert_eq!(missing.records[0].owner.thread_name, None);
        assert_eq!(missing.records[0].owner.project_key, None);
        assert_eq!(queries.swap(0, Ordering::SeqCst), 0);
        let reopened =
            Arc::new(crate::storage::migrated_test_db("provider-owners-reopened.db").await);
        reopened
            .execute_script(format!(
                "INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at)
             VALUES('reopened','default','P','reopened','/tmp/reopened',1,1);
             INSERT INTO jobs(id,project_id,status,created_at,updated_at)
             VALUES('{closed_job}','reopened','running',1,1);"
            ))
            .await
            .unwrap();
        dbs.register_team_db_for_test("closed".into(), reopened)
            .await;
        let reopened = rebuild_provider_admission_snapshot(
            &dbs,
            5_000,
            &[],
            Some(client.as_ref()),
            Some(&missing),
            &mut |_| {
                queries.fetch_add(1, Ordering::SeqCst);
            },
        )
        .await
        .unwrap();
        assert_eq!(queries.load(Ordering::SeqCst), 1);
        assert_eq!(
            reopened.records[0].owner.project_id.as_deref(),
            Some("reopened")
        );
        let _ = stop.send(());
    }

    #[tokio::test]
    async fn malformed_or_failed_team_batches_never_publish_partial_authority() {
        let mut wrong_workspace = status("queued");
        wrong_workspace.request.workspace_id = "other".into();
        let mut wrong_identity = status("queued");
        wrong_identity.request.id = "unexpected".into();
        let mut sibling = projection();
        sibling.request_id = "sibling".into();
        for (records, group, code) in [
            (vec![], vec![projection()], StatusCode::OK),
            (vec![wrong_workspace], vec![projection()], StatusCode::OK),
            (vec![wrong_identity], vec![projection()], StatusCode::OK),
            (
                vec![status("queued"), status("queued")],
                vec![projection(), sibling],
                StatusCode::OK,
            ),
            (
                vec![status("queued")],
                vec![projection()],
                StatusCode::SERVICE_UNAVAILABLE,
            ),
        ] {
            let (client, _, stop) = snapshot_server(records, Duration::ZERO, code).await;
            assert!(team_snapshot_projection(&client, "one", &group)
                .await
                .is_err());
            let _ = stop.send(());
        }
    }

    #[tokio::test]
    async fn provider_cache_serves_last_and_announces_only_changed_content() {
        use crate::orchestrator::generation_cache::rebuild_with;
        use std::sync::atomic::{AtomicUsize, Ordering};
        let builds = Arc::new(AtomicUsize::new(0));
        let announcements = Arc::new(AtomicUsize::new(0));
        let cache = ProviderAdmissionCache::new(
            "provider-test",
            Duration::from_millis(5),
            rebuild_with({
                let builds = builds.clone();
                move |_| {
                    let n = builds.fetch_add(1, Ordering::SeqCst);
                    async move {
                        EncodedProviderAdmissionSnapshot::encode(ProviderAdmissionSnapshot {
                            captured_at_unix_ms: n as i64,
                            revision: String::new(),
                            authorities: Vec::new(),
                            records: vec![projection()],
                            local_runner_id: None,
                        })
                    }
                }
            }),
            {
                let announcements = announcements.clone();
                Arc::new(move |_| {
                    announcements.fetch_add(1, Ordering::SeqCst);
                })
            },
        );
        let first = cache.get(PROVIDER_ADMISSION_KEY).await.unwrap();
        let again = cache.get(PROVIDER_ADMISSION_KEY).await.unwrap();
        assert!(
            Arc::ptr_eq(&first.json, &again.json),
            "warm reads neither query nor encode"
        );
        assert_eq!(builds.load(Ordering::SeqCst), 1);
        let announced = announcements.load(Ordering::SeqCst);
        cache.invalidate(PROVIDER_ADMISSION_KEY, "test-commit");
        let stale = cache.get(PROVIDER_ADMISSION_KEY).await.unwrap();
        assert!(
            Arc::ptr_eq(&first.json, &stale.json),
            "dirty read serves the last publication"
        );
        cache.settle(PROVIDER_ADMISSION_KEY).await;
        assert_eq!(builds.load(Ordering::SeqCst), 2);
        assert_eq!(
            announcements.load(Ordering::SeqCst),
            announced,
            "a clock-only refresh does not announce"
        );
    }

    #[test]
    fn missing_runner_credential_fails_closed() {
        assert!(matches!(
            TeamAdmissionClient::new("  "),
            Err(AdmissionClientError::Unavailable(_))
        ));
    }
}
