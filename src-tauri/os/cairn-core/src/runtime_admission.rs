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

fn team_projection_from_status(
    mut local: crate::storage::OrdinaryAdmissionProjection,
    status: AdmissionStatus,
) -> Result<crate::storage::OrdinaryAdmissionProjection, AdmissionClientError> {
    local.state = serde_json::from_value(Value::String(status.request.state)).map_err(|error| {
        AdmissionClientError::InvalidResponse(format!("invalid team admission state: {error}"))
    })?;
    local.reason = status
        .reason
        .map(|reason| {
            serde_json::from_value(Value::String(reason)).map_err(|error| {
                AdmissionClientError::InvalidResponse(format!(
                    "invalid team admission reason: {error}"
                ))
            })
        })
        .transpose()?;
    local.position = status.position;
    local.behind_count = status.behind_count;
    local.enqueue_age_ms = status.enqueue_age_ms;
    local.runtime_class = serde_json::from_value(Value::String(status.request.runtime_class))
        .map_err(|error| {
            AdmissionClientError::InvalidResponse(format!("invalid team runtime class: {error}"))
        })?;
    local.effective_claim = Some(serde_json::from_value(status.effective_claim).map_err(
        |error| {
            AdmissionClientError::InvalidResponse(format!("invalid team effective claim: {error}"))
        },
    )?);
    local.effective_capacity = Some(
        serde_json::from_value(status.effective_capacity.ok_or_else(|| {
            AdmissionClientError::InvalidResponse("team status omitted effective capacity".into())
        })?)
        .map_err(|error| {
            AdmissionClientError::InvalidResponse(format!(
                "invalid team effective capacity: {error}"
            ))
        })?,
    );
    local.runtime_host = status
        .runtime_host
        .map(|id| crate::storage::RuntimeHostIdentity {
            kind: crate::storage::PreparedRuntimeHostKind::Runner,
            id,
        });
    local.host_constraint = match status.host_constraint.as_deref() {
        Some("runner_host") => crate::storage::PreparedRuntimeHostRequirement::ResidentOn(
            crate::storage::PreparedRuntimeHostKind::Runner,
        ),
        None => crate::storage::PreparedRuntimeHostRequirement::None,
        Some(value) => {
            return Err(AdmissionClientError::InvalidResponse(format!(
                "invalid team host constraint: {value}"
            )))
        }
    };
    Ok(local)
}

pub(crate) fn authoritative_team_projection(
    local: crate::storage::OrdinaryAdmissionProjection,
    status: Result<AdmissionStatus, AdmissionClientError>,
) -> Option<crate::storage::OrdinaryAdmissionProjection> {
    match status.and_then(|status| team_projection_from_status(local.clone(), status)) {
        Ok(projection) => Some(projection),
        Err(error) => {
            log::warn!("team ordinary admission projection unavailable: {error}");
            // The immutable local row proves that this launch is pending, but it
            // is not authority for rank, capacity, claim pricing, or host. Keep
            // the activity visible while making every unavailable field explicit.
            let mut pending = local;
            pending.state = crate::storage::RuntimeAdmissionState::Queued;
            pending.reason =
                Some(crate::storage::RuntimeAdmissionReason::AdmissionAuthorityUnavailable);
            pending.position = None;
            pending.behind_count = None;
            pending.effective_claim = None;
            pending.effective_capacity = None;
            pending.runtime_host = None;
            Some(pending)
        }
    }
}

/// Compose the ordinary snapshot at the authority boundary. Team shadow rows
/// supply immutable job/run identity only; mutable admission fields come from
/// Postgres, and unavailable team status is omitted rather than fabricated.
pub async fn ordinary_admission_snapshot(
    db: &LocalDb,
    now: i64,
    private_budgets: &[crate::storage::LaneBudget],
    team_client: Option<&dyn RuntimeAdmissionClient>,
) -> crate::storage::DbResult<Vec<crate::storage::OrdinaryAdmissionProjection>> {
    let local = crate::storage::ordinary_admission_projections(db, now, private_budgets).await?;
    let mut result = Vec::with_capacity(local.len());
    for projection in local {
        if !projection.workspace_key.starts_with("team:") {
            result.push(projection);
            continue;
        }
        let Some(client) = team_client else { continue };
        let request_id = projection.request_id.clone();
        if let Some(projection) =
            authoritative_team_projection(projection, client.status(&request_id).await)
        {
            result.push(projection);
        }
    }
    Ok(result)
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
    pub launch_key: String,
    pub workspace_id: String,
    pub backend_lane: String,
    pub runtime_class: String,
    pub resource_claim: Value,
    pub priority: AdmissionPriority,
    pub state: String,
    #[serde(default)]
    pub lease_generation: i64,
    #[serde(default)]
    pub lease_owner: Option<String>,
    #[serde(default)]
    pub lease_runner_id: Option<String>,
    #[serde(default)]
    pub lease_host_id: Option<String>,
    #[serde(default)]
    pub lease_expires_at: Option<String>,
    #[serde(default)]
    pub admission_policy_revision: Option<i64>,
    #[serde(default)]
    pub admission_capacity: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AdmissionStatus {
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
    async fn reap(&self) -> Result<Vec<AdmissionRequest>, AdmissionClientError>;
}

/// HTTP implementation for team workspaces. The credential is the scoped token
/// returned by `POST /runners/register`, not a device or organization token.
pub struct TeamAdmissionClient {
    http: reqwest::Client,
    api: ApiConfig,
    runner_token: String,
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
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|error| AdmissionClientError::Unavailable(error.to_string()))?;
        Ok(Self {
            http,
            api,
            runner_token,
        })
    }

    async fn send<T: DeserializeOwned>(
        &self,
        method: Method,
        url: String,
        body: Option<Value>,
    ) -> Result<Option<T>, AdmissionClientError> {
        let mut request = self
            .http
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

    fn projection() -> crate::storage::OrdinaryAdmissionProjection {
        crate::storage::OrdinaryAdmissionProjection {
            workspace_key: "team:one".into(),
            request_id: "req".into(),
            job_id: "job".into(),
            run_id: "run".into(),
            state: crate::storage::RuntimeAdmissionState::Queued,
            reason: Some(crate::storage::RuntimeAdmissionReason::WaitingForRuntimeCapacity),
            position: Some(99),
            behind_count: Some(98),
            enqueue_age_ms: 1,
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
            lane_budget: None,
            runtime_host: None,
            host_constraint: crate::storage::PreparedRuntimeHostRequirement::None,
        }
    }

    fn status(state: &str) -> AdmissionStatus {
        AdmissionStatus {
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
        let value = authoritative_team_projection(projection(), Ok(status("queued"))).unwrap();
        assert_eq!(
            (value.position, value.behind_count, value.enqueue_age_ms),
            (Some(2), Some(4), 12345)
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
        let value = authoritative_team_projection(projection(), Ok(status("active"))).unwrap();
        assert_eq!(value.state, crate::storage::RuntimeAdmissionState::Active);
        assert_eq!(value.position, None);
        assert_eq!(value.runtime_host.unwrap().id, "host-1");
        assert_eq!(
            value.host_constraint,
            crate::storage::PreparedRuntimeHostRequirement::ResidentOn(
                crate::storage::PreparedRuntimeHostKind::Runner
            )
        );
    }

    #[test]
    fn team_outage_keeps_pending_work_visible_without_fabricated_authority() {
        let value = authoritative_team_projection(
            projection(),
            Err(AdmissionClientError::Unavailable("outage".into())),
        )
        .unwrap();
        assert_eq!(value.state, crate::storage::RuntimeAdmissionState::Queued);
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
    fn missing_runner_credential_fails_closed() {
        assert!(matches!(
            TeamAdmissionClient::new("  "),
            Err(AdmissionClientError::Unavailable(_))
        ));
    }
}
