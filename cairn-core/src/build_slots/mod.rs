//! Runner-side fleet-placement facade for supervised and enrolled executors.
//!
//! Core owns request construction, settings resolution, result correlation, and
//! the cached UI snapshot. Scheduling, workspaces, processes, cancellation, and
//! mutation sealing exist only in the executor process.

use crate::mcp::handlers::run::{ResolvedRunBatch, RunSpec};
use crate::orchestrator::Orchestrator;
use cairn_common::executor_protocol::{
    ExecutorAdvertisement, ExecutorCapabilities, ExecutorConfig, ExecutorIdentity, ExecutorMessage,
    PlacementConstraints, ProcessBatch, ProcessBatchItem, ProcessSandboxMode, RepositoryLocator,
    RunnerCallback, RunnerCallbackResult, MANAGED_OBJECT_REQUEST_TIMEOUT_SECONDS,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::Write;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot};

pub use cairn_common::executor_protocol::{
    ActiveBuildSlotRequest, BuildSlotExecutionMeta, BuildSlotOutcome, BuildSlotPoolSnapshot,
    BuildSlotPriority, BuildSlotRequest, BuildSlotUnavailableReason, MutationDelta, MutationPolicy,
    PersistentBuildSlotState, PersistentSlotLifecycle, QueuedBuildSlotRequest,
};

pub const DEFAULT_PROJECT_SLOT_COUNT: usize = 2;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BuildSlotsConfig {
    #[serde(default)]
    pub projects: HashMap<String, usize>,
    #[serde(default = "default_acquisition_deadline_seconds")]
    pub acquisition_deadline_seconds: u64,
    #[serde(default = "default_timeout_seconds")]
    pub default_timeout_seconds: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub global_capacity: Option<usize>,
}

impl Default for BuildSlotsConfig {
    fn default() -> Self {
        Self {
            projects: HashMap::new(),
            acquisition_deadline_seconds: default_acquisition_deadline_seconds(),
            default_timeout_seconds: default_timeout_seconds(),
            global_capacity: None,
        }
    }
}

fn default_acquisition_deadline_seconds() -> u64 {
    20
}
fn default_timeout_seconds() -> u64 {
    30 * 60
}

impl BuildSlotsConfig {
    pub fn slots_for(&self, project_id: &str) -> usize {
        let configured = self
            .projects
            .get(project_id)
            .copied()
            .unwrap_or(DEFAULT_PROJECT_SLOT_COUNT);
        self.global_capacity
            .map_or(configured, |cap| configured.min(cap))
    }
}

type RequestIdentity = (String, String);
const COLOCATED_EXECUTOR_ID: &str = "colocated";
const MIN_REQUEST_WATCHDOG_SLACK: Duration = Duration::from_millis(100);
const MAX_REQUEST_WATCHDOG_SLACK: Duration = Duration::from_secs(5);

struct PendingResult {
    executor_id: String,
    generation: u64,
    waiter: oneshot::Sender<BuildSlotOutcome>,
}
type PendingResults = HashMap<RequestIdentity, PendingResult>;

#[derive(Clone, Default)]
pub struct BuildSlotPool {
    connections: Arc<Mutex<HashMap<String, ExecutorConnectionState>>>,
    connection_ready: Arc<tokio::sync::Notify>,
    pending: Arc<Mutex<PendingResults>>,
    runner_contexts: Arc<Mutex<HashMap<String, RunnerCallbackContext>>>,
}

#[derive(Clone)]
struct RunnerCallbackContext {
    request: crate::mcp::types::McpCallbackRequest,
    run_context: Option<crate::mcp::handlers::RunContext>,
}

struct ExecutorConnectionState {
    identity: ExecutorIdentity,
    advertisement: ExecutorAdvertisement,
    generation: u64,
    sender: mpsc::UnboundedSender<ExecutorMessage>,
    snapshot: BuildSlotPoolSnapshot,
    colocated: bool,
}

#[derive(Clone, Debug)]
struct SelectedExecutor {
    executor_id: String,
    generation: u64,
    sender: mpsc::UnboundedSender<ExecutorMessage>,
    colocated: bool,
}

struct SubmitDropGuard {
    pool: BuildSlotPool,
    request_id: String,
    attempt_id: String,
    executor_id: String,
    generation: u64,
    armed: bool,
}

impl SubmitDropGuard {
    fn disarm(&mut self) {
        self.armed = false;
    }
}
impl Drop for SubmitDropGuard {
    fn drop(&mut self) {
        if self.armed {
            self.pool
                .pending
                .lock()
                .unwrap()
                .remove(&(self.request_id.clone(), self.attempt_id.clone()));
            let _ = self.pool.send_to(
                &self.executor_id,
                self.generation,
                ExecutorMessage::Cancel {
                    request_id: self.request_id.clone(),
                    attempt_id: self.attempt_id.clone(),
                },
            );
        }
    }
}

impl BuildSlotPool {
    pub fn attach_executor(&self, sender: mpsc::UnboundedSender<ExecutorMessage>) -> u64 {
        let advertisement = ExecutorAdvertisement {
            identity: ExecutorIdentity {
                device_id: "local-device".into(),
                executor_id: COLOCATED_EXECUTOR_ID.into(),
                display_name: "Local executor".into(),
            },
            capabilities: ExecutorCapabilities {
                os: std::env::consts::OS.into(),
                arch: std::env::consts::ARCH.into(),
                logical_cores: 1,
                toolchains: Vec::new(),
                projects_served: Vec::new(),
                slot_capacity: usize::MAX,
                disk_budget_bytes: None,
                memory_budget_bytes: None,
            },
            current_load: 0,
            warm_roots: Vec::new(),
            observed_at_unix_ms: unix_time_ms(),
        };
        self.attach_advertised_executor(advertisement, sender, true)
    }

    pub fn attach_advertised_executor(
        &self,
        advertisement: ExecutorAdvertisement,
        sender: mpsc::UnboundedSender<ExecutorMessage>,
        colocated: bool,
    ) -> u64 {
        let executor_id = advertisement.identity.executor_id.clone();
        let (generation, replaced) = {
            let mut connections = self.connections.lock().unwrap();
            let generation = connections
                .get(&executor_id)
                .map_or(1, |entry| entry.generation.wrapping_add(1).max(1));
            let replaced = connections
                .insert(
                    executor_id.clone(),
                    ExecutorConnectionState {
                        identity: advertisement.identity.clone(),
                        advertisement,
                        generation,
                        sender,
                        snapshot: BuildSlotPoolSnapshot::default(),
                        colocated,
                    },
                )
                .is_some();
            (generation, replaced)
        };
        if replaced {
            self.fail_for_executor(
                &executor_id,
                "executor connection was replaced before returning a result",
            );
        }
        self.connection_ready.notify_waiters();
        generation
    }

    pub fn disconnect_executor(&self, generation: u64) {
        let executor_id = self
            .connections
            .lock()
            .unwrap()
            .iter()
            .find(|(_, entry)| entry.colocated && entry.generation == generation)
            .map(|(id, _)| id.clone());
        if let Some(executor_id) = executor_id {
            self.disconnect_advertised_executor(&executor_id, generation);
        }
    }

    pub fn disconnect_advertised_executor(&self, executor_id: &str, generation: u64) {
        let disconnected = {
            let mut connections = self.connections.lock().unwrap();
            if connections
                .get(executor_id)
                .is_some_and(|entry| entry.generation == generation)
            {
                connections.remove(executor_id);
                true
            } else {
                false
            }
        };
        if disconnected {
            self.fail_for_executor(
                executor_id,
                "executor connection closed before returning a result",
            );
            self.connection_ready.notify_waiters();
        }
    }

    pub fn executor_generation(&self) -> Option<u64> {
        self.connections
            .lock()
            .unwrap()
            .values()
            .find(|entry| entry.colocated && !entry.sender.is_closed())
            .map(|entry| entry.generation)
    }

    pub fn managed_generation(&self, executor_id: &str, device_id: &str) -> Option<u64> {
        self.connections
            .lock()
            .unwrap()
            .get(executor_id)
            .filter(|entry| {
                !entry.colocated
                    && entry.identity.device_id == device_id
                    && !entry.sender.is_closed()
            })
            .map(|entry| entry.generation)
    }

    pub fn shutdown_advertised_executor(
        &self,
        executor_id: &str,
        device_id: &str,
        generation: u64,
    ) -> bool {
        let sender = self
            .connections
            .lock()
            .unwrap()
            .get(executor_id)
            .filter(|entry| entry.generation == generation && entry.identity.device_id == device_id)
            .map(|entry| entry.sender.clone());
        let Some(sender) = sender else { return false };
        let _ = sender.send(ExecutorMessage::Shutdown);
        self.disconnect_advertised_executor(executor_id, generation);
        true
    }

    pub fn shutdown_executor(&self) {
        let targets: Vec<_> = self
            .connections
            .lock()
            .unwrap()
            .values()
            .map(|entry| entry.sender.clone())
            .collect();
        for sender in targets {
            let _ = sender.send(ExecutorMessage::Shutdown);
        }
    }

    pub fn update_advertisement(
        &self,
        executor_id: &str,
        generation: u64,
        advertisement: ExecutorAdvertisement,
    ) -> bool {
        let mut connections = self.connections.lock().unwrap();
        let Some(entry) = connections.get_mut(executor_id) else {
            return false;
        };
        if entry.generation != generation || advertisement.identity != entry.identity {
            return false;
        }
        entry.advertisement = advertisement;
        self.connection_ready.notify_waiters();
        true
    }

    pub fn handle_executor_message(
        &self,
        executor_id: &str,
        generation: u64,
        message: ExecutorMessage,
    ) -> bool {
        if self
            .connections
            .lock()
            .unwrap()
            .get(executor_id)
            .is_none_or(|entry| entry.generation != generation)
        {
            return false;
        }
        match message {
            ExecutorMessage::Result {
                request_id,
                attempt_id,
                mut outcome,
            } => {
                if !outcome_matches(&outcome, &request_id, &attempt_id) {
                    return false;
                }
                let key = (request_id, attempt_id);
                let pending = self.pending.lock().unwrap().remove(&key);
                if let Some(pending) = pending {
                    if pending.executor_id != executor_id || pending.generation != generation {
                        self.pending.lock().unwrap().insert(key, pending);
                        return false;
                    }
                    if let BuildSlotOutcome::Completed { metadata, .. } = &mut outcome {
                        let canonical = self
                            .connections
                            .lock()
                            .unwrap()
                            .get(executor_id)
                            .filter(|connection| connection.generation == generation)
                            .map(|connection| {
                                (
                                    connection.identity.executor_id.clone(),
                                    connection.identity.device_id.clone(),
                                    connection.generation,
                                )
                            });
                        let Some((canonical_id, device_id, canonical_generation)) = canonical
                        else {
                            self.pending.lock().unwrap().insert(key, pending);
                            return false;
                        };
                        metadata.executor_id = canonical_id;
                        metadata.executor_device_id = device_id;
                        metadata.executor_connection_generation = canonical_generation;
                    }
                    let _ = pending.waiter.send(outcome);
                }
                false
            }
            ExecutorMessage::SnapshotResponse { snapshot, .. }
            | ExecutorMessage::SnapshotUpdated { snapshot } => {
                self.set_executor_snapshot(executor_id, generation, snapshot)
            }
            ExecutorMessage::Heartbeat { advertisement }
            | ExecutorMessage::AdvertisementUpdated { advertisement } => {
                self.update_advertisement(executor_id, generation, advertisement)
            }
            ExecutorMessage::InfrastructureDiagnostic { diagnostic } => {
                self.fail_for_executor(executor_id, &diagnostic);
                false
            }
            _ => false,
        }
    }

    fn set_executor_snapshot(
        &self,
        executor_id: &str,
        generation: u64,
        mut snapshot: BuildSlotPoolSnapshot,
    ) -> bool {
        let mut connections = self.connections.lock().unwrap();
        let Some(entry) = connections.get_mut(executor_id) else {
            return false;
        };
        if entry.generation != generation {
            return false;
        }
        for slot in &mut snapshot.slots {
            slot.executor_id = executor_id.to_string();
            slot.executor_display_name = Some(entry.identity.display_name.clone());
            if let Some(active) = &mut slot.active_request {
                active.executor_id = executor_id.to_string();
            }
        }
        for queued in &mut snapshot.queued_requests {
            queued.executor_id = executor_id.to_string();
        }
        entry.snapshot = snapshot;
        true
    }

    pub fn snapshot(&self) -> BuildSlotPoolSnapshot {
        let connections = self.connections.lock().unwrap();
        let mut ids: Vec<_> = connections.keys().cloned().collect();
        ids.sort();
        let mut aggregate = BuildSlotPoolSnapshot::default();
        for id in ids {
            let snapshot = &connections[&id].snapshot;
            aggregate.slots.extend(snapshot.slots.clone());
            aggregate
                .queued_requests
                .extend(snapshot.queued_requests.clone());
        }
        aggregate
            .slots
            .sort_by(|a, b| (&a.executor_id, &a.slot_id).cmp(&(&b.executor_id, &b.slot_id)));
        aggregate.queued_requests.sort_by(|a, b| {
            (a.queued_at_unix_ms, &a.executor_id, &a.request_id).cmp(&(
                b.queued_at_unix_ms,
                &b.executor_id,
                &b.request_id,
            ))
        });
        aggregate
    }

    pub fn cancel_request(&self, request_id: &str) -> bool {
        let owner = self
            .pending
            .lock()
            .unwrap()
            .iter()
            .find(|((id, _), _)| id == request_id)
            .map(|((_, attempt), pending)| {
                (
                    attempt.clone(),
                    pending.executor_id.clone(),
                    pending.generation,
                )
            });
        let Some((attempt_id, executor_id, generation)) = owner else {
            return false;
        };
        self.send_to(
            &executor_id,
            generation,
            ExecutorMessage::Cancel {
                request_id: request_id.into(),
                attempt_id,
            },
        )
        .is_ok()
    }

    pub fn cancel_job_requests(&self, job_id: &str) -> usize {
        let targets: Vec<_> = self
            .connections
            .lock()
            .unwrap()
            .values()
            .map(|entry| (entry.identity.executor_id.clone(), entry.generation))
            .collect();
        targets
            .into_iter()
            .filter(|(id, generation)| {
                self.send_to(
                    id,
                    *generation,
                    ExecutorMessage::CancelJob {
                        job_id: job_id.into(),
                    },
                )
                .is_ok()
            })
            .count()
    }

    pub async fn submit(&self, orch: &Orchestrator, request: BuildSlotRequest) -> BuildSlotOutcome {
        self.submit_execution(orch, request, None).await
    }

    pub(crate) async fn submit_run_batch(
        &self,
        orch: &Orchestrator,
        request: BuildSlotRequest,
        batch: ResolvedRunBatch,
    ) -> BuildSlotOutcome {
        let runner_context_id = uuid::Uuid::new_v4().to_string();
        self.runner_contexts.lock().unwrap().insert(
            runner_context_id.clone(),
            RunnerCallbackContext {
                request: batch.request.clone(),
                run_context: batch.run_context.clone(),
            },
        );
        let sandbox_mode = crate::mcp::handlers::fence::resolve_run_fence(orch, &batch.request)
            .await
            .map(|(_, fence)| {
                if crate::services::sandbox::sandbox_applies(fence) {
                    ProcessSandboxMode::Confined
                } else {
                    ProcessSandboxMode::Unconfined
                }
            })
            .unwrap_or(ProcessSandboxMode::Unconfined);
        let batch = match serialize_process_batch(
            batch,
            request.timeout_ms,
            runner_context_id.clone(),
            sandbox_mode,
        ) {
            Ok(batch) => batch,
            Err(diagnostic) => {
                return BuildSlotOutcome::Unavailable {
                    reason: BuildSlotUnavailableReason::Spawn,
                    diagnostic,
                }
            }
        };
        let outcome = self.submit_execution(orch, request, Some(batch)).await;
        self.runner_contexts
            .lock()
            .unwrap()
            .remove(&runner_context_id);
        outcome
    }

    pub async fn handle_runner_callback(
        &self,
        orch: &Orchestrator,
        callback: RunnerCallback,
    ) -> RunnerCallbackResult {
        let context_id = match &callback {
            RunnerCallback::SandboxDenied {
                runner_context_id, ..
            }
            | RunnerCallback::CacheCheckpoint {
                runner_context_id, ..
            }
            | RunnerCallback::ProcessEvent {
                runner_context_id, ..
            } => runner_context_id,
        };
        let Some(context) = self
            .runner_contexts
            .lock()
            .unwrap()
            .get(context_id)
            .cloned()
        else {
            return RunnerCallbackResult::Failed {
                diagnostic: "unknown or expired runner callback context".into(),
            };
        };
        match callback {
            RunnerCallback::SandboxDenied {
                command, denial, ..
            } => {
                use crate::mcp::handlers::fence::{self, FenceDecision};
                let Some((run_id, mode)) = fence::resolve_run_fence(orch, &context.request).await
                else {
                    return RunnerCallbackResult::Rejected {
                        diagnostic:
                            "sandbox denial cannot be adjudicated without an originating run fence"
                                .into(),
                    };
                };
                let crossing = match denial {
                    cairn_common::executor_protocol::SandboxDenial::Path(path) => {
                        let path = std::path::PathBuf::from(path);
                        fence::Crossing::shell_path(&path, &path.display().to_string())
                    }
                    cairn_common::executor_protocol::SandboxDenial::Command => {
                        fence::Crossing::shell_command(
                            format!("command blocked by the executor worktree sandbox: {command}"),
                            &command,
                        )
                    }
                };
                match fence::raise_fence(orch, &run_id, mode, &context.request, crossing).await {
                    FenceDecision::Allow => RunnerCallbackResult::Allowed,
                    FenceDecision::Deny(diagnostic) => {
                        RunnerCallbackResult::Rejected { diagnostic }
                    }
                    FenceDecision::Suspended => RunnerCallbackResult::Suspended,
                }
            }
            RunnerCallback::CacheCheckpoint {
                command,
                cwd,
                exit_code,
                ..
            } => {
                if let Some(run_context) = context.run_context {
                    crate::mcp::handlers::run::cache_checkpoint_callback(
                        orch,
                        &run_context.job_id,
                        &command,
                        &cwd,
                        exit_code,
                    )
                    .await;
                }
                RunnerCallbackResult::Completed
            }
            RunnerCallback::ProcessEvent {
                stream_id, payload, ..
            } => {
                if let Some(run_context) = context.run_context {
                    let _ = orch.services.emitter.emit(
                        "run-output",
                        serde_json::json!({
                            "runId": run_context.run_id,
                            "toolUseId": stream_id,
                            "chunk": payload,
                            "stream": "stdout",
                        }),
                    );
                }
                RunnerCallbackResult::Completed
            }
        }
    }

    async fn submit_execution(
        &self,
        orch: &Orchestrator,
        request: BuildSlotRequest,
        batch: Option<ProcessBatch>,
    ) -> BuildSlotOutcome {
        let config = match crate::config::settings::load_settings_file(&orch.config_dir) {
            Ok(file) => file.build_slots.unwrap_or_default(),
            Err(error) => {
                return BuildSlotOutcome::Unavailable {
                    reason: BuildSlotUnavailableReason::ExecutorUnavailable,
                    diagnostic: format!("load build-slot settings: {error}"),
                }
            }
        };
        let project_key = match crate::projects::crud::resolve_local_repo_path_and_key(
            &orch.db,
            &request.project_id,
        )
        .await
        {
            Ok((_, key)) => key,
            Err(error) => {
                return BuildSlotOutcome::Unavailable {
                    reason: BuildSlotUnavailableReason::ExecutorUnavailable,
                    diagnostic: format!("resolve build-slot project key: {error}"),
                }
            }
        };
        let executor_config = ExecutorConfig {
            project_id: request.project_id.clone(),
            capacity: config.slots_for(&project_key),
            acquisition_deadline_seconds: config.acquisition_deadline_seconds,
            default_timeout_seconds: config.default_timeout_seconds,
        };

        let selected = match self.select_executor(&request).await {
            Ok(selected) => selected,
            Err(outcome) => return outcome,
        };
        let mut request = request;
        if !selected.colocated {
            let identity = request.repository.identity();
            request.repository = RepositoryLocator::ManagedObjects {
                project_id: identity.project_id,
                repository_id: identity.repository_id,
                object_format: identity.object_format,
            };
            orch.object_plane.authorize_request(
                &request,
                &selected.executor_id,
                selected.generation,
            );
        }
        let key = (request.request_id.clone(), request.attempt_id.clone());
        let (tx, rx) = oneshot::channel();
        if self
            .pending
            .lock()
            .unwrap()
            .insert(
                key.clone(),
                PendingResult {
                    executor_id: selected.executor_id.clone(),
                    generation: selected.generation,
                    waiter: tx,
                },
            )
            .is_some()
        {
            return executor_unavailable("duplicate build-slot request identity".into());
        }
        let mut guard = SubmitDropGuard {
            pool: self.clone(),
            request_id: key.0.clone(),
            attempt_id: key.1.clone(),
            executor_id: selected.executor_id.clone(),
            generation: selected.generation,
            armed: true,
        };
        let watchdog = request_watchdog_duration(
            &request,
            batch.as_ref(),
            &executor_config,
            selected.colocated,
        );
        let sent = selected
            .sender
            .send(ExecutorMessage::Configure {
                config: executor_config,
            })
            .and_then(|_| {
                selected
                    .sender
                    .send(ExecutorMessage::Submit { request, batch })
            });
        if sent.is_err() {
            self.pending.lock().unwrap().remove(&key);
            if !selected.colocated {
                orch.object_plane.revoke_request(
                    &key.0,
                    &key.1,
                    &selected.executor_id,
                    selected.generation,
                );
            }
            guard.disarm();
            return executor_unavailable(
                "executor connection closed while submitting request".into(),
            );
        }
        let (outcome, watchdog_expired) = match tokio::time::timeout(watchdog, rx).await {
            Ok(result) => (
                result.unwrap_or_else(|_| {
                    executor_unavailable("executor result channel closed".into())
                }),
                false,
            ),
            Err(_) => (
                executor_unavailable(format!(
                    "executor did not return request {} attempt {} within the {}ms end-to-end watchdog budget; the in-flight attempt was cancelled",
                    key.0,
                    key.1,
                    watchdog.as_millis(),
                )),
                true,
            ),
        };
        if !selected.colocated {
            orch.object_plane.revoke_request(
                &key.0,
                &key.1,
                &selected.executor_id,
                selected.generation,
            );
        }
        if watchdog_expired {
            return outcome;
        }
        guard.disarm();
        outcome
    }

    async fn select_executor(
        &self,
        request: &BuildSlotRequest,
    ) -> Result<SelectedExecutor, BuildSlotOutcome> {
        loop {
            let notified = self.connection_ready.notified();
            let selection = {
                let connections = self.connections.lock().unwrap();
                choose_executor(&connections, request)
            };
            match selection {
                Ok(Some(selected)) => return Ok(selected),
                Err(diagnostic) => {
                    return Err(BuildSlotOutcome::Unavailable {
                        reason: BuildSlotUnavailableReason::NoMatchingExecutor,
                        diagnostic,
                    });
                }
                Ok(None) => {}
            }
            let remaining = request.deadline_unix_ms.saturating_sub(unix_time_ms());
            if remaining == 0
                || tokio::time::timeout(Duration::from_millis(remaining), notified)
                    .await
                    .is_err()
            {
                return Err(
                    if request.constraints.as_ref().is_some_and(|c| !c.is_empty()) {
                        BuildSlotOutcome::Unavailable {
                        reason: BuildSlotUnavailableReason::NoMatchingExecutor,
                        diagnostic: format!("no executor satisfying {} became usable before the acquisition deadline", format_constraints(request.constraints.as_ref().unwrap())),
                    }
                    } else {
                        executor_unavailable("colocated executor did not become ready before the acquisition deadline".into())
                    },
                );
            }
        }
    }

    #[cfg(test)]
    async fn wait_for_executor(
        &self,
        deadline_unix_ms: u64,
    ) -> Result<mpsc::UnboundedSender<ExecutorMessage>, String> {
        let request = BuildSlotRequest {
            request_id: String::new(),
            attempt_id: String::new(),
            project_id: String::new(),
            repository: RepositoryLocator::ColocatedPath {
                project_id: String::new(),
                repository_id: String::new(),
                absolute_path: String::new(),
            },
            base_commit: String::new(),
            command: String::new(),
            cwd: String::new(),
            env: Vec::new(),
            priority: BuildSlotPriority::ReviewCheck,
            deadline_unix_ms,
            timeout_ms: 0,
            mutation_policy: MutationPolicy::PureVerdict,
            requesting_job_id: None,
            affinity_key: None,
            constraints: None,
        };
        self.select_executor(&request)
            .await
            .map(|selected| selected.sender)
            .map_err(|outcome| match outcome {
                BuildSlotOutcome::Unavailable { diagnostic, .. } => diagnostic,
                _ => "executor unavailable".into(),
            })
    }

    fn send_to(
        &self,
        executor_id: &str,
        generation: u64,
        message: ExecutorMessage,
    ) -> Result<(), String> {
        let sender = self
            .connections
            .lock()
            .unwrap()
            .get(executor_id)
            .filter(|entry| entry.generation == generation)
            .map(|entry| entry.sender.clone())
            .ok_or_else(|| "executor is not connected at the selected generation".to_string())?;
        sender
            .send(message)
            .map_err(|_| "executor connection is closed".to_string())
    }

    fn fail_for_executor(&self, executor_id: &str, diagnostic: &str) {
        let mut pending = self.pending.lock().unwrap();
        let keys: Vec<_> = pending
            .iter()
            .filter(|(_, entry)| entry.executor_id == executor_id)
            .map(|(key, _)| key.clone())
            .collect();
        for key in keys {
            if let Some(entry) = pending.remove(&key) {
                let _ = entry
                    .waiter
                    .send(executor_unavailable(diagnostic.to_string()));
            }
        }
    }
}

fn choose_executor(
    connections: &HashMap<String, ExecutorConnectionState>,
    request: &BuildSlotRequest,
) -> Result<Option<SelectedExecutor>, String> {
    let constrained = request
        .constraints
        .as_ref()
        .is_some_and(|constraints| !constraints.is_empty());
    if !constrained {
        return Ok(connections
            .values()
            .find(|entry| entry.colocated && !entry.sender.is_closed())
            .map(selected_executor));
    }
    let constraints = request.constraints.as_ref().unwrap();
    let eligible: Vec<_> = connections
        .values()
        .filter(|entry| !entry.sender.is_closed())
        .filter(|entry| serves_project(entry, &request.project_id))
        .filter(|entry| matches_constraints(entry, constraints))
        .collect();
    if eligible.is_empty() {
        return Err(format!(
            "no live enrolled executor satisfies {} for project {}",
            format_constraints(constraints),
            request.project_id
        ));
    }
    let usable: Vec<_> = eligible
        .into_iter()
        .filter(|entry| {
            entry.advertisement.current_load < entry.advertisement.capabilities.slot_capacity
        })
        .collect();
    Ok(rank_usable_executors(usable, request, repository_sync_cost)
        .first()
        .map(|(entry, _)| selected_executor(entry)))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SyncCost {
    Known(u64),
    Unknown,
}

fn rank_usable_executors<'a>(
    usable: Vec<&'a ExecutorConnectionState>,
    request: &BuildSlotRequest,
    estimate: impl Fn(&BuildSlotRequest, &ExecutorConnectionState) -> SyncCost,
) -> Vec<(&'a ExecutorConnectionState, SyncCost)> {
    let mut ranked: Vec<_> = usable
        .into_iter()
        .map(|entry| {
            let cost = estimate(request, entry);
            (entry, cost)
        })
        .collect();
    ranked.sort_by(|(a, a_cost), (b, b_cost)| {
        sync_cost_key(*a_cost)
            .cmp(&sync_cost_key(*b_cost))
            .then_with(|| {
                a.advertisement
                    .current_load
                    .cmp(&b.advertisement.current_load)
            })
            .then_with(|| a.identity.executor_id.cmp(&b.identity.executor_id))
    });
    ranked
}

fn sync_cost_key(cost: SyncCost) -> (bool, u64) {
    match cost {
        SyncCost::Known(bytes) => (false, bytes),
        SyncCost::Unknown => (true, 0),
    }
}

fn repository_sync_cost(request: &BuildSlotRequest, entry: &ExecutorConnectionState) -> SyncCost {
    if entry.colocated {
        return SyncCost::Known(0);
    }

    let repository = request.repository.identity();
    let warm_root_commits: Vec<_> = entry
        .advertisement
        .warm_roots
        .iter()
        .filter(|root| root.repository == repository)
        .map(|root| root.commit.clone())
        .collect();
    if warm_root_commits
        .iter()
        .any(|commit| commit == &request.base_commit)
    {
        return SyncCost::Known(0);
    }

    let Some(repository_path) = request.repository.colocated_path() else {
        return SyncCost::Unknown;
    };
    missing_reachable_object_bytes(repository_path, &request.base_commit, &warm_root_commits)
        .map(SyncCost::Known)
        .unwrap_or(SyncCost::Unknown)
}

// Canonical object bytes are a stable placement approximation, not predicted
// compressed wire bytes. Pack deltas, Git LFS, submodules, shallow/promisor
// history, and stale advertised roots can all make the eventual transfer differ.
fn missing_reachable_object_bytes(
    repository: &str,
    base_commit: &str,
    warm_root_commits: &[String],
) -> Result<u64, String> {
    let mut revision_args = vec!["rev-list", "--objects", "--no-object-names", base_commit];
    let exclusions: Vec<_> = warm_root_commits
        .iter()
        .map(|commit| format!("^{commit}"))
        .collect();
    revision_args.extend(exclusions.iter().map(String::as_str));

    let objects = Command::new("git")
        .arg("-C")
        .arg(repository)
        .args(revision_args)
        .output()
        .map_err(|error| format!("failed to enumerate repository objects: {error}"))?;
    if !objects.status.success() {
        return Err(String::from_utf8_lossy(&objects.stderr).trim().to_string());
    }
    if objects.stdout.is_empty() {
        return Ok(0);
    }

    let mut child = Command::new("git")
        .arg("-C")
        .arg(repository)
        .args(["cat-file", "--batch-check=%(objectsize)"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| format!("failed to inspect repository objects: {error}"))?;
    child
        .stdin
        .take()
        .ok_or_else(|| "git cat-file stdin was unavailable".to_string())?
        .write_all(&objects.stdout)
        .map_err(|error| format!("failed to send repository objects to git: {error}"))?;
    let sizes = child
        .wait_with_output()
        .map_err(|error| format!("failed to read repository object sizes: {error}"))?;
    if !sizes.status.success() {
        return Err(String::from_utf8_lossy(&sizes.stderr).trim().to_string());
    }

    String::from_utf8(sizes.stdout)
        .map_err(|error| format!("git returned non-UTF-8 object sizes: {error}"))?
        .lines()
        .try_fold(0_u64, |total, size| {
            let size = size
                .parse::<u64>()
                .map_err(|error| format!("git returned invalid object size {size:?}: {error}"))?;
            total
                .checked_add(size)
                .ok_or_else(|| "repository object size total overflowed u64".to_string())
        })
}

fn selected_executor(entry: &ExecutorConnectionState) -> SelectedExecutor {
    SelectedExecutor {
        executor_id: entry.identity.executor_id.clone(),
        generation: entry.generation,
        sender: entry.sender.clone(),
        colocated: entry.colocated,
    }
}

// The colocated executor shares the runner's local project routing authority.
fn serves_project(entry: &ExecutorConnectionState, project_id: &str) -> bool {
    entry.colocated
        || entry
            .advertisement
            .capabilities
            .projects_served
            .iter()
            .any(|project| project == project_id)
}

fn matches_constraints(
    entry: &ExecutorConnectionState,
    constraints: &PlacementConstraints,
) -> bool {
    constraints
        .executor_id
        .as_ref()
        .is_none_or(|value| value == &entry.identity.executor_id)
        && constraints
            .device_id
            .as_ref()
            .is_none_or(|value| value == &entry.identity.device_id)
        && constraints
            .os
            .as_ref()
            .is_none_or(|value| value.eq_ignore_ascii_case(&entry.advertisement.capabilities.os))
        && constraints
            .arch
            .as_ref()
            .is_none_or(|value| value.eq_ignore_ascii_case(&entry.advertisement.capabilities.arch))
        && constraints.required_toolchains.iter().all(|required| {
            entry
                .advertisement
                .capabilities
                .toolchains
                .iter()
                .any(|available| available == required)
        })
}

fn format_constraints(constraints: &PlacementConstraints) -> String {
    serde_json::to_string(constraints)
        .unwrap_or_else(|_| "the requested placement constraints".into())
}

fn serialize_process_batch(
    batch: ResolvedRunBatch,
    default_timeout_ms: u32,
    runner_context_id: String,
    sandbox_mode: ProcessSandboxMode,
) -> Result<ProcessBatch, String> {
    let mut items = Vec::with_capacity(batch.resolved.len());
    for (index, (header, spec)) in batch.resolved.into_iter().enumerate() {
        let spec = spec.map_err(|error| format!("resolve process item {header}: {error}"))?;
        let (program, args, stdin, timeout) = match spec {
            RunSpec::Shell { command, timeout } => {
                (shell_program(), shell_args(command), None, timeout)
            }
            RunSpec::Script {
                program,
                args,
                timeout,
                stdin,
            } => (program, args, stdin, timeout),
            RunSpec::McpCall(_) | RunSpec::ReplSend { .. } => {
                return Err(format!(
                    "{header} is not process-backed and cannot use a build slot"
                ))
            }
        };
        items.push(ProcessBatchItem {
            header,
            stream_id: format!("{}:{index}", batch.tool_use_id),
            program,
            args,
            env: Vec::new(),
            stdin,
            timeout_ms: timeout.unwrap_or(default_timeout_ms),
        });
    }
    Ok(ProcessBatch {
        sequential: batch.originally_sequential,
        stop_on_error: batch.stop_on_error,
        sandbox_mode,
        items,
        runner_context_id: Some(runner_context_id),
    })
}

#[cfg(windows)]
fn shell_program() -> String {
    "cmd".into()
}
#[cfg(windows)]
fn shell_args(command: String) -> Vec<String> {
    vec!["/c".into(), command]
}
#[cfg(not(windows))]
fn shell_program() -> String {
    "/bin/sh".into()
}
#[cfg(not(windows))]
fn shell_args(command: String) -> Vec<String> {
    vec!["-c".into(), command]
}

fn outcome_matches(outcome: &BuildSlotOutcome, request_id: &str, attempt_id: &str) -> bool {
    match outcome {
        BuildSlotOutcome::Completed {
            request_id: r,
            attempt_id: a,
            ..
        }
        | BuildSlotOutcome::FailedAfterExecution {
            request_id: r,
            attempt_id: a,
            ..
        }
        | BuildSlotOutcome::Cancelled {
            request_id: r,
            attempt_id: a,
        } => r == request_id && a == attempt_id,
        BuildSlotOutcome::Unavailable { .. } => true,
    }
}

fn request_watchdog_duration(
    request: &BuildSlotRequest,
    batch: Option<&ProcessBatch>,
    executor_config: &ExecutorConfig,
    colocated: bool,
) -> Duration {
    let acquisition =
        Duration::from_millis(request.deadline_unix_ms.saturating_sub(unix_time_ms()));
    let phase_budget = Duration::from_secs(executor_config.default_timeout_seconds);
    // Provisioning/materialization and preparation are distinct executor phases.
    let infrastructure = phase_budget.saturating_mul(2);
    // Managed fetch and post-command delta upload each use the executor's bounded
    // whole-request HTTP deadline. Colocated execution performs neither transfer.
    let object_transfer = if colocated {
        Duration::ZERO
    } else {
        Duration::from_secs(MANAGED_OBJECT_REQUEST_TIMEOUT_SECONDS * 2)
    };
    let execution = match batch {
        None => Duration::from_millis(u64::from(request.timeout_ms)),
        Some(batch) if batch.sequential => {
            batch.items.iter().fold(Duration::ZERO, |total, item| {
                total.saturating_add(Duration::from_millis(u64::from(item.timeout_ms)))
            })
        }
        Some(batch) => batch
            .items
            .iter()
            .map(|item| Duration::from_millis(u64::from(item.timeout_ms)))
            .max()
            .unwrap_or(Duration::ZERO),
    };
    let end_to_end_budget = acquisition
        .saturating_add(infrastructure)
        .saturating_add(object_transfer)
        .saturating_add(execution);
    let proportional_slack = end_to_end_budget / 10;
    end_to_end_budget.saturating_add(
        proportional_slack.clamp(MIN_REQUEST_WATCHDOG_SLACK, MAX_REQUEST_WATCHDOG_SLACK),
    )
}

fn executor_unavailable(diagnostic: String) -> BuildSlotOutcome {
    BuildSlotOutcome::Unavailable {
        reason: BuildSlotUnavailableReason::ExecutorUnavailable,
        diagnostic,
    }
}

pub fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_codec::testutil::{commit_all, init_repo, write_file};
    use cairn_common::executor_protocol::{GitObjectFormat, VerifiedWarmRoot};

    fn resolved_process_batch(
        timeouts: Vec<Option<u32>>,
        sequential: bool,
        stop_on_error: bool,
    ) -> ResolvedRunBatch {
        ResolvedRunBatch {
            request: crate::mcp::types::McpCallbackRequest {
                thread_id: None,
                cwd: "/tmp".into(),
                run_id: None,
                tool: "run".into(),
                payload: serde_json::Value::Null,
                tool_use_id: None,
            },
            run_context: None,
            resolved: timeouts
                .into_iter()
                .enumerate()
                .map(|(index, timeout)| {
                    (
                        format!("command-{index}"),
                        Ok(RunSpec::Shell {
                            command: "true".into(),
                            timeout,
                        }),
                    )
                })
                .collect(),
            tool_use_id: "tool-use".into(),
            stop_on_error,
            originally_sequential: sequential,
        }
    }

    #[test]
    fn process_batch_serialization_preserves_millisecond_timeouts_and_flags() {
        let batch = serialize_process_batch(
            resolved_process_batch(vec![Some(3_000), None], true, false),
            1_800_000,
            "runner-context".into(),
            ProcessSandboxMode::Confined,
        )
        .unwrap();

        assert_eq!(batch.items[0].timeout_ms, 3_000);
        assert_eq!(batch.items[1].timeout_ms, 1_800_000);
        #[cfg(not(windows))]
        assert_eq!(batch.items[0].args, ["-c", "true"]);
        assert!(batch.sequential);
        assert!(!batch.stop_on_error);
    }

    #[test]
    fn default_capacity_and_key_overrides_are_unconditional() {
        let defaults = BuildSlotsConfig::default();
        assert_eq!(defaults.slots_for("CAIRN"), 2);
        let mut configured = defaults;
        configured.projects.insert("CAIRN".into(), 6);
        assert_eq!(configured.slots_for("CAIRN"), 6);
        assert_eq!(configured.slots_for("project-uuid"), 2);
        configured.global_capacity = Some(3);
        assert_eq!(configured.slots_for("CAIRN"), 3);
        assert_eq!(configured.slots_for("OTHER"), 2);
    }

    #[tokio::test]
    async fn new_submission_waits_for_executor_readiness() {
        let pool = BuildSlotPool::default();
        let attaching = pool.clone();
        let (tx, mut rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(25)).await;
            attaching.attach_executor(tx);
        });
        let request = BuildSlotRequest {
            request_id: "r".into(),
            attempt_id: "a".into(),
            project_id: "p".into(),
            repository: RepositoryLocator::ColocatedPath {
                project_id: "p".into(),
                repository_id: "repo".into(),
                absolute_path: "/repo".into(),
            },
            base_commit: "base".into(),
            command: "true".into(),
            cwd: String::new(),
            env: Vec::new(),
            priority: BuildSlotPriority::ReviewCheck,
            deadline_unix_ms: unix_time_ms() + 1_000,
            timeout_ms: 1_000,
            mutation_policy: MutationPolicy::PureVerdict,
            requesting_job_id: None,
            affinity_key: None,
            constraints: None,
        };
        let config = ExecutorConfig {
            project_id: "p".into(),
            capacity: 1,
            acquisition_deadline_seconds: 1,
            default_timeout_seconds: 1,
        };

        let sender = pool
            .wait_for_executor(request.deadline_unix_ms)
            .await
            .unwrap();
        sender.send(ExecutorMessage::Configure { config }).unwrap();
        sender
            .send(ExecutorMessage::Submit {
                request,
                batch: None,
            })
            .unwrap();
        assert!(matches!(
            rx.recv().await,
            Some(ExecutorMessage::Configure { .. })
        ));
        assert!(matches!(
            rx.recv().await,
            Some(ExecutorMessage::Submit { .. })
        ));
    }

    #[tokio::test]
    async fn wedged_submission_times_out_cancels_and_does_not_block_the_next_attempt() {
        let pool = BuildSlotPool::default();
        let (sender, mut executor) = mpsc::unbounded_channel();
        let generation = pool.attach_executor(sender);

        let first_key = ("request-1".to_string(), "attempt-1".to_string());
        let (first_tx, first_rx) = oneshot::channel();
        pool.pending.lock().unwrap().insert(
            first_key.clone(),
            PendingResult {
                executor_id: COLOCATED_EXECUTOR_ID.into(),
                generation,
                waiter: first_tx,
            },
        );
        let first_guard = SubmitDropGuard {
            pool: pool.clone(),
            request_id: first_key.0.clone(),
            attempt_id: first_key.1.clone(),
            executor_id: COLOCATED_EXECUTOR_ID.into(),
            generation,
            armed: true,
        };
        let outcome = tokio::time::timeout(Duration::from_millis(120), first_rx)
            .await
            .map(|result| result.unwrap())
            .unwrap_or_else(|_| executor_unavailable("executor request watchdog expired".into()));
        drop(first_guard);
        assert!(matches!(
            outcome,
            BuildSlotOutcome::Unavailable {
                reason: BuildSlotUnavailableReason::ExecutorUnavailable,
                ..
            }
        ));
        assert!(!pool.pending.lock().unwrap().contains_key(&first_key));
        assert!(matches!(
            executor.recv().await,
            Some(ExecutorMessage::Cancel { request_id, attempt_id })
                if request_id == first_key.0 && attempt_id == first_key.1
        ));

        let second_key = ("request-2".to_string(), "attempt-2".to_string());
        let (second_tx, second_rx) = oneshot::channel();
        pool.pending.lock().unwrap().insert(
            second_key.clone(),
            PendingResult {
                executor_id: COLOCATED_EXECUTOR_ID.into(),
                generation,
                waiter: second_tx,
            },
        );
        let completed = BuildSlotOutcome::Cancelled {
            request_id: second_key.0.clone(),
            attempt_id: second_key.1.clone(),
        };
        pool.handle_executor_message(
            COLOCATED_EXECUTOR_ID,
            generation,
            ExecutorMessage::Result {
                request_id: second_key.0,
                attempt_id: second_key.1,
                outcome: completed.clone(),
            },
        );
        assert_eq!(second_rx.await.unwrap(), completed);
    }

    #[test]
    fn watchdog_covers_preparation_and_full_process_batch_budget() {
        let mut request = constrained_request(std::env::consts::OS);
        request.deadline_unix_ms = unix_time_ms();
        request.timeout_ms = 500;
        let config = ExecutorConfig {
            project_id: request.project_id.clone(),
            capacity: 1,
            acquisition_deadline_seconds: 1,
            default_timeout_seconds: 2,
        };
        let batch = ProcessBatch {
            sequential: true,
            stop_on_error: false,
            sandbox_mode: ProcessSandboxMode::Unconfined,
            items: vec![
                ProcessBatchItem {
                    header: "one".into(),
                    stream_id: "one".into(),
                    program: "true".into(),
                    args: Vec::new(),
                    env: Vec::new(),
                    stdin: None,
                    timeout_ms: 600,
                },
                ProcessBatchItem {
                    header: "two".into(),
                    stream_id: "two".into(),
                    program: "true".into(),
                    args: Vec::new(),
                    env: Vec::new(),
                    stdin: None,
                    timeout_ms: 700,
                },
            ],
            runner_context_id: None,
        };

        let budget = request_watchdog_duration(&request, Some(&batch), &config, true);
        assert!(budget >= Duration::from_millis(5_300));
        assert!(budget > Duration::from_millis(u64::from(request.timeout_ms)));
    }

    #[tokio::test]
    async fn new_submission_fails_at_deadline_when_executor_stays_down() {
        let pool = BuildSlotPool::default();
        let request = BuildSlotRequest {
            request_id: "r".into(),
            attempt_id: "a".into(),
            project_id: "p".into(),
            repository: RepositoryLocator::ColocatedPath {
                project_id: "p".into(),
                repository_id: "repo".into(),
                absolute_path: "/repo".into(),
            },
            base_commit: "base".into(),
            command: "true".into(),
            cwd: String::new(),
            env: Vec::new(),
            priority: BuildSlotPriority::ReviewCheck,
            deadline_unix_ms: unix_time_ms() + 25,
            timeout_ms: 1_000,
            mutation_policy: MutationPolicy::PureVerdict,
            requesting_job_id: None,
            affinity_key: None,
            constraints: None,
        };
        let error = pool
            .wait_for_executor(request.deadline_unix_ms)
            .await
            .unwrap_err();
        assert!(error.contains("acquisition deadline"));
    }

    fn fleet_entry(
        id: &str,
        os: &str,
        load: usize,
        warm: &[&str],
    ) -> (String, ExecutorConnectionState) {
        let (sender, receiver) = mpsc::unbounded_channel();
        std::mem::forget(receiver);
        let identity = ExecutorIdentity {
            device_id: format!("device-{id}"),
            executor_id: id.into(),
            display_name: id.into(),
        };
        (
            id.into(),
            ExecutorConnectionState {
                identity: identity.clone(),
                advertisement: ExecutorAdvertisement {
                    identity,
                    capabilities: ExecutorCapabilities {
                        os: os.into(),
                        arch: "x86_64".into(),
                        logical_cores: 8,
                        toolchains: vec!["rust".into()],
                        projects_served: vec!["p".into()],
                        slot_capacity: 2,
                        disk_budget_bytes: None,
                        memory_budget_bytes: None,
                    },
                    current_load: load,
                    warm_roots: warm
                        .iter()
                        .map(|value| VerifiedWarmRoot {
                            repository: RepositoryLocator::ManagedObjects {
                                project_id: "p".into(),
                                repository_id: "repo".into(),
                                object_format: GitObjectFormat::Sha1,
                            }
                            .identity(),
                            commit: (*value).into(),
                        })
                        .collect(),
                    observed_at_unix_ms: 1,
                },
                generation: 1,
                sender,
                snapshot: BuildSlotPoolSnapshot::default(),
                colocated: id == COLOCATED_EXECUTOR_ID,
            },
        )
    }

    fn constrained_request(os: &str) -> BuildSlotRequest {
        BuildSlotRequest {
            request_id: "r".into(),
            attempt_id: "a".into(),
            project_id: "p".into(),
            repository: RepositoryLocator::ColocatedPath {
                project_id: "p".into(),
                repository_id: "repo".into(),
                absolute_path: "/repo".into(),
            },
            base_commit: "base".into(),
            command: "true".into(),
            cwd: String::new(),
            env: Vec::new(),
            priority: BuildSlotPriority::ReviewCheck,
            deadline_unix_ms: unix_time_ms() + 1_000,
            timeout_ms: 1_000,
            mutation_policy: MutationPolicy::PureVerdict,
            requesting_job_id: None,
            affinity_key: None,
            constraints: Some(PlacementConstraints {
                os: Some(os.into()),
                ..PlacementConstraints::default()
            }),
        }
    }

    #[test]
    fn hard_constraints_route_only_to_matching_executor() {
        let connections = HashMap::from([
            fleet_entry("linux", "linux", 0, &[]),
            fleet_entry("windows", "windows", 0, &[]),
        ]);
        let selected = choose_executor(&connections, &constrained_request("windows"))
            .unwrap()
            .unwrap();
        assert_eq!(selected.executor_id, "windows");
    }

    #[test]
    fn warm_executor_wins_only_when_it_is_usable() {
        let connections = HashMap::from([
            fleet_entry("cold", "linux", 0, &[]),
            fleet_entry("warm", "linux", 0, &["base"]),
        ]);
        assert_eq!(
            choose_executor(&connections, &constrained_request("linux"))
                .unwrap()
                .unwrap()
                .executor_id,
            "warm"
        );
        let connections = HashMap::from([
            fleet_entry("cold", "linux", 0, &[]),
            fleet_entry("warm", "linux", 2, &["base"]),
        ]);
        assert_eq!(
            choose_executor(&connections, &constrained_request("linux"))
                .unwrap()
                .unwrap()
                .executor_id,
            "cold"
        );
    }

    #[test]
    fn missing_byte_cost_is_scoped_to_the_request_repository() {
        let repo = tempfile::tempdir().unwrap();
        init_repo(repo.path());
        write_file(repo.path(), "base.txt", b"base");
        let warm = commit_all(repo.path(), "warm");
        write_file(repo.path(), "added.txt", b"request-specific bytes");
        let base = commit_all(repo.path(), "base");

        let cold =
            missing_reachable_object_bytes(repo.path().to_str().unwrap(), &base, &[]).unwrap();
        let incremental = missing_reachable_object_bytes(
            repo.path().to_str().unwrap(),
            &base,
            std::slice::from_ref(&warm),
        )
        .unwrap();
        assert!(incremental > 0);
        assert!(incremental < cold);

        let unrelated_repo = tempfile::tempdir().unwrap();
        init_repo(unrelated_repo.path());
        write_file(unrelated_repo.path(), "other.txt", b"other");
        let unrelated = commit_all(unrelated_repo.path(), "other");
        assert!(
            missing_reachable_object_bytes(repo.path().to_str().unwrap(), &base, &[unrelated],)
                .is_err()
        );
    }

    #[test]
    fn warm_root_is_zero_only_for_the_requested_repository() {
        let (_, mut entry) = fleet_entry("warm", "linux", 0, &["base"]);
        let request = constrained_request("linux");
        assert_eq!(repository_sync_cost(&request, &entry), SyncCost::Known(0));

        entry.advertisement.warm_roots[0].repository.repository_id = "other-repo".into();
        assert_eq!(repository_sync_cost(&request, &entry), SyncCost::Unknown);
    }

    #[test]
    fn known_missing_byte_cost_ranks_before_unknown() {
        let connections = HashMap::from([
            fleet_entry("known", "linux", 1, &[]),
            fleet_entry("unknown", "linux", 0, &[]),
        ]);
        let usable: Vec<_> = connections.values().collect();
        let ranked = rank_usable_executors(usable, &constrained_request("linux"), |_, entry| {
            if entry.identity.executor_id == "known" {
                SyncCost::Known(10)
            } else {
                SyncCost::Unknown
            }
        });
        assert_eq!(ranked[0].0.identity.executor_id, "known");
    }

    #[test]
    fn unknown_cost_does_not_exclude_the_only_usable_executor() {
        let connections = HashMap::from([fleet_entry("only", "linux", 0, &[])]);
        let usable: Vec<_> = connections.values().collect();
        let ranked = rank_usable_executors(usable, &constrained_request("linux"), |_, _| {
            SyncCost::Unknown
        });
        assert_eq!(ranked[0].0.identity.executor_id, "only");
    }

    #[test]
    fn explicit_constraints_can_select_colocated_executor() {
        let connections = HashMap::from([fleet_entry(COLOCATED_EXECUTOR_ID, "linux", 0, &[])]);
        let mut request = constrained_request("linux");
        request.constraints.as_mut().unwrap().executor_id = Some(COLOCATED_EXECUTOR_ID.into());
        assert_eq!(
            choose_executor(&connections, &request)
                .unwrap()
                .unwrap()
                .executor_id,
            COLOCATED_EXECUTOR_ID
        );
    }

    #[test]
    fn no_match_is_typed_and_never_uses_colocated() {
        let connections = HashMap::from([fleet_entry(COLOCATED_EXECUTOR_ID, "linux", 0, &[])]);
        assert!(
            choose_executor(&connections, &constrained_request("windows"))
                .unwrap_err()
                .contains("no live enrolled executor")
        );
    }

    #[test]
    fn mismatched_terminal_identity_is_rejected() {
        let outcome = BuildSlotOutcome::Cancelled {
            request_id: "r".into(),
            attempt_id: "old".into(),
        };
        assert!(!outcome_matches(&outcome, "r", "new"));
    }
}
