//! Runner-owned persistent build workspaces.
//!
//! The pool is intentionally check-agnostic. Requests name an immutable commit,
//! command, relative working directory, environment, priority, and deadline.
//! Check planning, verdict parsing, caching, and fallback remain in `execution`.

use crate::jj::{project_store_dir, JjEnv};
use crate::mcp::handlers::run::run_check_command;
use crate::orchestrator::Orchestrator;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, VecDeque};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Notify;

const IDENTITY_FILE: &str = "cairn-build-slot.json";
const STATE_FILE: &str = "cairn-build-slot-state.json";
const LOCK_FILE: &str = "cairn-build-slot.lock";
const MAX_HIGH_PRIORITY_BURST: u32 = 4;
pub const DEFAULT_PROJECT_SLOT_COUNT: usize = 2;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BuildSlotsConfig {
    // Always serialized: this struct crosses the transport to the settings UI,
    // where an omitted field materializes as `undefined` and crashes consumers
    // that treat the map as present. An empty `projects: {}` in settings.yaml
    // is a harmless cost by comparison.
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "camelCase")]
pub enum BuildSlotPriority {
    ReviewCheck,
    WriteCheck,
    AgentInteractive,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum MutationPolicy {
    PureVerdict,
    AllowDelta,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BuildSlotRequest {
    pub request_id: String,
    pub attempt_id: String,
    pub project_id: String,
    pub repository: String,
    pub base_commit: String,
    pub command: String,
    #[serde(default)]
    pub cwd: String,
    #[serde(default)]
    pub env: Vec<(String, String)>,
    pub priority: BuildSlotPriority,
    pub deadline_unix_ms: u64,
    pub timeout_ms: u32,
    pub mutation_policy: MutationPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requesting_job_id: Option<String>,
    /// Stable request-level placement identity (for example one agent turn).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub affinity_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MutationDelta {
    pub base_commit: String,
    pub delta_commit: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BuildSlotExecutionMeta {
    pub slot_id: String,
    pub lease_epoch: u64,
    pub started_at_unix_ms: u64,
    pub finished_at_unix_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum BuildSlotUnavailableReason {
    NoCapacity,
    Deadline,
    Provisioning,
    Checkout,
    Spawn,
    Preparation,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "camelCase")]
pub enum BuildSlotOutcome {
    Completed {
        request_id: String,
        attempt_id: String,
        exit_code: Option<i32>,
        output: String,
        timed_out: bool,
        metadata: BuildSlotExecutionMeta,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        mutation_delta: Option<MutationDelta>,
    },
    Unavailable {
        reason: BuildSlotUnavailableReason,
        diagnostic: String,
    },
    /// Execution began, so this failure is never eligible for local fallback.
    FailedAfterExecution {
        request_id: String,
        attempt_id: String,
        diagnostic: String,
    },
    Cancelled {
        request_id: String,
        attempt_id: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum PersistentSlotLifecycle {
    Provisioning,
    Idle,
    Queued,
    Running,
    Recovering,
    Retired,
    Quarantined,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ActiveBuildSlotRequest {
    pub request_id: String,
    pub attempt_id: String,
    pub command: String,
    pub priority: BuildSlotPriority,
    pub requesting_job_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub affinity_key: Option<String>,
    pub queued_at_unix_ms: u64,
    pub started_at_unix_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PersistentBuildSlotState {
    pub project_id: String,
    pub slot_id: String,
    pub path: String,
    pub workspace_name: String,
    pub repository: String,
    pub lifecycle: PersistentSlotLifecycle,
    pub lease_epoch: u64,
    pub last_sealed_commit: Option<String>,
    pub last_used_unix_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_affinity_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preparation_fingerprint: Option<String>,
    pub active_request: Option<ActiveBuildSlotRequest>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct QueuedBuildSlotRequest {
    pub request_id: String,
    pub attempt_id: String,
    pub project_id: String,
    pub command: String,
    pub priority: BuildSlotPriority,
    pub requesting_job_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub affinity_key: Option<String>,
    pub queued_at_unix_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BuildSlotPoolSnapshot {
    pub slots: Vec<PersistentBuildSlotState>,
    pub queued_requests: Vec<QueuedBuildSlotRequest>,
}

#[derive(Clone, Default)]
pub struct BuildSlotPool {
    inner: Arc<Mutex<PoolState>>,
    notify: Arc<Notify>,
    cancellations: Arc<Mutex<HashMap<String, Arc<AtomicBool>>>>,
}

#[derive(Default)]
struct PoolState {
    projects: HashMap<String, ProjectState>,
    next_sequence: u64,
}

#[derive(Default)]
struct ProjectState {
    slots: Vec<RuntimeSlot>,
    queue: VecDeque<QueueEntry>,
    high_priority_burst: u32,
}

struct RuntimeSlot {
    persistent: PersistentBuildSlotState,
    leased: bool,
    _ownership_lock: std::fs::File,
}
struct QueueEntry {
    sequence: u64,
    queued_at: u64,
    request: BuildSlotRequest,
}

/// Cancels a queued or running lease if its submit future is dropped (for
/// example when the MCP handler disconnects or is cancelled).
struct SubmitDropGuard {
    pool: BuildSlotPool,
    request_id: String,
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
            self.pool.cancel_request(&self.request_id);
        }
    }
}

impl BuildSlotPool {
    pub fn snapshot(&self) -> BuildSlotPoolSnapshot {
        let state = self.inner.lock().unwrap();
        let slots = state
            .projects
            .values()
            .flat_map(|p| p.slots.iter().map(|s| s.persistent.clone()))
            .collect();
        let queued_requests = state
            .projects
            .iter()
            .flat_map(|(project_id, project)| {
                project.queue.iter().map(|entry| QueuedBuildSlotRequest {
                    request_id: entry.request.request_id.clone(),
                    attempt_id: entry.request.attempt_id.clone(),
                    project_id: project_id.clone(),
                    command: entry.request.command.clone(),
                    priority: entry.request.priority,
                    requesting_job_id: entry.request.requesting_job_id.clone(),
                    affinity_key: entry.request.affinity_key.clone(),
                    queued_at_unix_ms: entry.queued_at,
                })
            })
            .collect();
        BuildSlotPoolSnapshot {
            slots,
            queued_requests,
        }
    }

    fn register_request(&self, request_id: &str, cancel: Arc<AtomicBool>) -> bool {
        let mut cancellations = self.cancellations.lock().unwrap();
        if cancellations.contains_key(request_id) {
            return false;
        }
        cancellations.insert(request_id.to_string(), cancel);
        true
    }

    pub fn cancel_request(&self, request_id: &str) -> bool {
        let token = self.cancellations.lock().unwrap().get(request_id).cloned();
        let mut state = self.inner.lock().unwrap();
        let mut found = token.is_some();
        // The token transition and lifecycle transition share the pool-state
        // critical section with the final publication fence. Whichever acquires
        // this lock first is the single atomic winner.
        if let Some(token) = &token {
            token.store(true, Ordering::SeqCst);
        }
        for project in state.projects.values_mut() {
            let before = project.queue.len();
            project.queue.retain(|q| q.request.request_id != request_id);
            found |= before != project.queue.len();
            for slot in &mut project.slots {
                if slot
                    .persistent
                    .active_request
                    .as_ref()
                    .is_some_and(|r| r.request_id == request_id)
                {
                    // Keep the lease fenced until the execution future has killed,
                    // reaped, and recovered the owned process/workspace. Releasing
                    // here would permit a second command into the same slot while
                    // cancellation cleanup is still running.
                    slot.persistent.lifecycle = PersistentSlotLifecycle::Recovering;
                    let _ = persist_state(&slot.persistent);
                    found = true;
                }
            }
        }
        drop(state);
        self.notify.notify_waiters();
        found
    }

    pub fn cancel_job_requests(&self, job_id: &str) -> usize {
        let ids: Vec<String> = {
            let state = self.inner.lock().unwrap();
            state
                .projects
                .values()
                .flat_map(|project| {
                    let queued = project
                        .queue
                        .iter()
                        .filter(|q| q.request.requesting_job_id.as_deref() == Some(job_id))
                        .map(|q| q.request.request_id.clone());
                    let running = project
                        .slots
                        .iter()
                        .filter_map(|slot| slot.persistent.active_request.as_ref())
                        .filter(|r| r.requesting_job_id.as_deref() == Some(job_id))
                        .map(|r| r.request_id.clone());
                    queued.chain(running).collect::<Vec<_>>()
                })
                .collect()
        };
        for id in &ids {
            self.cancel_request(id);
        }
        ids.len()
    }

    pub async fn submit(&self, orch: &Orchestrator, request: BuildSlotRequest) -> BuildSlotOutcome {
        self.submit_execution(orch, request, None).await
    }

    pub(crate) async fn submit_run_batch(
        &self,
        orch: &Orchestrator,
        request: BuildSlotRequest,
        batch: crate::mcp::handlers::run::ResolvedRunBatch,
    ) -> BuildSlotOutcome {
        self.submit_execution(orch, request, Some(batch)).await
    }

    async fn submit_execution(
        &self,
        orch: &Orchestrator,
        request: BuildSlotRequest,
        batch: Option<crate::mcp::handlers::run::ResolvedRunBatch>,
    ) -> BuildSlotOutcome {
        let config = match crate::config::settings::load_settings_file(&orch.config_dir) {
            Ok(file) => file.build_slots.unwrap_or_default(),
            Err(error) => {
                return BuildSlotOutcome::Unavailable {
                    reason: BuildSlotUnavailableReason::Provisioning,
                    diagnostic: format!("failed to load build-slot settings: {error}"),
                }
            }
        };
        let count = config.slots_for(&request.project_id);
        if count == 0 {
            return BuildSlotOutcome::Unavailable {
                reason: BuildSlotUnavailableReason::NoCapacity,
                diagnostic: "build-slot capacity is zero for this project".into(),
            };
        }
        if let Err(error) = validate_relative_cwd(&request.cwd) {
            return BuildSlotOutcome::Unavailable {
                reason: BuildSlotUnavailableReason::Provisioning,
                diagnostic: error,
            };
        }
        let cancel = Arc::new(AtomicBool::new(false));
        if !self.register_request(&request.request_id, cancel.clone()) {
            return BuildSlotOutcome::Cancelled {
                request_id: request.request_id,
                attempt_id: request.attempt_id,
            };
        }
        let mut drop_guard = SubmitDropGuard {
            pool: self.clone(),
            request_id: request.request_id.clone(),
            armed: true,
        };
        if let Err(error) = self.ensure_project_slots(orch, &request, count).await {
            self.cancellations
                .lock()
                .unwrap()
                .remove(&request.request_id);
            return BuildSlotOutcome::Unavailable {
                reason: BuildSlotUnavailableReason::Provisioning,
                diagnostic: error,
            };
        }
        let queued_at = unix_time_ms();
        {
            let mut state = self.inner.lock().unwrap();
            let sequence = state.next_sequence;
            state.next_sequence += 1;
            state
                .projects
                .entry(request.project_id.clone())
                .or_default()
                .queue
                .push_back(QueueEntry {
                    sequence,
                    queued_at,
                    request: request.clone(),
                });
        }
        let _ = orch.services.emitter.emit(
            "db-change",
            serde_json::json!({
                "table": "build_slots",
                "action": "queue",
                "request": {
                    "requestId": request.request_id,
                    "attemptId": request.attempt_id,
                    "command": request.command,
                    "priority": request.priority,
                    "requestingJobId": request.requesting_job_id,
                    "queuedAtUnixMs": queued_at,
                }
            }),
        );
        let lease = loop {
            if cancel.load(Ordering::SeqCst) {
                self.remove_queued(&request.request_id);
                self.cancellations
                    .lock()
                    .unwrap()
                    .remove(&request.request_id);
                self.emit_state_change(orch, "dequeue");
                return BuildSlotOutcome::Cancelled {
                    request_id: request.request_id,
                    attempt_id: request.attempt_id,
                };
            }
            if unix_time_ms() >= request.deadline_unix_ms {
                self.remove_queued(&request.request_id);
                self.cancellations
                    .lock()
                    .unwrap()
                    .remove(&request.request_id);
                self.emit_state_change(orch, "dequeue");
                return BuildSlotOutcome::Unavailable {
                    reason: BuildSlotUnavailableReason::Deadline,
                    diagnostic: "build-slot acquisition deadline elapsed".into(),
                };
            }
            if let Some(lease) = self.try_lease(orch, &request.project_id, &request.request_id) {
                break lease;
            }
            let remaining = request
                .deadline_unix_ms
                .saturating_sub(unix_time_ms())
                .min(250);
            let _ = tokio::time::timeout(
                Duration::from_millis(remaining.max(1)),
                self.notify.notified(),
            )
            .await;
        };
        self.emit_state_change(orch, "running");
        let outcome = self
            .execute_leased(orch, request.clone(), lease.clone(), cancel.clone(), batch)
            .await;
        self.release(&request, &lease, &outcome);
        self.cancellations
            .lock()
            .unwrap()
            .remove(&request.request_id);
        self.notify.notify_waiters();
        self.emit_state_change(orch, "idle");
        drop_guard.disarm();
        outcome
    }

    async fn ensure_project_slots(
        &self,
        orch: &Orchestrator,
        request: &BuildSlotRequest,
        count: usize,
    ) -> Result<(), String> {
        let existing = self
            .inner
            .lock()
            .unwrap()
            .projects
            .get(&request.project_id)
            .map_or(0, |p| p.slots.len());
        if existing >= count {
            return Ok(());
        }
        let root = orch
            .config_dir
            .join("build-slots")
            .join(safe_component(&request.project_id));
        std::fs::create_dir_all(&root).map_err(|e| format!("create build-slot root: {e}"))?;
        let jj = JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
        let repo = PathBuf::from(&request.repository);
        let store = project_store_dir(&orch.config_dir, &repo);
        let lock = orch.jj_store_lock(&store);
        let _guard = lock.lock().await;
        for index in existing..count {
            let slot_id = format!("slot-{}", index + 1);
            let path = root.join(&slot_id);
            let workspace_name = format!(
                "cairn-build-{}-{}",
                safe_component(&request.project_id),
                index + 1
            );
            let identity_path = path.join(".jj").join(IDENTITY_FILE);
            let mut adopted = None;
            if path.exists() {
                let identity = read_json::<PersistentBuildSlotState>(&identity_path);
                let state_path = path.join(".jj").join(STATE_FILE);
                let durable = read_json::<PersistentBuildSlotState>(&state_path);
                let agrees = |state: &PersistentBuildSlotState| {
                    state.project_id == request.project_id
                        && state.slot_id == slot_id
                        && state.path == path.to_string_lossy()
                        && state.workspace_name == workspace_name
                        && state.repository == request.repository
                };
                let proven =
                    identity.as_ref().is_some_and(&agrees) && durable.as_ref().is_some_and(&agrees);
                if proven {
                    adopted = durable;
                } else {
                    let quarantine = root.join(format!("{slot_id}.quarantine-{}", unix_time_ms()));
                    std::fs::rename(&path, &quarantine)
                        .map_err(|e| format!("quarantine ambiguous slot: {e}"))?;
                }
            }
            if !path.exists() {
                let path_string = path.to_string_lossy().into_owned();
                let args = [
                    "workspace",
                    "add",
                    "--name",
                    workspace_name.as_str(),
                    "-r",
                    request.base_commit.as_str(),
                    path_string.as_str(),
                ];
                jj.run(&store, &args, "create build slot")?;
            }
            let ownership_lock = acquire_ownership_lock(&path)?;
            let mut persistent = adopted.unwrap_or_else(|| PersistentBuildSlotState {
                project_id: request.project_id.clone(),
                slot_id: slot_id.clone(),
                path: path.to_string_lossy().into_owned(),
                workspace_name,
                repository: request.repository.clone(),
                lifecycle: PersistentSlotLifecycle::Idle,
                lease_epoch: 0,
                last_sealed_commit: Some(request.base_commit.clone()),
                last_used_unix_ms: unix_time_ms(),
                last_affinity_key: None,
                preparation_fingerprint: None,
                active_request: None,
            });
            reconcile_adopted_state(&jj, &path, request, &mut persistent)?;
            write_json_atomic(&identity_path, &persistent)?;
            persist_state(&persistent)?;
            self.inner
                .lock()
                .unwrap()
                .projects
                .entry(request.project_id.clone())
                .or_default()
                .slots
                .push(RuntimeSlot {
                    persistent,
                    leased: false,
                    _ownership_lock: ownership_lock,
                });
        }
        Ok(())
    }

    fn try_lease(&self, orch: &Orchestrator, project_id: &str, request_id: &str) -> Option<Lease> {
        let mut state = self.inner.lock().unwrap();
        let project = state.projects.get_mut(project_id)?;
        let chosen_queue = choose_queue_index(project)?;
        if project.queue.get(chosen_queue)?.request.request_id != request_id {
            return None;
        }
        let jj = JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
        let repository = Path::new(&project.queue[chosen_queue].request.repository);
        let store = project_store_dir(&orch.config_dir, repository);
        let slot_index = choose_slot(&jj, &store, project, &project.queue[chosen_queue].request)?;
        let entry = project.queue.remove(chosen_queue)?;
        let slot = &mut project.slots[slot_index];
        slot.leased = true;
        slot.persistent.lease_epoch += 1;
        slot.persistent.lifecycle = PersistentSlotLifecycle::Running;
        slot.persistent.active_request = Some(ActiveBuildSlotRequest {
            request_id: entry.request.request_id.clone(),
            attempt_id: entry.request.attempt_id.clone(),
            command: entry.request.command.clone(),
            priority: entry.request.priority,
            requesting_job_id: entry.request.requesting_job_id.clone(),
            affinity_key: entry.request.affinity_key.clone(),
            queued_at_unix_ms: entry.queued_at,
            started_at_unix_ms: Some(unix_time_ms()),
        });
        let _ = persist_state(&slot.persistent);
        Some(Lease {
            slot: slot.persistent.clone(),
        })
    }

    fn persist_preparation_fingerprint(
        &self,
        request: &BuildSlotRequest,
        lease: &Lease,
        fingerprint: String,
    ) -> Result<(), String> {
        let mut inner = self.inner.lock().unwrap();
        let slot = inner
            .projects
            .get_mut(&request.project_id)
            .and_then(|project| {
                project
                    .slots
                    .iter_mut()
                    .find(|slot| ownership_matches(slot, request, lease))
            })
            .ok_or_else(|| "build-slot lease changed while persisting preparation".to_string())?;
        slot.persistent.preparation_fingerprint = Some(fingerprint);
        persist_state(&slot.persistent)
    }

    async fn execute_leased(
        &self,
        orch: &Orchestrator,
        request: BuildSlotRequest,
        lease: Lease,
        cancel: Arc<AtomicBool>,
        batch: Option<crate::mcp::handlers::run::ResolvedRunBatch>,
    ) -> BuildSlotOutcome {
        let started = unix_time_ms();
        let path = PathBuf::from(&lease.slot.path);
        let jj = JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
        let checkout = {
            let store = project_store_dir(&orch.config_dir, Path::new(&request.repository));
            let lock = orch.jj_store_lock(&store);
            let _guard = lock.lock().await;
            recover_to_base(&jj, &path, &request.base_commit)
        };
        if let Err(error) = checkout {
            return BuildSlotOutcome::Unavailable {
                reason: BuildSlotUnavailableReason::Checkout,
                diagnostic: error,
            };
        }
        let slot_config = crate::config::settings::load_build_slots(&orch.config_dir);
        let preparation = prepare_slot(
            orch,
            &jj,
            &path,
            &request.base_commit,
            lease.slot.preparation_fingerprint.as_deref(),
            Duration::from_secs(slot_config.default_timeout_seconds),
        )
        .await;
        let preparation_fingerprint = match preparation {
            Ok(fingerprint) => fingerprint,
            Err(error) => {
                let _ = recover_to_base(&jj, &path, &request.base_commit);
                return BuildSlotOutcome::Unavailable {
                    reason: BuildSlotUnavailableReason::Preparation,
                    diagnostic: error,
                };
            }
        };
        if lease.slot.preparation_fingerprint.as_deref() != Some(&preparation_fingerprint) {
            if let Err(error) =
                self.persist_preparation_fingerprint(&request, &lease, preparation_fingerprint)
            {
                return BuildSlotOutcome::Unavailable {
                    reason: BuildSlotUnavailableReason::Preparation,
                    diagnostic: error,
                };
            }
        }
        let cwd = if request.cwd.is_empty() {
            path.clone()
        } else {
            path.join(&request.cwd)
        };
        let cwd_string = cwd.to_string_lossy().into_owned();
        let run = async {
            if let Some(batch) = batch {
                let outcomes = crate::mcp::handlers::run::execute_resolved_slot_batch(
                    orch,
                    &cwd_string,
                    batch,
                )
                .await;
                let succeeded = outcomes.iter().all(|outcome| outcome.succeeded);
                Ok(crate::mcp::handlers::run::CheckExecResult {
                    exit_code: Some(if succeeded { 0 } else { 1 }),
                    output: serde_json::to_string(&outcomes)
                        .map_err(|error| format!("serialize routed run outcomes: {error}"))?,
                    timed_out: false,
                })
            } else {
                let exports = request
                    .env
                    .iter()
                    .map(|(k, v)| format!("export {}='{}'; ", k, v.replace('\'', "'\\''")))
                    .collect::<String>();
                let command = format!("{exports}{}", request.command);
                let stream_id = format!("build-slot:{}", request.request_id);
                run_check_command(
                    orch,
                    &cwd_string,
                    &stream_id,
                    None,
                    &command,
                    request.timeout_ms,
                    true,
                )
                .await
            }
        };
        let result = tokio::select! {
            biased;
            _ = wait_cancel(cancel.clone()) => None,
            result = run => Some(result),
        };
        if result.is_none() {
            let _ = recover_to_base(&jj, &path, &request.base_commit);
            return BuildSlotOutcome::Cancelled {
                request_id: request.request_id,
                attempt_id: request.attempt_id,
            };
        }
        let exec = match result.unwrap() {
            Ok(exec) => exec,
            Err(error) => {
                return BuildSlotOutcome::Unavailable {
                    reason: BuildSlotUnavailableReason::Spawn,
                    diagnostic: error,
                }
            }
        };
        let mutation_delta = match seal_delta_after_execution(&jj, &path, &request) {
            Ok(delta) => delta,
            Err(outcome) => return *outcome,
        };
        if !self.lease_is_publishable(&request, &lease, &cancel) {
            return BuildSlotOutcome::Cancelled {
                request_id: request.request_id,
                attempt_id: request.attempt_id,
            };
        }
        BuildSlotOutcome::Completed {
            request_id: request.request_id,
            attempt_id: request.attempt_id,
            exit_code: exec.exit_code,
            output: exec.output,
            timed_out: exec.timed_out,
            metadata: BuildSlotExecutionMeta {
                slot_id: lease.slot.slot_id,
                lease_epoch: lease.slot.lease_epoch,
                started_at_unix_ms: started,
                finished_at_unix_ms: unix_time_ms(),
            },
            mutation_delta,
        }
    }

    fn lease_is_publishable(
        &self,
        request: &BuildSlotRequest,
        lease: &Lease,
        cancel: &AtomicBool,
    ) -> bool {
        let state = self.inner.lock().unwrap();
        let Some(project) = state.projects.get(&request.project_id) else {
            return false;
        };
        project.slots.iter().any(|slot| {
            ownership_matches(slot, request, lease)
                && slot.persistent.lifecycle == PersistentSlotLifecycle::Running
                && !cancel.load(Ordering::SeqCst)
        })
    }

    fn release(&self, request: &BuildSlotRequest, lease: &Lease, outcome: &BuildSlotOutcome) {
        let mut state = self.inner.lock().unwrap();
        if let Some(project) = state.projects.get_mut(&request.project_id) {
            if let Some(slot) = project
                .slots
                .iter_mut()
                .find(|slot| ownership_matches(slot, request, lease))
            {
                slot.leased = false;
                slot.persistent.lifecycle = PersistentSlotLifecycle::Idle;
                slot.persistent.active_request = None;
                slot.persistent.last_used_unix_ms = unix_time_ms();
                slot.persistent.last_affinity_key = request.affinity_key.clone();
                if let BuildSlotOutcome::Completed { mutation_delta, .. } = outcome {
                    slot.persistent.last_sealed_commit = Some(
                        mutation_delta
                            .as_ref()
                            .map(|d| d.delta_commit.as_str())
                            .unwrap_or(&request.base_commit)
                            .to_string(),
                    );
                }
                let _ = persist_state(&slot.persistent);
            }
        }
    }

    fn remove_queued(&self, request_id: &str) {
        let mut state = self.inner.lock().unwrap();
        for project in state.projects.values_mut() {
            project.queue.retain(|q| q.request.request_id != request_id);
        }
    }

    fn emit_state_change(&self, orch: &Orchestrator, action: &str) {
        let _ = orch.services.emitter.emit(
            "db-change",
            serde_json::json!({"table":"build_slots","action":action}),
        );
    }
}

#[derive(Clone)]
struct Lease {
    slot: PersistentBuildSlotState,
}

fn seal_delta_after_execution(
    jj: &JjEnv,
    path: &Path,
    request: &BuildSlotRequest,
) -> Result<Option<MutationDelta>, Box<BuildSlotOutcome>> {
    detect_and_seal_delta(jj, path, &request.base_commit).map_err(|error| {
        // The commands already ran. Report a non-fallbackable failure and
        // recover the reusable slot, rather than silently treating the tracked
        // mutation as an empty successful result.
        let recovery = recover_to_base(jj, path, &request.base_commit)
            .err()
            .map(|recovery| format!("; slot recovery also failed: {recovery}"))
            .unwrap_or_default();
        Box::new(BuildSlotOutcome::FailedAfterExecution {
            request_id: request.request_id.clone(),
            attempt_id: request.attempt_id.clone(),
            diagnostic: format!(
                "failed to inspect or seal tracked build-slot mutations after execution: {error}{recovery}"
            ),
        })
    })
}

fn ownership_matches(slot: &RuntimeSlot, request: &BuildSlotRequest, lease: &Lease) -> bool {
    slot.leased
        && slot.persistent.slot_id == lease.slot.slot_id
        && slot.persistent.lease_epoch == lease.slot.lease_epoch
        && slot
            .persistent
            .active_request
            .as_ref()
            .is_some_and(|active| {
                active.request_id == request.request_id && active.attempt_id == request.attempt_id
            })
}

fn choose_queue_index(project: &mut ProjectState) -> Option<usize> {
    if project.queue.is_empty() {
        return None;
    }
    let oldest_lower = project
        .queue
        .iter()
        .enumerate()
        .filter(|(_, q)| q.request.priority != BuildSlotPriority::AgentInteractive)
        .min_by_key(|(_, q)| q.sequence)
        .map(|(i, _)| i);
    if project.high_priority_burst >= MAX_HIGH_PRIORITY_BURST {
        if let Some(index) = oldest_lower {
            project.high_priority_burst = 0;
            return Some(index);
        }
    }
    let index = project
        .queue
        .iter()
        .enumerate()
        .max_by_key(|(_, q)| (q.request.priority, std::cmp::Reverse(q.sequence)))
        .map(|(i, _)| i)?;
    if project.queue[index].request.priority == BuildSlotPriority::AgentInteractive {
        project.high_priority_burst += 1;
    } else {
        project.high_priority_burst = 0;
    }
    Some(index)
}

fn choose_slot(
    jj: &JjEnv,
    store: &Path,
    project: &ProjectState,
    request: &BuildSlotRequest,
) -> Option<usize> {
    project
        .slots
        .iter()
        .enumerate()
        .filter(|(_, slot)| !slot.leased)
        .min_by_key(|(_, slot)| {
            let distance = slot
                .persistent
                .last_sealed_commit
                .as_deref()
                .and_then(|commit| ancestor_distance(jj, store, commit, &request.base_commit))
                .unwrap_or(u64::MAX);
            (
                request
                    .affinity_key
                    .as_ref()
                    .is_none_or(|key| slot.persistent.last_affinity_key.as_ref() != Some(key)),
                distance,
                slot.persistent.last_used_unix_ms,
                slot.persistent.slot_id.clone(),
            )
        })
        .map(|(index, _)| index)
}

fn ancestor_distance(jj: &JjEnv, store: &Path, ancestor: &str, descendant: &str) -> Option<u64> {
    if ancestor == descendant {
        return Some(0);
    }
    let revset = format!("{ancestor}::{descendant}");
    let commits = jj
        .run(
            store,
            &[
                "log",
                "-r",
                &revset,
                "--no-graph",
                "-T",
                "commit_id ++ \"\\n\"",
            ],
            "compute build-slot affinity",
        )
        .ok()?;
    let count = commits
        .lines()
        .filter(|line| !line.trim().is_empty())
        .count();
    if count >= 2 {
        Some((count - 1) as u64)
    } else {
        None
    }
}

fn validate_relative_cwd(cwd: &str) -> Result<(), String> {
    let path = Path::new(cwd);
    if path.is_absolute()
        || path.components().any(|c| {
            matches!(
                c,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        return Err(
            "build-slot cwd must be repository-relative and may not traverse parents".into(),
        );
    }
    Ok(())
}

fn reconcile_adopted_state(
    jj: &JjEnv,
    path: &Path,
    request: &BuildSlotRequest,
    persistent: &mut PersistentBuildSlotState,
) -> Result<(), String> {
    // A new runner generation must never reuse an epoch. Any state that was
    // not durably idle is recovered before it can enter scheduling.
    persistent.lease_epoch = persistent.lease_epoch.saturating_add(1);
    if persistent.lifecycle != PersistentSlotLifecycle::Idle || persistent.active_request.is_some()
    {
        persistent.lifecycle = PersistentSlotLifecycle::Recovering;
        persist_state(persistent)?;
        let recovery_base = persistent
            .last_sealed_commit
            .as_deref()
            .unwrap_or(request.base_commit.as_str());
        recover_to_base(jj, path, recovery_base)?;
        persistent.active_request = None;
        persistent.lifecycle = PersistentSlotLifecycle::Idle;
    }
    Ok(())
}

async fn prepare_slot(
    orch: &Orchestrator,
    jj: &JjEnv,
    path: &Path,
    base: &str,
    current_fingerprint: Option<&str>,
    timeout: Duration,
) -> Result<String, String> {
    let settings_path = path.join(".cairn").join("config.yaml");
    let setup_commands = if settings_path.exists() {
        let yaml = std::fs::read_to_string(&settings_path)
            .map_err(|error| format!("read project settings for slot preparation: {error}"))?;
        serde_yaml::from_str::<crate::config::project_settings::ProjectSettingsFile>(&yaml)
            .map_err(|error| format!("parse project settings for slot preparation: {error}"))?
            .setup_commands
            .unwrap_or_default()
    } else {
        Vec::new()
    };
    let fingerprint = preparation_fingerprint(jj, path, base, &setup_commands)?;
    if current_fingerprint == Some(fingerprint.as_str()) {
        return Ok(fingerprint);
    }

    let process = orch.services.process.clone();
    let worktree = path.to_path_buf();
    let commands = setup_commands.clone();
    let cancel = Arc::new(AtomicBool::new(false));
    let child_slot: Arc<Mutex<Option<Box<dyn crate::services::ChildProcess>>>> =
        Arc::new(Mutex::new(None));
    let task_cancel = cancel.clone();
    let task_child_slot = child_slot.clone();
    let mut task = tokio::task::spawn_blocking(move || {
        let sink: crate::execution::jobs::setup_progress::SetupSink = Arc::new(|_| {});
        crate::git::worktree::run_setup_commands_with_process_streaming(
            &*process,
            &worktree,
            &commands,
            &sink,
            "",
            None,
            &task_cancel,
            &task_child_slot,
        )
        .map_err(|error| error.to_string())
    });
    match tokio::time::timeout(timeout, &mut task).await {
        Ok(joined) => {
            joined.map_err(|error| format!("slot preparation task failed: {error}"))??
        }
        Err(_) => {
            cancel.store(true, Ordering::SeqCst);
            if let Some(child) = child_slot.lock().unwrap().as_mut() {
                let _ = child.kill();
            }
            let _ = task.await;
            return Err(format!(
                "slot preparation exceeded its {} second timeout",
                timeout.as_secs()
            ));
        }
    }

    let head = crate::jj::head_commit(jj, path)?;
    if head != base {
        return Err(format!(
            "slot preparation changed the checkout base from {base} to {head}"
        ));
    }
    let changed = jj.run(
        path,
        &["diff", "--summary"],
        "inspect slot preparation mutations",
    )?;
    if !changed.trim().is_empty() {
        return Err(format!(
            "slot preparation modified tracked files:\n{}",
            changed.trim()
        ));
    }
    Ok(fingerprint)
}

fn preparation_fingerprint(
    jj: &JjEnv,
    path: &Path,
    base: &str,
    setup_commands: &[String],
) -> Result<String, String> {
    let mut hasher = Sha256::new();
    hasher.update(b"cairn-slot-preparation-v1\0");
    for command in setup_commands {
        hasher.update(command.as_bytes());
        hasher.update(b"\0");
    }
    let files = jj.run(
        path,
        &["file", "list", "-r", base],
        "list dependency lockfiles for slot preparation",
    )?;
    let mut lockfiles: Vec<&str> = files
        .lines()
        .filter(|file| is_dependency_lockfile(file))
        .collect();
    lockfiles.sort_unstable();
    for relative in lockfiles {
        hasher.update(relative.as_bytes());
        hasher.update(b"\0");
        let content = std::fs::read(path.join(relative))
            .map_err(|error| format!("read tracked dependency lockfile {relative}: {error}"))?;
        hasher.update(&content);
        hasher.update(b"\0");
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn is_dependency_lockfile(path: &str) -> bool {
    matches!(
        Path::new(path).file_name().and_then(|name| name.to_str()),
        Some(
            "bun.lock"
                | "bun.lockb"
                | "Cargo.lock"
                | "package-lock.json"
                | "pnpm-lock.yaml"
                | "yarn.lock"
                | "uv.lock"
                | "poetry.lock"
                | "Pipfile.lock"
                | "Gemfile.lock"
                | "composer.lock"
                | "go.sum"
        )
    )
}

fn recover_to_base(jj: &JjEnv, path: &Path, base: &str) -> Result<(), String> {
    let _ = jj.run(
        path,
        &["abandon", "@"],
        "discard prior build-slot working copy",
    );
    jj.run(path, &["new", base], "materialize build-slot commit")?;
    Ok(())
}

fn detect_and_seal_delta(
    jj: &JjEnv,
    path: &Path,
    base: &str,
) -> Result<Option<MutationDelta>, String> {
    let changed = jj.run(path, &["diff", "--summary"], "inspect build-slot mutation")?;
    if changed.trim().is_empty() {
        return Ok(None);
    }
    jj.run(
        path,
        &["commit", "-m", "cairn: build-slot mutation delta"],
        "seal build-slot mutation",
    )?;
    let delta = jj.run(
        path,
        &["log", "-r", "@-", "--no-graph", "-T", "commit_id"],
        "resolve build-slot delta",
    )?;
    Ok(Some(MutationDelta {
        base_commit: base.to_string(),
        delta_commit: delta,
    }))
}

async fn wait_cancel(cancel: Arc<AtomicBool>) {
    while !cancel.load(Ordering::SeqCst) {
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn persist_state(state: &PersistentBuildSlotState) -> Result<(), String> {
    write_json_atomic(&Path::new(&state.path).join(".jj").join(STATE_FILE), state)
}
fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> Option<T> {
    serde_json::from_slice(&std::fs::read(path).ok()?).ok()
}

fn acquire_ownership_lock(slot_path: &Path) -> Result<std::fs::File, String> {
    let path = slot_path.join(".jj").join(LOCK_FILE);
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&path)
        .map_err(|e| format!("open build-slot ownership lock: {e}"))?;
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;
        let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if result != 0 {
            return Err(format!(
                "build slot is owned by another runner: {}",
                path.display()
            ));
        }
    }
    Ok(file)
}

fn write_json_atomic<T: Serialize>(path: &Path, value: &T) -> Result<(), String> {
    let tmp = path.with_extension(format!("tmp-{}", std::process::id()));
    let mut bytes = serde_json::to_vec_pretty(value).map_err(|e| e.to_string())?;
    bytes.push(b'\n');
    std::fs::write(&tmp, bytes).map_err(|e| e.to_string())?;
    std::fs::rename(tmp, path).map_err(|e| e.to_string())
}
fn safe_component(value: &str) -> String {
    value
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '-'
            }
        })
        .collect()
}
pub(crate) fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    fn request(id: &str, priority: BuildSlotPriority) -> BuildSlotRequest {
        BuildSlotRequest {
            request_id: id.into(),
            attempt_id: "a".into(),
            project_id: "p".into(),
            repository: "/repo".into(),
            base_commit: "abc".into(),
            command: "false".into(),
            cwd: String::new(),
            env: vec![],
            priority,
            deadline_unix_ms: 99,
            timeout_ms: 1,
            mutation_policy: MutationPolicy::PureVerdict,
            requesting_job_id: None,
            affinity_key: None,
        }
    }
    #[test]
    fn default_capacity_and_overrides_are_unconditional() {
        let defaults = BuildSlotsConfig::default();
        assert_eq!(defaults.slots_for("any-project"), 2);

        let overridden = BuildSlotsConfig {
            projects: HashMap::from([("project".to_string(), 4)]),
            global_capacity: Some(3),
            ..BuildSlotsConfig::default()
        };
        assert_eq!(overridden.slots_for("project"), 3);
        assert_eq!(overridden.slots_for("other"), 2);
    }

    #[test]
    fn dependency_lockfile_detection_is_path_independent() {
        assert!(is_dependency_lockfile("bun.lock"));
        assert!(is_dependency_lockfile("web/package-lock.json"));
        assert!(is_dependency_lockfile("src-tauri/Cargo.lock"));
        assert!(!is_dependency_lockfile("package.json"));
        assert!(!is_dependency_lockfile("Cargo.toml"));
    }

    #[test]
    fn request_and_delta_serde_round_trip() {
        let value = BuildSlotOutcome::Completed {
            request_id: "r".into(),
            attempt_id: "a".into(),
            exit_code: Some(1),
            output: "failed".into(),
            timed_out: false,
            metadata: BuildSlotExecutionMeta {
                slot_id: "s".into(),
                lease_epoch: 2,
                started_at_unix_ms: 3,
                finished_at_unix_ms: 4,
            },
            mutation_delta: Some(MutationDelta {
                base_commit: "x".into(),
                delta_commit: "d".into(),
            }),
        };
        assert_eq!(
            serde_json::from_str::<BuildSlotOutcome>(&serde_json::to_string(&value).unwrap())
                .unwrap(),
            value
        );
    }
    #[test]
    fn priority_fifo_and_bounded_aging() {
        let mut p = ProjectState::default();
        for (i, (id, pri)) in [
            ("review", BuildSlotPriority::ReviewCheck),
            ("a", BuildSlotPriority::AgentInteractive),
            ("b", BuildSlotPriority::AgentInteractive),
        ]
        .into_iter()
        .enumerate()
        {
            p.queue.push_back(QueueEntry {
                sequence: i as u64,
                queued_at: 0,
                request: request(id, pri),
            });
        }
        assert_eq!(choose_queue_index(&mut p), Some(1));
        p.queue.remove(1);
        assert_eq!(choose_queue_index(&mut p), Some(1));
        p.queue.remove(1);
        assert_eq!(choose_queue_index(&mut p), Some(0));
    }
    #[test]
    fn duplicate_request_id_registers_exactly_once() {
        let pool = BuildSlotPool::default();
        assert!(pool.register_request("same", Arc::new(AtomicBool::new(false))));
        assert!(!pool.register_request("same", Arc::new(AtomicBool::new(false))));
        assert_eq!(pool.cancellations.lock().unwrap().len(), 1);
    }

    #[test]
    fn stale_attempt_or_epoch_does_not_match_current_lease() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(temp.path().join(".jj")).unwrap();
        let lock = acquire_ownership_lock(temp.path()).unwrap();
        let active = ActiveBuildSlotRequest {
            request_id: "r".into(),
            attempt_id: "current".into(),
            command: "true".into(),
            priority: BuildSlotPriority::ReviewCheck,
            requesting_job_id: None,
            affinity_key: None,
            queued_at_unix_ms: 1,
            started_at_unix_ms: Some(2),
        };
        let persistent = PersistentBuildSlotState {
            project_id: "p".into(),
            slot_id: "s".into(),
            path: temp.path().to_string_lossy().into_owned(),
            workspace_name: "w".into(),
            repository: "/repo".into(),
            lifecycle: PersistentSlotLifecycle::Running,
            lease_epoch: 7,
            last_sealed_commit: Some("abc".into()),
            last_used_unix_ms: 1,
            last_affinity_key: None,
            preparation_fingerprint: None,
            active_request: Some(active),
        };
        let slot = RuntimeSlot {
            persistent: persistent.clone(),
            leased: true,
            _ownership_lock: lock,
        };
        let current = BuildSlotRequest {
            attempt_id: "current".into(),
            ..request("r", BuildSlotPriority::ReviewCheck)
        };
        assert!(ownership_matches(
            &slot,
            &current,
            &Lease {
                slot: persistent.clone()
            }
        ));
        let stale_attempt = BuildSlotRequest {
            attempt_id: "stale".into(),
            ..current.clone()
        };
        assert!(!ownership_matches(
            &slot,
            &stale_attempt,
            &Lease {
                slot: persistent.clone()
            }
        ));
        let mut stale_epoch = persistent;
        stale_epoch.lease_epoch = 6;
        assert!(!ownership_matches(
            &slot,
            &current,
            &Lease { slot: stale_epoch }
        ));
    }

    #[test]
    fn cancellation_after_process_completion_blocks_publication() {
        let pool = BuildSlotPool::default();
        let temp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(temp.path().join(".jj")).unwrap();
        let lock = acquire_ownership_lock(temp.path()).unwrap();
        let request = BuildSlotRequest {
            attempt_id: "a".into(),
            ..request("r", BuildSlotPriority::ReviewCheck)
        };
        let persistent = PersistentBuildSlotState {
            project_id: "p".into(),
            slot_id: "s".into(),
            path: temp.path().to_string_lossy().into_owned(),
            workspace_name: "w".into(),
            repository: "/repo".into(),
            lifecycle: PersistentSlotLifecycle::Running,
            lease_epoch: 7,
            last_sealed_commit: Some("abc".into()),
            last_used_unix_ms: 1,
            last_affinity_key: None,
            preparation_fingerprint: None,
            active_request: Some(ActiveBuildSlotRequest {
                request_id: "r".into(),
                attempt_id: "a".into(),
                command: "true".into(),
                priority: BuildSlotPriority::ReviewCheck,
                requesting_job_id: None,
                affinity_key: None,
                queued_at_unix_ms: 1,
                started_at_unix_ms: Some(2),
            }),
        };
        let lease = Lease {
            slot: persistent.clone(),
        };
        pool.inner
            .lock()
            .unwrap()
            .projects
            .entry("p".into())
            .or_default()
            .slots
            .push(RuntimeSlot {
                persistent,
                leased: true,
                _ownership_lock: lock,
            });
        let token = Arc::new(AtomicBool::new(false));
        pool.cancellations
            .lock()
            .unwrap()
            .insert("r".into(), token.clone());
        assert!(pool.cancel_request("r"));
        assert!(!pool.lease_is_publishable(&request, &lease, &token));
        let state = pool.inner.lock().unwrap();
        assert_eq!(
            state.projects["p"].slots[0].persistent.lifecycle,
            PersistentSlotLifecycle::Recovering
        );
    }

    #[test]
    fn affinity_prefers_matching_free_slot_without_idling_capacity() {
        let temp = tempfile::tempdir().unwrap();
        let mut project = ProjectState::default();
        for (slot_id, affinity, last_used) in [("match", Some("turn"), 10), ("other", None, 1)] {
            let path = temp.path().join(slot_id);
            std::fs::create_dir_all(path.join(".jj")).unwrap();
            project.slots.push(RuntimeSlot {
                persistent: PersistentBuildSlotState {
                    project_id: "p".into(),
                    slot_id: slot_id.into(),
                    path: path.to_string_lossy().into_owned(),
                    workspace_name: slot_id.into(),
                    repository: "/repo".into(),
                    lifecycle: PersistentSlotLifecycle::Idle,
                    lease_epoch: 0,
                    last_sealed_commit: None,
                    last_used_unix_ms: last_used,
                    last_affinity_key: affinity.map(str::to_string),
                    preparation_fingerprint: None,
                    active_request: None,
                },
                leased: false,
                _ownership_lock: acquire_ownership_lock(&path).unwrap(),
            });
        }
        let request = BuildSlotRequest {
            affinity_key: Some("turn".into()),
            ..request("r", BuildSlotPriority::AgentInteractive)
        };
        let jj = JjEnv::resolve("jj", temp.path());
        assert_eq!(choose_slot(&jj, temp.path(), &project, &request), Some(0));

        project.slots[0].leased = true;
        assert_eq!(choose_slot(&jj, temp.path(), &project, &request), Some(1));
    }

    #[test]
    fn snapshot_includes_queued_request_metadata() {
        let pool = BuildSlotPool::default();
        {
            let mut state = pool.inner.lock().unwrap();
            state
                .projects
                .entry("p".into())
                .or_default()
                .queue
                .push_back(QueueEntry {
                    sequence: 1,
                    queued_at: 42,
                    request: request("queued", BuildSlotPriority::ReviewCheck),
                });
        }
        let snapshot = pool.snapshot();
        assert_eq!(snapshot.queued_requests.len(), 1);
        assert_eq!(snapshot.queued_requests[0].request_id, "queued");
        assert_eq!(snapshot.queued_requests[0].queued_at_unix_ms, 42);
    }

    #[test]
    fn runner_generation_advances_idle_epoch_without_losing_affinity() {
        let temp = tempfile::tempdir().unwrap();
        let mut state = PersistentBuildSlotState {
            project_id: "p".into(),
            slot_id: "s".into(),
            path: temp.path().to_string_lossy().into_owned(),
            workspace_name: "w".into(),
            repository: "/repo".into(),
            lifecycle: PersistentSlotLifecycle::Idle,
            lease_epoch: 9,
            last_sealed_commit: Some("warm-commit".into()),
            last_used_unix_ms: 7,
            last_affinity_key: Some("turn".into()),
            preparation_fingerprint: Some("warm".into()),
            active_request: None,
        };
        let jj = JjEnv::resolve("jj", temp.path());
        reconcile_adopted_state(
            &jj,
            temp.path(),
            &request("r", BuildSlotPriority::ReviewCheck),
            &mut state,
        )
        .unwrap();
        assert_eq!(state.lease_epoch, 10);
        assert_eq!(state.last_sealed_commit.as_deref(), Some("warm-commit"));
    }

    #[test]
    fn ancestry_distance_uses_commit_graph() {
        let temp = tempfile::tempdir().unwrap();
        let repo = temp.path().join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        let run = |program: &str, args: &[&str]| {
            let status = std::process::Command::new(program)
                .args(args)
                .current_dir(&repo)
                .status()
                .unwrap();
            assert!(status.success(), "{program} {args:?}");
        };
        run("git", &["init", "-q"]);
        run("git", &["config", "user.name", "Test"]);
        run("git", &["config", "user.email", "test@example.com"]);
        std::fs::write(repo.join("file"), "one").unwrap();
        run("git", &["add", "file"]);
        run("git", &["commit", "-qm", "one"]);
        let first = std::process::Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(&repo)
            .output()
            .unwrap();
        let first = String::from_utf8(first.stdout).unwrap().trim().to_string();
        std::fs::write(repo.join("file"), "two").unwrap();
        run("git", &["commit", "-qam", "two"]);
        let second = std::process::Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(&repo)
            .output()
            .unwrap();
        let second = String::from_utf8(second.stdout).unwrap().trim().to_string();
        run("jj", &["git", "init", "--colocate"]);
        let jj = JjEnv::resolve("jj", temp.path());
        assert_eq!(ancestor_distance(&jj, &repo, &first, &second), Some(1));
        assert_eq!(ancestor_distance(&jj, &repo, &second, &first), None);
        assert_eq!(ancestor_distance(&jj, &repo, &second, &second), Some(0));
    }

    #[test]
    fn delta_seal_failure_is_non_fallbackable_after_execution() {
        let temp = tempfile::tempdir().unwrap();
        let missing_jj = temp
            .path()
            .join("missing-jj")
            .to_string_lossy()
            .into_owned();
        let jj = JjEnv::resolve(&missing_jj, temp.path());
        let request = request("delta-failure", BuildSlotPriority::AgentInteractive);
        let outcome = seal_delta_after_execution(&jj, temp.path(), &request).unwrap_err();
        match *outcome {
            BuildSlotOutcome::FailedAfterExecution {
                request_id,
                diagnostic,
                ..
            } => {
                assert_eq!(request_id, "delta-failure");
                assert!(diagnostic.contains("failed to inspect or seal"));
            }
            other => panic!("delta sealing failure must not be fallbackable: {other:?}"),
        }
    }

    #[test]
    fn completed_nonzero_is_not_unavailable() {
        let value = BuildSlotOutcome::Completed {
            request_id: "r".into(),
            attempt_id: "a".into(),
            exit_code: Some(1),
            output: "assertion failed".into(),
            timed_out: false,
            metadata: BuildSlotExecutionMeta {
                slot_id: "s".into(),
                lease_epoch: 1,
                started_at_unix_ms: 1,
                finished_at_unix_ms: 2,
            },
            mutation_delta: None,
        };
        assert!(matches!(
            value,
            BuildSlotOutcome::Completed {
                exit_code: Some(1),
                ..
            }
        ));
    }

    #[cfg(unix)]
    #[test]
    fn ownership_lock_fences_second_owner() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(temp.path().join(".jj")).unwrap();
        let first = acquire_ownership_lock(temp.path()).unwrap();
        let second = acquire_ownership_lock(temp.path());
        assert!(second.is_err());
        drop(first);
    }

    #[test]
    fn relative_cwd_rejects_escape() {
        assert!(validate_relative_cwd("src-tauri").is_ok());
        assert!(validate_relative_cwd("../secret").is_err());
        assert!(validate_relative_cwd("/tmp").is_err());
    }
}
