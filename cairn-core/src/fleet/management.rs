//! The one place this runner's fleet is managed from.
//!
//! Enrollment, configuration, and removal exist once, here. Both callers reach
//! the same code: the authenticated invoke surface the desktop app and the CLI
//! use, and the `cairn://executors` resource writes agents use. The runner owns
//! the side effects — SSH provisioning, the enrollment claim, supervision — and
//! installs itself behind [`RemoteExecutorLifecycle`]; everything above that
//! trait is request normalization, authorization, and the safety rules that must
//! hold no matter which surface asked.
//!
//! Two rules are load-bearing and belong to this module rather than to either
//! caller:
//!
//! - **Enrollment is observable.** An SSH bootstrap takes minutes and can fail
//!   at eight different places. Starting one returns an operation id
//!   immediately and the manager records the phase it is actually in, so a
//!   waiting surface renders the truth instead of a spinner.
//! - **Removal cannot cross occupied work.** Draining is what disabling a
//!   machine means; removal is refused while anything is still running, queued,
//!   or resident on it, and the check is repeated under the runner's own
//!   mutation gate so an admission cannot slip in behind it.
//!
//! Authorization belongs to each caller's own boundary, not to this module.
//! `/api/invoke` carries a person, so it requires an owner or admin. The MCP
//! callback carries a machine: it authenticates with the runner's own local
//! secret, and anything holding that secret can already run arbitrary commands
//! on this host through `run` — strictly more than enrolling an SSH executor.
//! A second gate there would refuse nothing it has not already granted, so
//! there isn't one.

use std::collections::VecDeque;
use std::sync::atomic::Ordering;
use std::sync::{Mutex, OnceLock};

use async_trait::async_trait;
use cairn_common::executor_protocol::{
    executor_names_match, normalize_executor_name, ExecutorRuntimePolicy,
};
use serde::{Deserialize, Serialize};

use super::{unix_time_ms, FleetConfig, RemoteExecutorConfig, RemoteExecutorDeclaration};
use crate::orchestrator::Orchestrator;

/// The lowest tunnel port a derived declaration will claim.
const FIRST_TUNNEL_PORT: u16 = 43_849;

/// How many finished enrollment operations are kept for inspection. Terminal
/// records are evidence, not state: an operator wants the last few failures and
/// nothing more, and a runner that has enrolled machines for a year must not be
/// holding a year of them.
const RETAINED_TERMINAL_OPERATIONS: usize = 8;

/// What a lifecycle mutation did, in the words the caller reports.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RemoteExecutorMutationResult {
    pub config: RemoteExecutorConfig,
    pub os: Option<String>,
    pub arch: Option<String>,
    pub attach_state: String,
}

/// The runner-owned side of fleet management.
///
/// Implemented by the runner's remote-executor manager, which is the only thing
/// that may drive SSH, the enrollment claim, and supervision. Everything a
/// caller does arrives through the free functions in this module, which uphold
/// the shared rules first and then delegate here.
#[async_trait]
pub trait RemoteExecutorLifecycle: Send + Sync {
    /// Provision, enroll, and attach a machine, reporting each real phase it
    /// reaches through `progress`.
    async fn add(
        &self,
        declaration: RemoteExecutorDeclaration,
        progress: EnrollmentProgress,
    ) -> Result<RemoteExecutorMutationResult, String>;

    /// Stop supervising a machine, clean it up, and revoke its enrollment.
    ///
    /// The occupancy refusal is applied before this is called AND again inside
    /// the implementation under its own mutation gate; an implementation that
    /// skips the second check has a race, not a shortcut.
    async fn remove(&self, executor_id: &str) -> Result<RemoteExecutorMutationResult, String>;

    /// Give an enrolled machine a different public address.
    ///
    /// A name is what placement requests are written in, so changing one is a
    /// fleet-wide operation rather than a label edit: the configuration, the
    /// enrollment claim, and the running executor's own advertisement all have
    /// to move together or the machine answers to two names, one of which
    /// reaches nothing.
    async fn rename(
        &self,
        executor_id: &str,
        new_name: &str,
    ) -> Result<RemoteExecutorMutationResult, String>;
}

/// Where an enrollment actually is, named at the boundaries the manager really
/// crosses.
///
/// Each variant is a place the operation can stop, which is the point: an
/// enrollment that fails at `InstallingBinary` and one that fails at
/// `AwaitingReady` ask different things of a person, and a single "failed" would
/// hide which.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum EnrollmentPhase {
    Validating,
    ProbingHost,
    ResolvingArtifact,
    InstallingBinary,
    PersistingConfiguration,
    GrantingEnrollment,
    StartingSupervision,
    AwaitingReady,
    Ready,
    CleaningUp,
    Failed,
    RetryRemoveRequired,
}

impl EnrollmentPhase {
    /// The phase in the words a surface renders.
    pub fn label(self) -> &'static str {
        match self {
            Self::Validating => "validating the request",
            Self::ProbingHost => "probing the host and its platform",
            Self::ResolvingArtifact => "resolving an executor build for it",
            Self::InstallingBinary => "installing the executor binary",
            Self::PersistingConfiguration => "persisting the enrollment configuration",
            Self::GrantingEnrollment => "granting the enrollment",
            Self::StartingSupervision => "starting supervision",
            Self::AwaitingReady => "waiting for the executor to report Ready",
            Self::Ready => "ready",
            Self::CleaningUp => "rolling back after a failure",
            Self::Failed => "failed",
            Self::RetryRemoveRequired => "failed; rollback incomplete",
        }
    }

    /// True once the operation has stopped moving on its own.
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Ready | Self::Failed | Self::RetryRemoveRequired)
    }
}

/// Whether the runner managed to undo a failed enrollment.
///
/// A clean rollback and an incomplete one are different situations for the
/// person reading them: the first leaves nothing behind, the second leaves a
/// tombstone that only a remove can clear.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum EnrollmentCleanup {
    NotApplicable,
    Complete,
    Incomplete,
}

/// One enrollment attempt, as it is happening or as it ended.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EnrollmentOperation {
    pub id: String,
    /// The public name the machine will answer to, and the name its URI uses.
    pub name: String,
    pub uri: String,
    pub phase: EnrollmentPhase,
    pub started_at_unix_ms: u64,
    pub updated_at_unix_ms: u64,
    /// The runner's own account of a failure. Never carries a credential or the
    /// ssh argument vector; a diagnostic is for a person, not for replaying a
    /// command.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub diagnostic: Option<String>,
    pub cleanup: EnrollmentCleanup,
}

impl EnrollmentOperation {
    /// How long this operation has been running, against the supplied instant.
    pub fn elapsed_ms(&self, now_unix_ms: u64) -> u64 {
        let end = if self.phase.is_terminal() {
            self.updated_at_unix_ms
        } else {
            now_unix_ms.max(self.started_at_unix_ms)
        };
        end.saturating_sub(self.started_at_unix_ms)
    }
}

/// The handle a lifecycle implementation reports progress through.
///
/// Cheap to clone and safe to hold across awaits, so the manager can record a
/// phase exactly where it crosses the boundary rather than guessing afterwards.
#[derive(Clone)]
pub struct EnrollmentProgress {
    operations: std::sync::Arc<EnrollmentOperations>,
    id: String,
}

impl EnrollmentProgress {
    /// Record the phase this enrollment has actually reached.
    pub fn phase(&self, phase: EnrollmentPhase) {
        self.operations.record(&self.id, phase, None, None);
    }

    /// Record a failure, with the runner's account of it and whether the
    /// rollback finished.
    pub fn failed(&self, diagnostic: impl Into<String>, cleanup: EnrollmentCleanup) {
        let phase = match cleanup {
            EnrollmentCleanup::Incomplete => EnrollmentPhase::RetryRemoveRequired,
            _ => EnrollmentPhase::Failed,
        };
        self.operations
            .record(&self.id, phase, Some(diagnostic.into()), Some(cleanup));
    }

    /// The operation this handle reports for.
    pub fn id(&self) -> &str {
        &self.id
    }
}

/// The bounded record of enrollment attempts this runner has made.
#[derive(Default)]
pub struct EnrollmentOperations {
    entries: Mutex<VecDeque<EnrollmentOperation>>,
}

impl EnrollmentOperations {
    /// Begin recording an enrollment for a public name.
    ///
    /// The id carries a process-unique counter, not just the clock: two
    /// enrollments of the same name can start inside one millisecond — the
    /// manager's mutation gate serializes their *work*, not their arrival — and
    /// two records sharing an id would send both progress handles to the first
    /// one, stranding the second at `validating` forever. A stranded operation
    /// is never terminal, so it is also never evicted.
    pub fn start(&self, name: &str) -> EnrollmentOperation {
        static SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let now = unix_time_ms();
        let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let operation = EnrollmentOperation {
            id: format!("enroll-{now}-{sequence}-{name}"),
            name: name.to_string(),
            uri: format!("cairn://executors/{name}"),
            phase: EnrollmentPhase::Validating,
            started_at_unix_ms: now,
            updated_at_unix_ms: now,
            diagnostic: None,
            cleanup: EnrollmentCleanup::NotApplicable,
        };
        let mut entries = self.entries.lock().unwrap();
        entries.push_back(operation.clone());
        Self::evict(&mut entries);
        operation
    }

    fn record(
        &self,
        id: &str,
        phase: EnrollmentPhase,
        diagnostic: Option<String>,
        cleanup: Option<EnrollmentCleanup>,
    ) {
        let mut entries = self.entries.lock().unwrap();
        if let Some(entry) = entries.iter_mut().find(|entry| entry.id == id) {
            entry.phase = phase;
            entry.updated_at_unix_ms = unix_time_ms();
            if let Some(diagnostic) = diagnostic {
                entry.diagnostic = Some(diagnostic);
            }
            if let Some(cleanup) = cleanup {
                entry.cleanup = cleanup;
            }
        }
        Self::evict(&mut entries);
    }

    /// Drop the oldest finished operations once there are more than the runner
    /// keeps. In-flight operations are never evicted: they are live state, and
    /// losing one would strand the surface waiting on it.
    fn evict(entries: &mut VecDeque<EnrollmentOperation>) {
        loop {
            let terminal = entries.iter().filter(|e| e.phase.is_terminal()).count();
            if terminal <= RETAINED_TERMINAL_OPERATIONS {
                return;
            }
            let Some(index) = entries.iter().position(|e| e.phase.is_terminal()) else {
                return;
            };
            entries.remove(index);
        }
    }

    /// A handle a lifecycle implementation reports phases through.
    pub fn progress(self: &std::sync::Arc<Self>, id: &str) -> EnrollmentProgress {
        EnrollmentProgress {
            operations: self.clone(),
            id: id.to_string(),
        }
    }

    /// Every recorded operation, oldest first.
    pub fn all(&self) -> Vec<EnrollmentOperation> {
        self.entries.lock().unwrap().iter().cloned().collect()
    }

    /// Operations that have not finished yet.
    pub fn in_flight(&self) -> Vec<EnrollmentOperation> {
        self.entries
            .lock()
            .unwrap()
            .iter()
            .filter(|entry| !entry.phase.is_terminal())
            .cloned()
            .collect()
    }

    /// The most recent operation for a public name, in flight or finished.
    pub fn latest_for(&self, name: &str) -> Option<EnrollmentOperation> {
        self.entries
            .lock()
            .unwrap()
            .iter()
            .rfind(|entry| executor_names_match(&entry.name, name))
            .cloned()
    }
}

/// Runner-scoped fleet-management state: who implements the lifecycle, what
/// enrollments are in flight, and whether machine-local callers may manage the
/// fleet at all.
#[derive(Default)]
pub struct ExecutorManagementState {
    lifecycle: OnceLock<std::sync::Arc<dyn RemoteExecutorLifecycle>>,
    operations: std::sync::Arc<EnrollmentOperations>,
}

impl ExecutorManagementState {
    /// Install the runner's lifecycle implementation. Called once at startup.
    pub fn install(&self, lifecycle: std::sync::Arc<dyn RemoteExecutorLifecycle>) {
        let _ = self.lifecycle.set(lifecycle);
    }

    fn lifecycle(&self) -> Result<&dyn RemoteExecutorLifecycle, String> {
        self.lifecycle
            .get()
            .map(|lifecycle| lifecycle.as_ref())
            .ok_or_else(|| "remote executors are unavailable on this host".to_string())
    }

    /// Enrollment operations this runner has recorded.
    pub fn operations(&self) -> &std::sync::Arc<EnrollmentOperations> {
        &self.operations
    }
}

/// What is on a machine right now, in the terms a removal refusal states.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ExecutorOccupancy {
    pub running: usize,
    pub queued: usize,
    pub resident_cells: usize,
    pub resident_processes: usize,
}

impl ExecutorOccupancy {
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }

    /// The occupancy in the words a refusal uses. Counts only: the holders
    /// behind them are opaque runner identities, and naming one would put an
    /// address in front of an operator that addresses nothing.
    pub fn summary(&self) -> String {
        let mut parts = Vec::new();
        if self.running > 0 {
            parts.push(format!("{} running", self.running));
        }
        if self.queued > 0 {
            parts.push(format!("{} queued", self.queued));
        }
        if self.resident_cells > 0 {
            parts.push(format!(
                "{} resident execution environment(s)",
                self.resident_cells
            ));
        }
        if self.resident_processes > 0 {
            parts.push(format!("{} resident process(es)", self.resident_processes));
        }
        if parts.is_empty() {
            "nothing".to_string()
        } else {
            parts.join(", ")
        }
    }
}

/// What is currently on one machine, from the authoritative fleet snapshot.
pub fn occupancy(orch: &Orchestrator, executor_id: &str) -> ExecutorOccupancy {
    let inspections = orch.fleet.inspect_executors(unix_time_ms());
    let Some(inspection) = inspections
        .iter()
        .find(|inspection| inspection.health.identity.executor_id == executor_id)
    else {
        // A machine that is not attached is running nothing by definition: the
        // work it could hold lives in its own process, and there is no process.
        return ExecutorOccupancy::default();
    };
    let snapshot = &inspection.occupancy;
    ExecutorOccupancy {
        running: snapshot.executing_requests.len(),
        queued: snapshot.queued_requests.len(),
        resident_cells: snapshot
            .cells
            .iter()
            .filter(|cell| cell.residency.is_some())
            .count(),
        resident_processes: snapshot
            .cells
            .iter()
            .map(|cell| cell.occupancy.processes.len())
            .sum(),
    }
}

/// Refuse a removal while the machine still holds work.
///
/// Removal stops supervision, cleans the remote, revokes the enrollment, and
/// disconnects the generation. Doing that underneath running work does not
/// stop the work politely — it strands it. So the answer is a refusal that says
/// what is still there and what to do about it, and the drain the operator then
/// enables is what actually empties the machine.
pub fn refuse_removal_while_occupied(
    orch: &Orchestrator,
    executor_id: &str,
    name: &str,
) -> Result<(), String> {
    removal_refusal(occupancy(orch, executor_id), name)
}

/// The removal rule itself, over an occupancy reading.
///
/// Separated from the snapshot it reads so the rule can be stated once and
/// checked against each kind of occupancy independently: running work, queued
/// work, and residency each refuse on their own, and none of them is allowed to
/// be the one that quietly does not.
pub fn removal_refusal(occupancy: ExecutorOccupancy, name: &str) -> Result<(), String> {
    if occupancy.is_empty() {
        return Ok(());
    }
    Err(format!(
        "{name} still has {}. Enable draining first so it stops accepting new work, wait for what is on it to finish, then remove it. Removing it now would strand that work.",
        occupancy.summary()
    ))
}

/// Resolve whatever a caller addressed a machine by — its public name, or the
/// internal identity older surfaces hold — to the identity manager calls take.
///
/// Operators and agents address machines by name; only the runner's own
/// machinery needs the identity. A reference that addresses nothing is refused
/// with the ones that do.
pub fn resolve_executor_reference(orch: &Orchestrator, reference: &str) -> Result<String, String> {
    let configured = crate::config::settings::load_fleet(&orch.config_dir);
    if let Some(config) = configured.remote_executors.values().find(|config| {
        executor_names_match(&config.display_name, reference) || config.executor_id == reference
    }) {
        return Ok(config.executor_id.clone());
    }
    // Attached machines include the runner's own colocated executor, which has
    // no remote declaration but is still a live target for drain and policy.
    let attached = orch.fleet.inspect_executors(unix_time_ms());
    if let Some(inspection) = attached.iter().find(|inspection| {
        executor_names_match(&inspection.name, reference)
            || inspection.health.identity.executor_id == reference
    }) {
        return Ok(inspection.health.identity.executor_id.clone());
    }
    let mut known: Vec<_> = configured
        .remote_executors
        .values()
        .filter_map(|config| normalize_executor_name(&config.display_name))
        .chain(attached.iter().map(|inspection| inspection.name.clone()))
        .collect();
    known.sort();
    known.dedup();
    let known = if known.is_empty() {
        "no executor is configured".to_string()
    } else {
        known.join(", ")
    };
    Err(format!(
        "no executor is named {reference}. Known executors: {known}. Read cairn://executors for live state."
    ))
}

/// The public name a machine is addressed by, for messages about it.
pub fn public_name(orch: &Orchestrator, executor_id: &str) -> String {
    let configured = crate::config::settings::load_fleet(&orch.config_dir);
    configured
        .remote_executors
        .get(executor_id)
        .and_then(|config| normalize_executor_name(&config.display_name))
        .or_else(|| orch.fleet.executor_public_name(executor_id))
        .unwrap_or_else(|| executor_id.to_string())
}

/// An enrollment request, in the shape both surfaces accept.
///
/// Only the host and the SSH user are required. Everything else is derived, and
/// an omitted field means "derive it" while a blank one is a value the runner
/// rejects — which is why every optional field is an `Option<String>` rather
/// than a defaulted `String`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EnrollmentRequest {
    pub host: String,
    pub ssh_user: String,
    pub binary_path: Option<String>,
    pub cairn_home: Option<String>,
    pub executor_id: Option<String>,
    pub device_id: Option<String>,
    pub display_name: Option<String>,
    /// Project keys this machine serves. Omitted or empty means every project;
    /// a nonempty list restricts it to exactly those.
    pub project_keys: Option<Vec<String>>,
    pub tunnel_port: Option<u16>,
    pub extra_ssh_args: Option<Vec<String>>,
}

/// What starting an enrollment answers with, immediately.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EnrollmentStarted {
    pub operation_id: String,
    /// The name the machine will answer to, and the URI it will be readable at.
    pub name: String,
    pub uri: String,
}

/// Derive the identity, paths, and tunnel port a minimal request leaves out.
///
/// A repeated add of an already-configured machine reconstructs its persisted
/// declaration rather than deriving a second one, so re-running an enrollment is
/// idempotent instead of allocating a fresh port and a conflicting identity.
pub fn declaration_defaults(
    request: &EnrollmentRequest,
    configured: &FleetConfig,
) -> Result<RemoteExecutorDeclaration, String> {
    let derived_executor_id = safe_executor_id(&request.host);
    let executor_id = request
        .executor_id
        .as_deref()
        .unwrap_or(&derived_executor_id)
        .to_string();
    if executor_id.is_empty() {
        return Err("host does not contain a usable executor identity".into());
    }
    if let Some(existing) = configured.remote_executors.get(&executor_id) {
        return Ok(RemoteExecutorDeclaration {
            host: existing.host.clone(),
            ssh_user: existing.ssh_user.clone(),
            binary_path: Some(existing.binary_path.clone()),
            cairn_home: Some(existing.cairn_home.clone()),
            executor_id: existing.executor_id.clone(),
            device_id: existing.device_id.clone(),
            display_name: existing.display_name.clone(),
            project_ids: existing.project_ids.clone(),
            tunnel_port: existing.tunnel_port,
            extra_ssh_args: existing.extra_ssh_args.clone(),
        });
    }
    let used = configured
        .remote_executors
        .values()
        .map(|config| config.tunnel_port)
        .collect::<std::collections::HashSet<_>>();
    let tunnel_port = (FIRST_TUNNEL_PORT..=u16::MAX)
        .find(|port| !used.contains(port))
        .ok_or_else(|| "no unused remote tunnel port is available".to_string())?;
    Ok(RemoteExecutorDeclaration {
        host: request.host.clone(),
        ssh_user: request.ssh_user.clone(),
        binary_path: None,
        cairn_home: None,
        executor_id: executor_id.clone(),
        device_id: format!("{executor_id}-device"),
        display_name: executor_id,
        project_ids: Vec::new(),
        tunnel_port,
        extra_ssh_args: Vec::new(),
    })
}

/// A filename- and identity-safe executor id derived from a host.
pub fn safe_executor_id(host: &str) -> String {
    let id = host
        .trim_end_matches(dot)
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '-' {
                character.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();
    id.trim_matches('-').to_string()
}

fn dot(character: char) -> bool {
    character == '.'
}

/// Resolve project keys to the project ids a declaration carries.
///
/// An empty or omitted selection means every project: eligibility is the
/// default, and a nonempty list is a restriction the operator chose.
pub async fn resolve_project_ids(
    orch: &Orchestrator,
    keys: &[String],
) -> Result<Vec<String>, String> {
    let mut project_ids = Vec::new();
    for key in keys {
        let mut matches = Vec::new();
        for db in orch.db.all_dbs().await {
            for project in crate::projects::crud::list_db(&db)
                .await
                .map_err(|error| error.to_string())?
            {
                if project.key.eq_ignore_ascii_case(key) && project.is_workspace == 0 {
                    matches.push(project.id);
                }
            }
        }
        matches.sort();
        matches.dedup();
        if matches.len() != 1 {
            let reason = if matches.is_empty() {
                "unknown"
            } else {
                "ambiguous"
            };
            return Err(format!("project key '{key}' is {reason}"));
        }
        project_ids.push(matches.remove(0));
    }
    Ok(project_ids)
}

/// Turn a request into the declaration the runner enrolls, resolving project
/// keys and filling in everything the caller left out.
pub async fn build_declaration(
    orch: &Orchestrator,
    request: EnrollmentRequest,
) -> Result<RemoteExecutorDeclaration, String> {
    let configured = crate::config::settings::load_fleet(&orch.config_dir);
    let defaults = declaration_defaults(&request, &configured)?;
    let project_ids = match request.project_keys.as_deref() {
        Some(keys) => resolve_project_ids(orch, keys).await?,
        None => defaults.project_ids.clone(),
    };
    Ok(RemoteExecutorDeclaration {
        host: request.host,
        ssh_user: request.ssh_user,
        binary_path: request.binary_path.or(defaults.binary_path),
        cairn_home: request.cairn_home.or(defaults.cairn_home),
        executor_id: request.executor_id.unwrap_or(defaults.executor_id),
        device_id: request.device_id.unwrap_or(defaults.device_id),
        display_name: request.display_name.unwrap_or(defaults.display_name),
        project_ids,
        tunnel_port: request.tunnel_port.unwrap_or(defaults.tunnel_port),
        extra_ssh_args: request.extra_ssh_args.unwrap_or(defaults.extra_ssh_args),
    })
}

/// Start enrolling a machine and answer with the operation to watch.
///
/// The SSH bootstrap runs on its own task: it probes a host, may install a
/// binary, and waits for the executor to report Ready, which is minutes of work
/// no caller should be held open for. What comes back immediately is the
/// operation id and the URI the machine will be readable at, and the phases
/// arrive on the operation as the manager actually reaches them.
pub async fn enroll(
    orch: &Orchestrator,
    request: EnrollmentRequest,
) -> Result<EnrollmentStarted, String> {
    let declaration = build_declaration(orch, request).await?;
    let name = normalize_executor_name(&declaration.display_name)
        .unwrap_or_else(|| declaration.display_name.clone());
    let management = orch.fleet.management();
    // Fail before an operation exists if there is nothing to run it, so a
    // caller is not handed an id that will never move.
    management.lifecycle()?;
    let operation = management.operations.start(&name);
    let progress = management.operations.progress(&operation.id);
    let started = EnrollmentStarted {
        operation_id: operation.id.clone(),
        name: operation.name.clone(),
        uri: operation.uri.clone(),
    };
    let lifecycle = management
        .lifecycle
        .get()
        .expect("the lifecycle was present a moment ago")
        .clone();
    let emitter = orch.services.emitter.clone();
    tokio::spawn(async move {
        let outcome = lifecycle.add(declaration, progress.clone()).await;
        match outcome {
            Ok(_) => progress.phase(EnrollmentPhase::Ready),
            Err(error) => {
                // "Retry Remove" is the manager's own words for a rollback it
                // could not finish, and it is the difference between an
                // enrollment that left nothing behind and one that did.
                let cleanup = if error.contains("Retry Remove") {
                    EnrollmentCleanup::Incomplete
                } else {
                    EnrollmentCleanup::Complete
                };
                progress.failed(error, cleanup);
            }
        }
        let _ = emitter.emit_empty(ENROLLMENT_CHANGED_EVENT);
    });
    let _ = orch.services.emitter.emit_empty(ENROLLMENT_CHANGED_EVENT);
    Ok(started)
}

/// The app event that says enrollment state moved. A hint, not the state: a
/// surface that receives one rereads the operation and the fleet.
pub const ENROLLMENT_CHANGED_EVENT: &str = "executor-enrollment-changed";

/// Give an enrolled machine a different public name.
pub async fn rename(
    orch: &Orchestrator,
    reference: &str,
    new_name: &str,
) -> Result<RemoteExecutorMutationResult, String> {
    let executor_id = resolve_executor_reference(orch, reference)?;
    let result = orch
        .fleet
        .management()
        .lifecycle()?
        .rename(&executor_id, new_name)
        .await?;
    let _ = orch.services.emitter.emit_empty(ENROLLMENT_CHANGED_EVENT);
    Ok(result)
}

/// Remove a machine and revoke its enrollment, once it is empty.
pub async fn remove(
    orch: &Orchestrator,
    reference: &str,
) -> Result<RemoteExecutorMutationResult, String> {
    let executor_id = resolve_executor_reference(orch, reference)?;
    let name = public_name(orch, &executor_id);
    refuse_removal_while_occupied(orch, &executor_id, &name)?;
    let result = orch
        .fleet
        .management()
        .lifecycle()?
        .remove(&executor_id)
        .await?;
    let _ = orch.services.emitter.emit_empty(ENROLLMENT_CHANGED_EVENT);
    Ok(result)
}

/// Apply a runtime policy to a machine, persisting it and applying it live.
///
/// The settings write happens first so a restart keeps the policy, and is rolled
/// back if the live application is refused — a persisted policy the running
/// executor never accepted is a lie the next reader would believe.
pub async fn set_runtime_policy(
    orch: &Orchestrator,
    reference: &str,
    expected_generation: u64,
    policy: ExecutorRuntimePolicy,
) -> Result<ExecutorRuntimePolicy, String> {
    policy.validate()?;
    let executor_id = resolve_executor_reference(orch, reference)?;
    ensure_current_generation(orch, &executor_id, expected_generation)?;

    let mut config = crate::config::settings::load_fleet(&orch.config_dir);
    let previous = config
        .executor_policies
        .insert(executor_id.clone(), policy.clone());
    crate::config::settings::set_fleet(&orch.config_dir, &config)?;

    match orch
        .fleet
        .set_executor_runtime_policy(&executor_id, expected_generation, policy)
        .await
    {
        Ok(applied) => Ok(applied),
        Err(error) => {
            match previous {
                Some(previous) => {
                    config
                        .executor_policies
                        .insert(executor_id.clone(), previous);
                }
                None => {
                    config.executor_policies.remove(&executor_id);
                }
            }
            let _ = crate::config::settings::set_fleet(&orch.config_dir, &config);
            Err(error)
        }
    }
}

/// Turn draining on or off. This is what disabling a machine means: it refuses
/// new admissions and leaves what is already on it alone. It is live and
/// generation-fenced, so a reconnect or a runner restart loses it.
pub async fn set_drain_mode(
    orch: &Orchestrator,
    reference: &str,
    expected_generation: u64,
    enabled: bool,
) -> Result<bool, String> {
    let executor_id = resolve_executor_reference(orch, reference)?;
    orch.fleet
        .set_executor_drain_mode(&executor_id, expected_generation, enabled)
        .await
}

/// Refuse a fenced control whose generation is not the one that is live.
pub fn ensure_current_generation(
    orch: &Orchestrator,
    executor_id: &str,
    expected_generation: u64,
) -> Result<(), String> {
    let current = orch
        .fleet
        .executor_health(unix_time_ms())
        .into_iter()
        .find(|executor| executor.identity.executor_id == executor_id)
        .map(|executor| executor.connection_generation);
    if current != Some(expected_generation) {
        return Err("executor connection generation is stale".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(host: &str) -> EnrollmentRequest {
        EnrollmentRequest {
            host: host.into(),
            ssh_user: "dev".into(),
            ..EnrollmentRequest::default()
        }
    }

    #[test]
    fn executor_identity_default_is_filename_safe() {
        assert_eq!(safe_executor_id("BGLab-UB.local"), "bglab-ub-local");
    }

    #[test]
    fn repeated_minimal_enrollment_reuses_the_persisted_declaration() {
        let first = declaration_defaults(&request("builder.local"), &FleetConfig::default())
            .expect("initial defaults resolve");
        let mut configured = FleetConfig::default();
        configured.remote_executors.insert(
            first.executor_id.clone(),
            RemoteExecutorConfig {
                host: first.host.clone(),
                ssh_user: first.ssh_user.clone(),
                platform: super::super::RemotePlatform::LinuxX86_64,
                binary_path: "/home/dev/.local/bin/cairn-executor".into(),
                cairn_home: "/home/dev/.cairn-executor".into(),
                executor_id: first.executor_id.clone(),
                device_id: first.device_id.clone(),
                display_name: first.display_name.clone(),
                project_ids: first.project_ids.clone(),
                tunnel_port: first.tunnel_port,
                extra_ssh_args: first.extra_ssh_args.clone(),
            },
        );

        let repeated = declaration_defaults(&request("builder.local"), &configured)
            .expect("repeated defaults resolve");

        assert_eq!(first.binary_path, None);
        assert_eq!(repeated.tunnel_port, first.tunnel_port);
        assert_eq!(
            repeated.binary_path.as_deref(),
            Some("/home/dev/.local/bin/cairn-executor")
        );
    }

    /// An enrollment that is still working is live state; a finished one is
    /// evidence. The registry keeps every one of the first and a bounded window
    /// of the second, so a long-lived runner does not accumulate them.
    #[test]
    fn finished_operations_are_bounded_and_in_flight_ones_are_never_evicted() {
        let operations = EnrollmentOperations::default();
        let live = operations.start("still-going");
        for index in 0..(RETAINED_TERMINAL_OPERATIONS + 4) {
            let operation = operations.start(&format!("done-{index}"));
            operations.record(&operation.id, EnrollmentPhase::Ready, None, None);
        }

        let all = operations.all();
        assert_eq!(
            all.iter().filter(|entry| entry.phase.is_terminal()).count(),
            RETAINED_TERMINAL_OPERATIONS
        );
        assert!(
            all.iter().any(|entry| entry.id == live.id),
            "an unfinished enrollment is never evicted"
        );
        assert_eq!(operations.in_flight().len(), 1);
    }

    /// Two enrollments of the same name starting in the same millisecond must
    /// remain two distinguishable operations. Sharing an id sent both progress
    /// handles to whichever was recorded first, leaving the other pinned at its
    /// opening phase and, because that is not terminal, never evicted.
    #[test]
    fn same_name_enrollments_started_together_stay_distinguishable() {
        let operations = std::sync::Arc::new(EnrollmentOperations::default());
        let first = operations.start("bglab-ub");
        let second = operations.start("bglab-ub");
        assert_ne!(first.id, second.id);

        operations
            .progress(&second.id)
            .phase(EnrollmentPhase::ProbingHost);

        let recorded = operations.all();
        let first = recorded.iter().find(|e| e.id == first.id).expect("kept");
        let second = recorded.iter().find(|e| e.id == second.id).expect("kept");
        assert_eq!(first.phase, EnrollmentPhase::Validating);
        assert_eq!(
            second.phase,
            EnrollmentPhase::ProbingHost,
            "a progress handle moves its own operation, not the first one sharing a name"
        );
    }

    #[test]
    fn a_failure_that_could_not_roll_back_is_a_different_state_than_one_that_did() {
        let operations = std::sync::Arc::new(EnrollmentOperations::default());
        let clean = operations.start("clean");
        let dirty = operations.start("dirty");
        let handle = |id: &str| operations.progress(id);

        handle(&clean.id).failed("host refused the key", EnrollmentCleanup::Complete);
        handle(&dirty.id).failed(
            "initial enrollment failed; rollback incomplete. Retry Remove.",
            EnrollmentCleanup::Incomplete,
        );

        let recorded = operations.all();
        let clean = recorded.iter().find(|e| e.id == clean.id).expect("kept");
        let dirty = recorded.iter().find(|e| e.id == dirty.id).expect("kept");
        assert_eq!(clean.phase, EnrollmentPhase::Failed);
        assert_eq!(clean.cleanup, EnrollmentCleanup::Complete);
        assert_eq!(dirty.phase, EnrollmentPhase::RetryRemoveRequired);
        assert_eq!(dirty.cleanup, EnrollmentCleanup::Incomplete);
    }

    #[test]
    fn every_phase_reads_as_a_place_the_work_actually_is() {
        for phase in [
            EnrollmentPhase::Validating,
            EnrollmentPhase::ProbingHost,
            EnrollmentPhase::ResolvingArtifact,
            EnrollmentPhase::InstallingBinary,
            EnrollmentPhase::PersistingConfiguration,
            EnrollmentPhase::GrantingEnrollment,
            EnrollmentPhase::StartingSupervision,
            EnrollmentPhase::AwaitingReady,
            EnrollmentPhase::CleaningUp,
        ] {
            assert!(!phase.is_terminal(), "{phase:?} is a step, not an ending");
            assert!(!phase.label().is_empty());
        }
        for phase in [
            EnrollmentPhase::Ready,
            EnrollmentPhase::Failed,
            EnrollmentPhase::RetryRemoveRequired,
        ] {
            assert!(phase.is_terminal());
        }
    }

    /// A lifecycle that was never installed is a refusal with a reason, not a
    /// panic and not a silent no-op: a host without remote execution says so.
    #[test]
    fn an_uninstalled_lifecycle_refuses_rather_than_pretending() {
        let state = ExecutorManagementState::default();
        let error = state
            .lifecycle()
            .err()
            .expect("an uninstalled lifecycle refuses");
        assert!(error.contains("unavailable"), "{error}");
    }

    /// Each kind of occupancy refuses on its own. A rule that only caught
    /// running commands would let a removal cross a resident dev instance,
    /// which is exactly the work that has no other way to survive it.
    #[test]
    fn every_kind_of_occupancy_refuses_removal_on_its_own() {
        for occupancy in [
            ExecutorOccupancy {
                running: 1,
                ..ExecutorOccupancy::default()
            },
            ExecutorOccupancy {
                queued: 1,
                ..ExecutorOccupancy::default()
            },
            ExecutorOccupancy {
                resident_cells: 1,
                ..ExecutorOccupancy::default()
            },
            ExecutorOccupancy {
                resident_processes: 1,
                ..ExecutorOccupancy::default()
            },
        ] {
            let refusal = removal_refusal(occupancy, "bglab-ub")
                .err()
                .unwrap_or_else(|| panic!("{occupancy:?} must refuse removal"));
            assert!(refusal.contains("bglab-ub"), "{refusal}");
            assert!(
                refusal.contains("draining"),
                "a refusal has to name the way out of it: {refusal}"
            );
        }
    }

    /// Once the machine is empty — which is what draining produces — removal
    /// proceeds. There is no timeout and no force: emptiness is the condition.
    #[test]
    fn an_empty_machine_may_be_removed() {
        assert!(removal_refusal(ExecutorOccupancy::default(), "bglab-ub").is_ok());
    }

    #[test]
    fn occupancy_states_what_is_there_without_naming_a_holder() {
        let occupied = ExecutorOccupancy {
            running: 1,
            queued: 2,
            resident_cells: 1,
            resident_processes: 3,
        };
        assert!(!occupied.is_empty());
        let summary = occupied.summary();
        assert!(summary.contains("1 running"), "{summary}");
        assert!(summary.contains("2 queued"), "{summary}");
        assert!(
            summary.contains("1 resident execution environment"),
            "{summary}"
        );
        assert!(summary.contains("3 resident process"), "{summary}");
        assert!(ExecutorOccupancy::default().is_empty());
    }
}
