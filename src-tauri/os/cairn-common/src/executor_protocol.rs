//! Versioned wire contract between the runner and enrolled executors.
//!
//! Build-cell requests are immutable. Cancellation is deliberately represented
//! as a separate control message so dropping a runner-side waiter cannot mutate
//! or ambiguously replay an admitted request.

use crate::protocol::{CallbackRequest, CallbackResponse};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::Path;
use std::{collections::HashMap, future::Future};

/// Process exit code used when an executor has been told it cannot attach.
///
/// This is a process-boundary contract between `cairn-executor` and the local
/// runner supervisor. Other failures remain ordinary non-zero exits and retain
/// the supervisor's restart pacing.
pub const EXECUTOR_PERMANENT_REFUSAL_EXIT_CODE: u8 = 78;

/// Process exit code requesting that the resident service be relaunched.
///
/// This is deliberately non-zero so failure-only launchd and systemd policies
/// restart the executor, and distinct from permanent refusal so the supervisor
/// retains its ordinary retry behavior.
pub const EXECUTOR_RESTART_EXIT_CODE: u8 = 75;

/// Process exit code for an executor that could not take its Cairn home's
/// executor generation because another live process still holds it.
///
/// Split from an ordinary failure because retrying cannot change it: the holder
/// is the same process until it exits. A supervisor answers this by naming that
/// holder and waiting for it or retiring it, never by backing off and spawning
/// into the same held lock on a cadence.
pub const EXECUTOR_GENERATION_HELD_EXIT_CODE: u8 = 69;

/// Environment and argument names read directly by the independently installed
/// executor. Runner launch and probe builders use these same constants so one
/// side cannot silently rename an input without the other.
pub const EXECUTOR_RUNNER_URL_ENV: &str = "CAIRN_EXECUTOR_RUNNER_URL";
/// Files a resident executor reads out of its own Cairn home, and that the SSH
/// bootstrap ceremony writes there. Named here for the same reason the
/// environment variables are: the ceremony and the daemon are on opposite sides
/// of a machine boundary, and a filename either side could rename alone is a
/// contract nothing checks.
pub const EXECUTOR_CONFIG_FILE_NAME: &str = "executor.json";
pub const EXECUTOR_CREDENTIAL_FILE_NAME: &str = "executor_credential";
/// Where a resident executor records why it last stopped on an error, in the
/// directory holding its config file. A service-managed daemon's stderr lands
/// in a log the runner does not read; this record is the same fact in a shape
/// the bootstrap ceremony can ask for over the ssh session it already holds.
pub const EXECUTOR_LAST_EXIT_FILE_NAME: &str = "executor_last_exit.json";
pub const EXECUTOR_AUTH_TOKEN_ENV: &str = "CAIRN_MCP_AUTH_TOKEN";
pub const EXECUTOR_CAIRN_HOME_ENV: &str = "CAIRN_HOME";
pub const EXECUTOR_ID_ENV: &str = "CAIRN_EXECUTOR_ID";
pub const EXECUTOR_DEVICE_ID_ENV: &str = "CAIRN_EXECUTOR_DEVICE_ID";
pub const EXECUTOR_DISPLAY_NAME_ENV: &str = "CAIRN_EXECUTOR_DISPLAY_NAME";
pub const EXECUTOR_PROJECTS_ENV: &str = "CAIRN_EXECUTOR_PROJECTS";
pub const EXECUTOR_ENROLLMENT_TOKEN_ENV: &str = "CAIRN_EXECUTOR_ENROLLMENT_TOKEN";
pub const EXECUTOR_RUNNER_DEVICE_ID_ENV: &str = "CAIRN_EXECUTOR_RUNNER_DEVICE_ID";
/// The value a supervisor hands one launch of an executor, so that launch can
/// later be told apart from any other process running the same binary. A
/// colocated executor presents it in its enrollment; the runner admits only the
/// one it last issued.
pub const EXECUTOR_SUPERVISION_NONCE_ENV: &str = "CAIRN_EXECUTOR_SUPERVISION_NONCE";
pub const EXECUTOR_DISTRIBUTION_INFO_ARG: &str = "--distribution-info";

/// Receives and accepts one executor Ready message before starting work that is
/// explicitly outside the readiness boundary.
pub async fn accept_ready_then<T, R, E, Receive, Accept, PostReady>(
    receive: Receive,
    accept: Accept,
    post_ready: PostReady,
) -> Result<R, E>
where
    Receive: Future<Output = Result<T, E>>,
    Accept: FnOnce(T) -> Result<R, E>,
    PostReady: FnOnce(),
{
    let message = receive.await?;
    let accepted = accept(message)?;
    post_ready();
    Ok(accepted)
}

/// Verifies downloaded executor bytes against the artifact manifest.
pub fn verify_bytes(bytes: &[u8], artifact: &ExecutorArtifact) -> Result<(), String> {
    let found = hex::encode(Sha256::digest(bytes));
    let target = &artifact.target;
    if bytes.len() as u64 != artifact.size {
        return Err(format!(
            "{target} executor artifact size mismatch: manifest declares {} bytes sha256 {}, found {} bytes sha256 {found}",
            artifact.size,
            artifact.sha256,
            bytes.len()
        ));
    }
    if found != artifact.sha256 {
        return Err(format!(
            "{target} executor artifact checksum mismatch: manifest declares sha256 {}, found {found} (both {} bytes)",
            artifact.sha256, artifact.size
        ));
    }
    Ok(())
}

/// Reads and verifies an executor artifact from disk.
pub async fn verify_file(path: &Path, artifact: &ExecutorArtifact) -> Result<(), String> {
    let bytes = tokio::fs::read(path)
        .await
        .map_err(|error| error.to_string())?;
    verify_bytes(&bytes, artifact)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum UpdateState {
    Pending,
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct UpdateReport {
    pub state: UpdateState,
    pub from_build: String,
    pub to_distribution_id: String,
    pub to_build: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cause: Option<String>,
    pub at_unix_ms: u64,
}

/// A published executor target and the canonical closed platform vocabulary.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "kebab-case")]
pub enum RemotePlatform {
    #[default]
    LinuxX86_64,
    LinuxArm64,
    WindowsX86_64,
    DarwinArm64,
}

impl RemotePlatform {
    pub const fn os(self) -> &'static str {
        match self {
            Self::LinuxX86_64 | Self::LinuxArm64 => "linux",
            Self::WindowsX86_64 => "windows",
            Self::DarwinArm64 => "macos",
        }
    }

    pub const fn arch(self) -> &'static str {
        match self {
            Self::LinuxX86_64 | Self::WindowsX86_64 => "x86_64",
            Self::LinuxArm64 | Self::DarwinArm64 => "aarch64",
        }
    }

    pub const fn target(self) -> &'static str {
        match self {
            Self::LinuxX86_64 => "x86_64-unknown-linux-gnu",
            Self::LinuxArm64 => "aarch64-unknown-linux-gnu",
            Self::WindowsX86_64 => "x86_64-pc-windows-msvc",
            Self::DarwinArm64 => "aarch64-apple-darwin",
        }
    }

    /// The platform this process is running on, or `None` where Cairn publishes
    /// no executor artifact.
    ///
    /// `None` is meaningful rather than defensive: containment evidence binds to
    /// a closed platform identity, so a machine outside the published set has
    /// nothing to bind to and reports no measurements at all rather than
    /// inventing a name that no placement rule knows how to compare.
    pub fn current() -> Option<Self> {
        match (std::env::consts::OS, std::env::consts::ARCH) {
            ("linux", "x86_64") => Some(Self::LinuxX86_64),
            ("linux", "aarch64") => Some(Self::LinuxArm64),
            ("windows", "x86_64") => Some(Self::WindowsX86_64),
            ("macos", "aarch64") => Some(Self::DarwinArm64),
            _ => None,
        }
    }

    pub fn is_absolute(self, path: &str) -> bool {
        match self {
            Self::LinuxX86_64 | Self::LinuxArm64 | Self::DarwinArm64 => path.starts_with('/'),
            Self::WindowsX86_64 => {
                let bytes = path.as_bytes();
                (bytes.len() >= 3
                    && bytes[0].is_ascii_alphabetic()
                    && bytes[1] == b':'
                    && matches!(bytes[2], b'\\' | b'/'))
                    || path.starts_with(r"\\")
            }
        }
    }
}

/// Protocol intent for a resident process that must use the spawning machine's
/// default shell. This is deliberately not a filesystem path: resolving a path
/// before placement would couple the process to the runner's platform.
pub const RESIDENT_PROCESS_DEFAULT_SHELL: &str = "cairn:default-shell";
/// Protocol intent for the command flag accepted by the resolved shell.
pub const RESIDENT_PROCESS_SHELL_COMMAND_FLAG: &str = "cairn:shell-command-flag";

/// The compact result of shadow-ranking a pinned batch as hermetic work.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PlacementCounterfactualWinner {
    pub executor_name: String,
    pub executor_id: String,
    pub reason: PlacementReason,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prediction: Option<PlacementPrediction>,
}

/// One bounded, generation-aware transition in an enrolled remote's link.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RemoteConnectionTransition {
    pub occurred_at_unix_ms: u64,
    pub phase: RemoteConnectionPhase,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generation: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_process_exit_status: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_stderr: Option<String>,
    /// How big the executor process was the last time the generation named
    /// above reported, present only on a transition that ended a generation
    /// which had reported at least once.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_at_generation_end: Option<GenerationEndMemory>,
}

/// The last process telemetry a connection generation reported, kept by the
/// runner rather than by the machine that reported it.
///
/// An executor's own memory readings live in its process and reach the runner
/// on a heartbeat, so a kill takes the last reading with it and the machine
/// that comes back reports a fresh process starting from nothing. That is why a
/// kernel-confirmed out-of-memory kill could leave no size anywhere: the
/// evidence died with the only thing holding it. Retaining the final sample on
/// the other side of the link costs the machine nothing, needs nothing from a
/// dying process, and survives every way one can end.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct GenerationEndMemory {
    /// When the runner received this reading. The gap between it and the
    /// transition is how much of the generation's final growth this reading
    /// cannot see.
    pub received_at_unix_ms: u64,
    pub process: ProcessTelemetry,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum RemoteConnectionPhase {
    Attempting,
    CredentialRecovered,
    ProtocolReady,
    ProtocolReadyTimedOut,
    HeartbeatLost,
    /// The link beat on time throughout, but the facts those beats carried
    /// stopped being recomputed. Distinct from [`Self::HeartbeatLost`] because
    /// the two look nothing alike from the machine's side: nothing was lost
    /// here, and an operator reading "heartbeat lost" against a machine whose
    /// heartbeats never missed would be looking for the wrong failure.
    ///
    /// Additive and runner-local, so it is not a wire compatibility boundary
    /// and does not bump [`EXECUTOR_PROTOCOL_VERSION`]: this whole enum is the
    /// runner's own record of what it observed about a link, never anything an
    /// executor sends or reads.
    TelemetryStalled,
    Disconnected,
    RemoteProcessExited,
    RetryScheduled,
    /// A job's execution environment on this machine was given up and rebuilt
    /// on another, because this machine stopped answering (silent while still
    /// connected, or disconnected) for longer than the environment lease
    /// grace. Not a link transition: the link may still be attached. It is on
    /// this timeline because it is the consequence of one, and an operator
    /// reading why a machine lost its work should find it beside the silence
    /// that caused it. Runner-local like the rest of this enum.
    EnvironmentEvicted,
}

impl RemoteConnectionPhase {
    /// Whether this transition ends the generation it names, as opposed to
    /// being a step on the way to establishing one.
    ///
    /// The distinction decides which transitions carry a final memory reading.
    /// A generation's last size belongs beside the moment it stopped existing;
    /// attaching it to the attempt that created the generation would date the
    /// reading to before anything had been measured.
    pub fn ends_generation(self) -> bool {
        match self {
            Self::HeartbeatLost
            | Self::TelemetryStalled
            | Self::Disconnected
            | Self::RemoteProcessExited => true,
            Self::Attempting
            | Self::CredentialRecovered
            | Self::ProtocolReady
            | Self::ProtocolReadyTimedOut
            | Self::RetryScheduled
            | Self::EnvironmentEvicted => false,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CommandProcessHealth {
    pub tracked_live_count: usize,
    pub reaped_stale_count: usize,
    pub unverified_count: usize,
    /// Cells whose tree a destructive operation declined to touch because a
    /// process from a prior epoch of that cell was still alive inside it.
    ///
    /// Capacity, not fault: the operation was skipped and will be retried, and
    /// the cell is intact.
    #[serde(default)]
    pub survivor_blocked_count: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oldest_age_ms: Option<u64>,
}

/// What a released cell is still waiting to die.
///
/// A batch that completes normally has every process in its tree verified gone
/// before the cell is released, and this is absent. It is present exactly when
/// that verification did not succeed — a descendant that escaped the group the
/// executor created, or a reap that could not prove absence — and it is the one
/// record that outlives the epoch it belonged to.
///
/// It exists because the executor's in-memory model answers a bookkeeping
/// question ("is this cell assigned to anyone?") and destructive tree
/// operations need a physical one ("is anything still writing in this tree?").
/// Release used to drop command ownership outright, so the physical question
/// had no evidence left to consult.
///
/// Additive and defaulted, so this field's *shape* is not a wire compatibility
/// boundary and does not on its own bump [`EXECUTOR_PROTOCOL_VERSION`]. Neither
/// [`CellOccupancy`] nor [`CommandProcessHealth`] refuses unknown keys, so a
/// peer that never heard of draining ignores the key, and this executor
/// defaults it from that peer's payload.
///
/// What the *behavior* built on this field costs in version numbers is a
/// separate question, and it is decided against the rule in `docs/executor.md`
/// ("What the version claims") rather than against the shape above. Expressing
/// an outcome in refusal vocabulary every peer already understands is not the
/// exception to that rule: v86 said [`CellUnavailableReason::SlotUnhealthy`], a
/// reason no peer had to learn, and moved the number anyway, because what a
/// peer *did* with that refusal had changed.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DrainingCommandOwnership {
    /// The epoch these processes were launched under. Stamped so a survivor can
    /// never be mistaken for the current occupant's own work.
    #[serde(default)]
    pub cell_epoch: u64,
    #[serde(default)]
    pub released_at_unix_ms: u64,
    #[serde(default)]
    pub ownership: CommandBatchOwnership,
}

/// The launch nonce (and stream id) under which a cell's drain records
/// processes the executor found alive in the cell's tree from an epoch that has
/// ended, rather than processes it launched.
///
/// Such an identity is evidence, not ownership. It came from scanning the
/// tree's working directories, never from a launch the executor recorded, so
/// nothing proves Cairn started it: it may be a daemon a build detached, or an
/// operator's own shell. It is waited on — the cell stays fenced while it runs —
/// and never signalled, by the executor's drain pass or by the runner retiring a
/// colocated executor. Every other drain identity was recorded at launch and is
/// reaped automatically.
pub const PRIOR_EPOCH_SURVIVOR_NONCE: &str = "prior-epoch-survivor";

impl CommandPhysicalProcessIdentity {
    /// Whether this identity was recorded as tree-scan evidence of a prior
    /// epoch rather than by a launch; see [`PRIOR_EPOCH_SURVIVOR_NONCE`].
    pub fn is_prior_epoch_survivor_evidence(&self) -> bool {
        self.launch_nonce == PRIOR_EPOCH_SURVIVOR_NONCE
    }
}

/// Durable ownership of one transient command batch in a cell.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CommandBatchOwnership {
    #[serde(default)]
    pub executor_id: String,
    #[serde(default)]
    pub cell_id: String,
    #[serde(default)]
    pub request_id: String,
    #[serde(default)]
    pub attempt_id: String,
    #[serde(default)]
    pub batch_launch_nonce: String,
    #[serde(default)]
    pub launched_at_unix_ms: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub items: Vec<CommandItemOwnership>,
}

/// One requested batch item and every physical launch attempted for it.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CommandItemOwnership {
    #[serde(default)]
    pub item_index: usize,
    #[serde(default)]
    pub stream_id: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attempts: Vec<CommandAttemptOwnership>,
}

/// Persisted lifecycle of one launch attempt, including sandbox retries.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CommandAttemptOwnership {
    #[serde(default)]
    pub attempt_index: usize,
    #[serde(default)]
    pub state: CommandAttemptLaunchState,
    #[serde(default)]
    pub launch_nonce: String,
    #[serde(default)]
    pub launched_at_unix_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity_error: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub processes: Vec<CommandPhysicalProcessIdentity>,
}

/// Launch intent is durable before spawn; running is published only after the
/// leader's complete physical identity has been recorded.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum CommandAttemptLaunchState {
    #[default]
    LaunchIntent,
    Running,
    Reaping,
}

/// PID-reuse-fenced identity of one observed member of an owned command tree.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CommandPhysicalProcessIdentity {
    pub pid: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_group_id: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<u32>,
    pub start_token: String,
    pub launch_nonce: String,
    #[serde(default)]
    pub observed_at_unix_ms: u64,
    #[serde(default)]
    pub parent: CommandProcessParentEvidence,
}

/// Evidence by which a physical identity was attached to the batch. Parent
/// identities carry their own kernel start token so a recycled PPID cannot
/// confer ownership.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum CommandProcessParentEvidence {
    #[default]
    Leader,
    Descendant {
        parent_pid: u32,
        parent_start_token: String,
    },
    ProcessGroup {
        process_group_id: u32,
        leader_pid: u32,
        leader_start_token: String,
    },
    Session {
        session_id: u32,
        leader_pid: u32,
        leader_start_token: String,
    },
}

/// Durable proof that a resident process group belongs to one executor launch.
///
/// PIDs are recyclable, so cleanup matches the kernel start token before
/// signalling the recorded group. The nonce identifies the durable launch
/// generation in diagnostics and prevents records from being conflated.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ResidentProcessOwnership {
    pub pid: u32,
    pub process_group_id: u32,
    pub launch_nonce: String,
    pub start_token: String,
    pub launched_at_unix_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PlacementTieBreak {
    pub candidates: Vec<String>,
    pub deciding_reason: String,
}

pub const fn interactive_cpu_reserve(logical_cores: usize) -> u32 {
    let total = admission_concurrency_budget(logical_cores);
    if total < 4 {
        1
    } else {
        let proportional = total.saturating_add(7) / 8;
        if proportional < 2 {
            2
        } else {
            proportional
        }
    }
}

/// Admission capacity for one executor role. Colocated executors preserve
/// foreground headroom; dedicated executors expose the whole machine.
pub const fn executor_cpu_budget(logical_cores: usize, colocated: bool) -> u32 {
    let total = admission_concurrency_budget(logical_cores);
    if colocated {
        let bulk = total.saturating_sub(interactive_cpu_reserve(logical_cores));
        if bulk < 1 {
            1
        } else {
            bulk
        }
    } else {
        total
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SandboxCapabilityEvidence {
    /// Whether Cairn attached a policy to this executor process.
    pub cairn_sandboxed_marker: bool,
    /// A live behavioral measurement, not an inference from the marker.
    pub nested_sandbox_write_available: bool,
    pub nested_sandbox_write_detail: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub enum TrustDomain {
    /// One leased cell: its checkout, scratch, and spawned processes. The domain
    /// a hostile agent is assumed to already own.
    Cell,
    /// The executor account and the machine's own filesystem and process table.
    ///
    /// The same machine authorization addresses as
    /// [`crate::authorization::AuthorityPlace::Executor`], asked a different
    /// question. Authorization addresses it as an enrolled principal — may work
    /// run here, may it be enrolled or revoked. Containment addresses its local
    /// account, filesystem, and process table as the destination of a crossing.
    /// One place, two verbs.
    ExecutorHost,
    /// State shared by every cell on one machine, regardless of project.
    MachineShared,
    /// Another cell on the same machine, possibly another project or workspace.
    PeerCell,
    /// The runner process that owns placement, credentials, and the workspace.
    Runner,
    /// Another agent, reached through what this one publishes.
    PeerAgent,
    /// Ambient, unauthenticated reach outside the machine.
    ///
    /// Named for the network rather than for being "external" because
    /// authorization already has an
    /// [`crate::authorization::AuthorityPlace::ExternalAccount`], and the two
    /// are different things wearing adjacent words: that is an authenticated
    /// account at a named provider, this is the open internet reached with
    /// whatever credential a guest process happens to hold. No containment edge
    /// addresses an external account today, and one that did would take its own
    /// domain rather than widening this one.
    ExternalNetwork,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub enum Direction {
    GuestToDest,
    DestToGuest,
    Bidirectional,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub enum Carrier {
    Filesystem,
    Socket,
    ProcessEnv,
    ExecutorLink,
    McpRelay,
    ObjectChannel,
    PackageManager,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub enum ProcessClass {
    BatchSpawn,
    PipedResident,
    PtyResident,
    Repl,
    Workflow,
    DevInstance,
    ExecutorItself,
    /// The runner/orchestrator process itself, outside any executor guest.
    Runner,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub enum EffectClass {
    Read,
    Write,
    ReadWrite,
    Execute,
    Publish,
    Consume,
    Invoke,
    Signal,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct EdgeKey {
    pub source: TrustDomain,
    pub destination: TrustDomain,
    pub direction: Direction,
    pub carrier: Carrier,
    pub process_class: ProcessClass,
    pub effect: EffectClass,
    pub policy_mode: PolicyMode,
}

/// The runtime sandbox policy a spawn actually applies.
///
/// This is part of edge identity rather than a property of an observation,
/// because the same spawn function reaches all three: `run_process_attempt`
/// selects among [`ProcessSandboxMode`] variants, and `Unconfined` builds no
/// policy at all. If the mode were merely a field to compare, a measurement of
/// the confined path and a request for the unconfined path would be two
/// coordinates that happen to differ, and any comparison that forgot to check
/// one would silently succeed. Splitting the edges makes the mismatch
/// unrepresentable instead: confined evidence and an unconfined workload are
/// about different edges, so no comparison between them can accidentally pass.
///
/// `ReadOnlyCheckout` is its own mode rather than a weaker `Confined`. It keeps
/// the checkout readable while denying writes into it, which is a different
/// enforced claim, and it earns only what its own crossing proves.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub enum PolicyMode {
    Confined,
    ReadOnlyCheckout,
    Unconfined,
    /// The edge does not run under a filesystem policy at all, so no mode
    /// distinguishes it. Network egress is unaffected by which filesystem
    /// profile is applied, and an executor-side control surface has no guest
    /// policy in the first place.
    NotApplicable,
}

impl PolicyMode {
    /// The containment mode a requested process sandbox mode runs under, so a
    /// caller cannot pair a batch with evidence about a different policy.
    pub const fn of(mode: ProcessSandboxMode) -> Self {
        match mode {
            ProcessSandboxMode::Confined => Self::Confined,
            ProcessSandboxMode::ReadOnlyCheckout => Self::ReadOnlyCheckout,
            ProcessSandboxMode::Unconfined => Self::Unconfined,
        }
    }
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
#[repr(usize)]
pub enum EdgeId {
    BatchSpawnFilesystemConfined,
    BatchSpawnFilesystemReadOnlyCheckout,
    BatchSpawnFilesystemUnconfined,
    PipedResidentFilesystemConfined,
    PipedResidentFilesystemReadOnlyCheckout,
    PipedResidentFilesystemUnconfined,
    PtyResidentFilesystemConfined,
    PtyResidentFilesystemReadOnlyCheckout,
    PtyResidentFilesystemUnconfined,
    ReplFilesystemConfined,
    ReplFilesystemReadOnlyCheckout,
    ReplFilesystemUnconfined,
    WorkflowFilesystemConfined,
    WorkflowFilesystemReadOnlyCheckout,
    WorkflowFilesystemUnconfined,
    DevInstanceFilesystemConfined,
    DevInstanceFilesystemReadOnlyCheckout,
    DevInstanceFilesystemUnconfined,
    BatchSpawnEgress,
    PipedResidentEgress,
    PtyResidentEgress,
    ReplEgress,
    WorkflowEgress,
    DevInstanceEgress,
    BatchSpawnEnvironmentInheritance,
    PipedResidentEnvironmentInheritance,
    PtyResidentEnvironmentInheritance,
    ReplEnvironmentInheritance,
    WorkflowEnvironmentInheritance,
    DevInstanceEnvironmentInheritance,
    DependencyCatalogPublish,
    DependencyCatalogConsume,
    CargoRegistryCache,
    NpmCache,
    BunCache,
    ToolchainCache,
    CellScratchWithinResidency,
    CellScratchAcrossOccupants,
    SiblingCellTree,
    McpRelayPerBatchCapability,
    McpRelayResidentRunnerWide,
    ArtifactGuestToPublisher,
    ArtifactPublisherToStore,
    ArtifactStoreToConsumer,
    ExecutorLinkControl,
    CellLeaseControl,
    BuildServiceClientEnv,
    BuildServiceStateDir,
    DevInstanceExternalIngress,
    DesktopAutomationControl,
    RunnerExecutorListenerIngress,
    ExecutorDialOutCredential,
    ExecutorArtifactPull,
    RunnerArtifactReleaseFetch,
    ExecutionQueueOffer,
}
impl EdgeId {
    pub const COUNT: usize = 55;
    pub const fn definition(self) -> &'static EdgeDefinition {
        &CONTAINMENT_EDGE_DEFINITIONS[self as usize]
    }
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub enum EnvelopeId {
    CellFilesystem,
    BatchMcpOperation,
    ExecutionArtifactAppend,
    TypedHostOperation,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum Authentication {
    None,
    ExecutorEnrollment,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum MediationPoint {
    Unmediated,
    SandboxProfile,
    DependencyCatalog,
    PackageManager,
    CellLease,
    McpRelay,
    ArtifactPublisher,
    ExecutorLink,
    /// The runner's execution queue: an append-only journal of offers, claims,
    /// and declines, one entry offered to one machine at a time.
    ExecutionQueue,
    BuildService,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum EdgeScoping {
    Unscoped,
    Batch,
    Cell,
    Execution,
    Executor,
    Machine,
    Runner,
    Service,
    Workspace,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum Mutability {
    Mutable,
    AppendOnly,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum Persistence {
    Process,
    Batch,
    Connection,
    Cell,
    Residency,
    Execution,
    Machine,
    Runner,
    Workspace,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum AuditEvidence {
    None,
    FilesystemProbe,
    NetworkProbe,
    PublicationJournal,
    CellJournal,
    CallbackJournal,
    ArtifactJournal,
    ProtocolJournal,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum Revocation {
    ProcessExit,
    BatchCompletion,
    Disconnect,
    CellRelease,
    ExecutionCompletion,
    RunnerRestart,
    ConfigurationChange,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub enum InvalidationInput {
    ExecutorBuild,
    BootGeneration,
    SandboxProfile,
    MountTopology,
    NetworkPolicy,
    DnsConfiguration,
    ProxyConfiguration,
    DependencyCatalogConfiguration,
    PackageConfiguration,
    ToolchainConfiguration,
    CellLeasePolicy,
    RelayPolicy,
    ArtifactPolicy,
    ExecutorLinkPolicy,
    BuildServiceConfiguration,
    /// Which variables the runner selects and assembles into a spawn's
    /// environment. Wholesale inheritance means this is effectively the runner's
    /// entire environment; see cairn/3391.
    EnvironmentAssemblyPolicy,
    DevInstanceIngressPolicy,
    DesktopAutomationConfiguration,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ProbeVersion {
    V1,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EdgeDefinition {
    pub id: EdgeId,
    pub key: EdgeKey,
    pub authentication: Authentication,
    pub declared_envelopes: &'static [EnvelopeId],
    pub mediation: MediationPoint,
    pub scoping: EdgeScoping,
    pub mutability: Mutability,
    pub persistence: Persistence,
    pub audit_evidence: AuditEvidence,
    pub revocation: Revocation,
    pub invalidation_inputs: &'static [InvalidationInput],
    pub claim_revision: u32,
    pub supported_probe_versions: &'static [ProbeVersion],
}
const NO_ENVELOPES: &[EnvelopeId] = &[];
const CELL_FILESYSTEM_ENVELOPES: &[EnvelopeId] = &[EnvelopeId::CellFilesystem];
const BATCH_MCP_ENVELOPES: &[EnvelopeId] = &[EnvelopeId::BatchMcpOperation];
const ARTIFACT_ENVELOPES: &[EnvelopeId] = &[EnvelopeId::ExecutionArtifactAppend];
const HOST_ENVELOPES: &[EnvelopeId] = &[EnvelopeId::TypedHostOperation];
const PROBE_VERSIONS: &[ProbeVersion] = &[ProbeVersion::V1];
const FILESYSTEM_INPUTS: &[InvalidationInput] = &[
    InvalidationInput::ExecutorBuild,
    InvalidationInput::BootGeneration,
    InvalidationInput::SandboxProfile,
    InvalidationInput::MountTopology,
];
const NETWORK_INPUTS: &[InvalidationInput] = &[
    InvalidationInput::ExecutorBuild,
    InvalidationInput::BootGeneration,
    InvalidationInput::NetworkPolicy,
    InvalidationInput::DnsConfiguration,
    InvalidationInput::ProxyConfiguration,
];
// Each edge below declares the inputs that could WIDEN it, not merely the ones
// Cairn happens to be able to read. That distinction is the whole mechanism: an
// edge whose declared inputs are all readable can carry positive evidence, and
// an edge that names an input with no closed readable form cannot fingerprint
// at all, so it stays unmeasured until the boundary that owns it lands. Naming
// only the readable subset would manufacture exactly the false confidence the
// fingerprint exists to prevent.
const CATALOG_INPUTS: &[InvalidationInput] = &[
    InvalidationInput::ExecutorBuild,
    InvalidationInput::BootGeneration,
    InvalidationInput::DependencyCatalogConfiguration,
];
const PACKAGE_INPUTS: &[InvalidationInput] = &[
    InvalidationInput::ExecutorBuild,
    InvalidationInput::BootGeneration,
    InvalidationInput::PackageConfiguration,
    InvalidationInput::ToolchainConfiguration,
];
const LEASE_INPUTS: &[InvalidationInput] = &[
    InvalidationInput::ExecutorBuild,
    InvalidationInput::BootGeneration,
    InvalidationInput::MountTopology,
    InvalidationInput::CellLeasePolicy,
];
const RELAY_INPUTS: &[InvalidationInput] = &[
    InvalidationInput::ExecutorBuild,
    InvalidationInput::BootGeneration,
    InvalidationInput::RelayPolicy,
];
const ARTIFACT_INPUTS: &[InvalidationInput] = &[
    InvalidationInput::ExecutorBuild,
    InvalidationInput::BootGeneration,
    InvalidationInput::ArtifactPolicy,
];
const LINK_INPUTS: &[InvalidationInput] = &[
    InvalidationInput::ExecutorBuild,
    InvalidationInput::BootGeneration,
    InvalidationInput::ExecutorLinkPolicy,
];
const SERVICE_INPUTS: &[InvalidationInput] = &[
    InvalidationInput::ExecutorBuild,
    InvalidationInput::BootGeneration,
    InvalidationInput::BuildServiceConfiguration,
];
const ENVIRONMENT_INPUTS: &[InvalidationInput] = &[
    InvalidationInput::ExecutorBuild,
    InvalidationInput::BootGeneration,
    InvalidationInput::EnvironmentAssemblyPolicy,
];
const INGRESS_INPUTS: &[InvalidationInput] = &[
    InvalidationInput::ExecutorBuild,
    InvalidationInput::BootGeneration,
    InvalidationInput::DevInstanceIngressPolicy,
    InvalidationInput::NetworkPolicy,
];
const DESKTOP_INPUTS: &[InvalidationInput] = &[
    InvalidationInput::ExecutorBuild,
    InvalidationInput::BootGeneration,
    InvalidationInput::DesktopAutomationConfiguration,
];
/// The listener's reach is set by its link policy and by the network it is bound
/// to, so a change to either could widen who can reach it.
const LISTENER_INPUTS: &[InvalidationInput] = &[
    InvalidationInput::ExecutorBuild,
    InvalidationInput::BootGeneration,
    InvalidationInput::ExecutorLinkPolicy,
    InvalidationInput::NetworkPolicy,
];
// CONTAINMENT_INVENTORY_BEGIN
pub const CONTAINMENT_EDGE_DEFINITIONS: [EdgeDefinition; EdgeId::COUNT] = [
    EdgeDefinition {
        id: EdgeId::BatchSpawnFilesystemConfined,
        key: EdgeKey {
            source: TrustDomain::Cell,
            destination: TrustDomain::ExecutorHost,
            direction: Direction::GuestToDest,
            carrier: Carrier::Filesystem,
            process_class: ProcessClass::BatchSpawn,
            effect: EffectClass::Write,
            policy_mode: PolicyMode::Confined,
        },
        authentication: Authentication::None,
        declared_envelopes: CELL_FILESYSTEM_ENVELOPES,
        mediation: MediationPoint::SandboxProfile,
        scoping: EdgeScoping::Cell,
        mutability: Mutability::Mutable,
        persistence: Persistence::Process,
        audit_evidence: AuditEvidence::FilesystemProbe,
        revocation: Revocation::ProcessExit,
        invalidation_inputs: FILESYSTEM_INPUTS,
        claim_revision: 1,
        supported_probe_versions: PROBE_VERSIONS,
    },
    EdgeDefinition {
        id: EdgeId::BatchSpawnFilesystemReadOnlyCheckout,
        key: EdgeKey {
            source: TrustDomain::Cell,
            destination: TrustDomain::ExecutorHost,
            direction: Direction::GuestToDest,
            carrier: Carrier::Filesystem,
            process_class: ProcessClass::BatchSpawn,
            effect: EffectClass::Write,
            policy_mode: PolicyMode::ReadOnlyCheckout,
        },
        authentication: Authentication::None,
        declared_envelopes: CELL_FILESYSTEM_ENVELOPES,
        mediation: MediationPoint::SandboxProfile,
        scoping: EdgeScoping::Cell,
        mutability: Mutability::Mutable,
        persistence: Persistence::Process,
        audit_evidence: AuditEvidence::FilesystemProbe,
        revocation: Revocation::ProcessExit,
        invalidation_inputs: FILESYSTEM_INPUTS,
        claim_revision: 1,
        supported_probe_versions: PROBE_VERSIONS,
    },
    EdgeDefinition {
        id: EdgeId::BatchSpawnFilesystemUnconfined,
        key: EdgeKey {
            source: TrustDomain::Cell,
            destination: TrustDomain::ExecutorHost,
            direction: Direction::GuestToDest,
            carrier: Carrier::Filesystem,
            process_class: ProcessClass::BatchSpawn,
            effect: EffectClass::Write,
            policy_mode: PolicyMode::Unconfined,
        },
        authentication: Authentication::None,
        declared_envelopes: NO_ENVELOPES,
        mediation: MediationPoint::Unmediated,
        scoping: EdgeScoping::Cell,
        mutability: Mutability::Mutable,
        persistence: Persistence::Process,
        audit_evidence: AuditEvidence::FilesystemProbe,
        revocation: Revocation::ProcessExit,
        invalidation_inputs: FILESYSTEM_INPUTS,
        claim_revision: 1,
        supported_probe_versions: PROBE_VERSIONS,
    },
    EdgeDefinition {
        id: EdgeId::PipedResidentFilesystemConfined,
        key: EdgeKey {
            source: TrustDomain::Cell,
            destination: TrustDomain::ExecutorHost,
            direction: Direction::GuestToDest,
            carrier: Carrier::Filesystem,
            process_class: ProcessClass::PipedResident,
            effect: EffectClass::Write,
            policy_mode: PolicyMode::Confined,
        },
        authentication: Authentication::None,
        declared_envelopes: CELL_FILESYSTEM_ENVELOPES,
        mediation: MediationPoint::SandboxProfile,
        scoping: EdgeScoping::Cell,
        mutability: Mutability::Mutable,
        persistence: Persistence::Process,
        audit_evidence: AuditEvidence::FilesystemProbe,
        revocation: Revocation::ProcessExit,
        invalidation_inputs: FILESYSTEM_INPUTS,
        claim_revision: 1,
        supported_probe_versions: PROBE_VERSIONS,
    },
    EdgeDefinition {
        id: EdgeId::PipedResidentFilesystemReadOnlyCheckout,
        key: EdgeKey {
            source: TrustDomain::Cell,
            destination: TrustDomain::ExecutorHost,
            direction: Direction::GuestToDest,
            carrier: Carrier::Filesystem,
            process_class: ProcessClass::PipedResident,
            effect: EffectClass::Write,
            policy_mode: PolicyMode::ReadOnlyCheckout,
        },
        authentication: Authentication::None,
        declared_envelopes: CELL_FILESYSTEM_ENVELOPES,
        mediation: MediationPoint::SandboxProfile,
        scoping: EdgeScoping::Cell,
        mutability: Mutability::Mutable,
        persistence: Persistence::Process,
        audit_evidence: AuditEvidence::FilesystemProbe,
        revocation: Revocation::ProcessExit,
        invalidation_inputs: FILESYSTEM_INPUTS,
        claim_revision: 1,
        supported_probe_versions: PROBE_VERSIONS,
    },
    EdgeDefinition {
        id: EdgeId::PipedResidentFilesystemUnconfined,
        key: EdgeKey {
            source: TrustDomain::Cell,
            destination: TrustDomain::ExecutorHost,
            direction: Direction::GuestToDest,
            carrier: Carrier::Filesystem,
            process_class: ProcessClass::PipedResident,
            effect: EffectClass::Write,
            policy_mode: PolicyMode::Unconfined,
        },
        authentication: Authentication::None,
        declared_envelopes: NO_ENVELOPES,
        mediation: MediationPoint::Unmediated,
        scoping: EdgeScoping::Cell,
        mutability: Mutability::Mutable,
        persistence: Persistence::Process,
        audit_evidence: AuditEvidence::FilesystemProbe,
        revocation: Revocation::ProcessExit,
        invalidation_inputs: FILESYSTEM_INPUTS,
        claim_revision: 1,
        supported_probe_versions: PROBE_VERSIONS,
    },
    EdgeDefinition {
        id: EdgeId::PtyResidentFilesystemConfined,
        key: EdgeKey {
            source: TrustDomain::Cell,
            destination: TrustDomain::ExecutorHost,
            direction: Direction::GuestToDest,
            carrier: Carrier::Filesystem,
            process_class: ProcessClass::PtyResident,
            effect: EffectClass::Write,
            policy_mode: PolicyMode::Confined,
        },
        authentication: Authentication::None,
        declared_envelopes: CELL_FILESYSTEM_ENVELOPES,
        mediation: MediationPoint::SandboxProfile,
        scoping: EdgeScoping::Cell,
        mutability: Mutability::Mutable,
        persistence: Persistence::Process,
        audit_evidence: AuditEvidence::FilesystemProbe,
        revocation: Revocation::ProcessExit,
        invalidation_inputs: FILESYSTEM_INPUTS,
        claim_revision: 1,
        supported_probe_versions: PROBE_VERSIONS,
    },
    EdgeDefinition {
        id: EdgeId::PtyResidentFilesystemReadOnlyCheckout,
        key: EdgeKey {
            source: TrustDomain::Cell,
            destination: TrustDomain::ExecutorHost,
            direction: Direction::GuestToDest,
            carrier: Carrier::Filesystem,
            process_class: ProcessClass::PtyResident,
            effect: EffectClass::Write,
            policy_mode: PolicyMode::ReadOnlyCheckout,
        },
        authentication: Authentication::None,
        declared_envelopes: CELL_FILESYSTEM_ENVELOPES,
        mediation: MediationPoint::SandboxProfile,
        scoping: EdgeScoping::Cell,
        mutability: Mutability::Mutable,
        persistence: Persistence::Process,
        audit_evidence: AuditEvidence::FilesystemProbe,
        revocation: Revocation::ProcessExit,
        invalidation_inputs: FILESYSTEM_INPUTS,
        claim_revision: 1,
        supported_probe_versions: PROBE_VERSIONS,
    },
    EdgeDefinition {
        id: EdgeId::PtyResidentFilesystemUnconfined,
        key: EdgeKey {
            source: TrustDomain::Cell,
            destination: TrustDomain::ExecutorHost,
            direction: Direction::GuestToDest,
            carrier: Carrier::Filesystem,
            process_class: ProcessClass::PtyResident,
            effect: EffectClass::Write,
            policy_mode: PolicyMode::Unconfined,
        },
        authentication: Authentication::None,
        declared_envelopes: NO_ENVELOPES,
        mediation: MediationPoint::Unmediated,
        scoping: EdgeScoping::Cell,
        mutability: Mutability::Mutable,
        persistence: Persistence::Process,
        audit_evidence: AuditEvidence::FilesystemProbe,
        revocation: Revocation::ProcessExit,
        invalidation_inputs: FILESYSTEM_INPUTS,
        claim_revision: 1,
        supported_probe_versions: PROBE_VERSIONS,
    },
    EdgeDefinition {
        id: EdgeId::ReplFilesystemConfined,
        key: EdgeKey {
            source: TrustDomain::Cell,
            destination: TrustDomain::ExecutorHost,
            direction: Direction::GuestToDest,
            carrier: Carrier::Filesystem,
            process_class: ProcessClass::Repl,
            effect: EffectClass::Write,
            policy_mode: PolicyMode::Confined,
        },
        authentication: Authentication::None,
        declared_envelopes: CELL_FILESYSTEM_ENVELOPES,
        mediation: MediationPoint::SandboxProfile,
        scoping: EdgeScoping::Cell,
        mutability: Mutability::Mutable,
        persistence: Persistence::Process,
        audit_evidence: AuditEvidence::FilesystemProbe,
        revocation: Revocation::ProcessExit,
        invalidation_inputs: FILESYSTEM_INPUTS,
        claim_revision: 1,
        supported_probe_versions: PROBE_VERSIONS,
    },
    EdgeDefinition {
        id: EdgeId::ReplFilesystemReadOnlyCheckout,
        key: EdgeKey {
            source: TrustDomain::Cell,
            destination: TrustDomain::ExecutorHost,
            direction: Direction::GuestToDest,
            carrier: Carrier::Filesystem,
            process_class: ProcessClass::Repl,
            effect: EffectClass::Write,
            policy_mode: PolicyMode::ReadOnlyCheckout,
        },
        authentication: Authentication::None,
        declared_envelopes: CELL_FILESYSTEM_ENVELOPES,
        mediation: MediationPoint::SandboxProfile,
        scoping: EdgeScoping::Cell,
        mutability: Mutability::Mutable,
        persistence: Persistence::Process,
        audit_evidence: AuditEvidence::FilesystemProbe,
        revocation: Revocation::ProcessExit,
        invalidation_inputs: FILESYSTEM_INPUTS,
        claim_revision: 1,
        supported_probe_versions: PROBE_VERSIONS,
    },
    EdgeDefinition {
        id: EdgeId::ReplFilesystemUnconfined,
        key: EdgeKey {
            source: TrustDomain::Cell,
            destination: TrustDomain::ExecutorHost,
            direction: Direction::GuestToDest,
            carrier: Carrier::Filesystem,
            process_class: ProcessClass::Repl,
            effect: EffectClass::Write,
            policy_mode: PolicyMode::Unconfined,
        },
        authentication: Authentication::None,
        declared_envelopes: NO_ENVELOPES,
        mediation: MediationPoint::Unmediated,
        scoping: EdgeScoping::Cell,
        mutability: Mutability::Mutable,
        persistence: Persistence::Process,
        audit_evidence: AuditEvidence::FilesystemProbe,
        revocation: Revocation::ProcessExit,
        invalidation_inputs: FILESYSTEM_INPUTS,
        claim_revision: 1,
        supported_probe_versions: PROBE_VERSIONS,
    },
    EdgeDefinition {
        id: EdgeId::WorkflowFilesystemConfined,
        key: EdgeKey {
            source: TrustDomain::Cell,
            destination: TrustDomain::ExecutorHost,
            direction: Direction::GuestToDest,
            carrier: Carrier::Filesystem,
            process_class: ProcessClass::Workflow,
            effect: EffectClass::Write,
            policy_mode: PolicyMode::Confined,
        },
        authentication: Authentication::None,
        declared_envelopes: CELL_FILESYSTEM_ENVELOPES,
        mediation: MediationPoint::SandboxProfile,
        scoping: EdgeScoping::Cell,
        mutability: Mutability::Mutable,
        persistence: Persistence::Process,
        audit_evidence: AuditEvidence::FilesystemProbe,
        revocation: Revocation::ProcessExit,
        invalidation_inputs: FILESYSTEM_INPUTS,
        claim_revision: 1,
        supported_probe_versions: PROBE_VERSIONS,
    },
    EdgeDefinition {
        id: EdgeId::WorkflowFilesystemReadOnlyCheckout,
        key: EdgeKey {
            source: TrustDomain::Cell,
            destination: TrustDomain::ExecutorHost,
            direction: Direction::GuestToDest,
            carrier: Carrier::Filesystem,
            process_class: ProcessClass::Workflow,
            effect: EffectClass::Write,
            policy_mode: PolicyMode::ReadOnlyCheckout,
        },
        authentication: Authentication::None,
        declared_envelopes: CELL_FILESYSTEM_ENVELOPES,
        mediation: MediationPoint::SandboxProfile,
        scoping: EdgeScoping::Cell,
        mutability: Mutability::Mutable,
        persistence: Persistence::Process,
        audit_evidence: AuditEvidence::FilesystemProbe,
        revocation: Revocation::ProcessExit,
        invalidation_inputs: FILESYSTEM_INPUTS,
        claim_revision: 1,
        supported_probe_versions: PROBE_VERSIONS,
    },
    EdgeDefinition {
        id: EdgeId::WorkflowFilesystemUnconfined,
        key: EdgeKey {
            source: TrustDomain::Cell,
            destination: TrustDomain::ExecutorHost,
            direction: Direction::GuestToDest,
            carrier: Carrier::Filesystem,
            process_class: ProcessClass::Workflow,
            effect: EffectClass::Write,
            policy_mode: PolicyMode::Unconfined,
        },
        authentication: Authentication::None,
        declared_envelopes: NO_ENVELOPES,
        mediation: MediationPoint::Unmediated,
        scoping: EdgeScoping::Cell,
        mutability: Mutability::Mutable,
        persistence: Persistence::Process,
        audit_evidence: AuditEvidence::FilesystemProbe,
        revocation: Revocation::ProcessExit,
        invalidation_inputs: FILESYSTEM_INPUTS,
        claim_revision: 1,
        supported_probe_versions: PROBE_VERSIONS,
    },
    EdgeDefinition {
        id: EdgeId::DevInstanceFilesystemConfined,
        key: EdgeKey {
            source: TrustDomain::Cell,
            destination: TrustDomain::ExecutorHost,
            direction: Direction::GuestToDest,
            carrier: Carrier::Filesystem,
            process_class: ProcessClass::DevInstance,
            effect: EffectClass::Write,
            policy_mode: PolicyMode::Confined,
        },
        authentication: Authentication::None,
        declared_envelopes: CELL_FILESYSTEM_ENVELOPES,
        mediation: MediationPoint::SandboxProfile,
        scoping: EdgeScoping::Cell,
        mutability: Mutability::Mutable,
        persistence: Persistence::Process,
        audit_evidence: AuditEvidence::FilesystemProbe,
        revocation: Revocation::ProcessExit,
        invalidation_inputs: FILESYSTEM_INPUTS,
        claim_revision: 1,
        supported_probe_versions: PROBE_VERSIONS,
    },
    EdgeDefinition {
        id: EdgeId::DevInstanceFilesystemReadOnlyCheckout,
        key: EdgeKey {
            source: TrustDomain::Cell,
            destination: TrustDomain::ExecutorHost,
            direction: Direction::GuestToDest,
            carrier: Carrier::Filesystem,
            process_class: ProcessClass::DevInstance,
            effect: EffectClass::Write,
            policy_mode: PolicyMode::ReadOnlyCheckout,
        },
        authentication: Authentication::None,
        declared_envelopes: CELL_FILESYSTEM_ENVELOPES,
        mediation: MediationPoint::SandboxProfile,
        scoping: EdgeScoping::Cell,
        mutability: Mutability::Mutable,
        persistence: Persistence::Process,
        audit_evidence: AuditEvidence::FilesystemProbe,
        revocation: Revocation::ProcessExit,
        invalidation_inputs: FILESYSTEM_INPUTS,
        claim_revision: 1,
        supported_probe_versions: PROBE_VERSIONS,
    },
    EdgeDefinition {
        id: EdgeId::DevInstanceFilesystemUnconfined,
        key: EdgeKey {
            source: TrustDomain::Cell,
            destination: TrustDomain::ExecutorHost,
            direction: Direction::GuestToDest,
            carrier: Carrier::Filesystem,
            process_class: ProcessClass::DevInstance,
            effect: EffectClass::Write,
            policy_mode: PolicyMode::Unconfined,
        },
        authentication: Authentication::None,
        declared_envelopes: NO_ENVELOPES,
        mediation: MediationPoint::Unmediated,
        scoping: EdgeScoping::Cell,
        mutability: Mutability::Mutable,
        persistence: Persistence::Process,
        audit_evidence: AuditEvidence::FilesystemProbe,
        revocation: Revocation::ProcessExit,
        invalidation_inputs: FILESYSTEM_INPUTS,
        claim_revision: 1,
        supported_probe_versions: PROBE_VERSIONS,
    },
    EdgeDefinition {
        id: EdgeId::BatchSpawnEgress,
        key: EdgeKey {
            source: TrustDomain::Cell,
            destination: TrustDomain::Runner,
            direction: Direction::Bidirectional,
            carrier: Carrier::Socket,
            process_class: ProcessClass::BatchSpawn,
            effect: EffectClass::Invoke,
            policy_mode: PolicyMode::NotApplicable,
        },
        authentication: Authentication::None,
        declared_envelopes: NO_ENVELOPES,
        mediation: MediationPoint::Unmediated,
        scoping: EdgeScoping::Unscoped,
        mutability: Mutability::Mutable,
        persistence: Persistence::Process,
        audit_evidence: AuditEvidence::None,
        revocation: Revocation::ProcessExit,
        invalidation_inputs: NETWORK_INPUTS,
        claim_revision: 1,
        supported_probe_versions: PROBE_VERSIONS,
    },
    EdgeDefinition {
        id: EdgeId::PipedResidentEgress,
        key: EdgeKey {
            source: TrustDomain::Cell,
            destination: TrustDomain::ExternalNetwork,
            direction: Direction::Bidirectional,
            carrier: Carrier::Socket,
            process_class: ProcessClass::PipedResident,
            effect: EffectClass::Invoke,
            policy_mode: PolicyMode::NotApplicable,
        },
        authentication: Authentication::None,
        declared_envelopes: NO_ENVELOPES,
        mediation: MediationPoint::Unmediated,
        scoping: EdgeScoping::Unscoped,
        mutability: Mutability::Mutable,
        persistence: Persistence::Process,
        audit_evidence: AuditEvidence::None,
        revocation: Revocation::ProcessExit,
        invalidation_inputs: NETWORK_INPUTS,
        claim_revision: 1,
        supported_probe_versions: PROBE_VERSIONS,
    },
    EdgeDefinition {
        id: EdgeId::PtyResidentEgress,
        key: EdgeKey {
            source: TrustDomain::Cell,
            destination: TrustDomain::ExternalNetwork,
            direction: Direction::Bidirectional,
            carrier: Carrier::Socket,
            process_class: ProcessClass::PtyResident,
            effect: EffectClass::Invoke,
            policy_mode: PolicyMode::NotApplicable,
        },
        authentication: Authentication::None,
        declared_envelopes: NO_ENVELOPES,
        mediation: MediationPoint::Unmediated,
        scoping: EdgeScoping::Unscoped,
        mutability: Mutability::Mutable,
        persistence: Persistence::Process,
        audit_evidence: AuditEvidence::None,
        revocation: Revocation::ProcessExit,
        invalidation_inputs: NETWORK_INPUTS,
        claim_revision: 1,
        supported_probe_versions: PROBE_VERSIONS,
    },
    EdgeDefinition {
        id: EdgeId::ReplEgress,
        key: EdgeKey {
            source: TrustDomain::Cell,
            destination: TrustDomain::ExternalNetwork,
            direction: Direction::Bidirectional,
            carrier: Carrier::Socket,
            process_class: ProcessClass::Repl,
            effect: EffectClass::Invoke,
            policy_mode: PolicyMode::NotApplicable,
        },
        authentication: Authentication::None,
        declared_envelopes: NO_ENVELOPES,
        mediation: MediationPoint::Unmediated,
        scoping: EdgeScoping::Unscoped,
        mutability: Mutability::Mutable,
        persistence: Persistence::Process,
        audit_evidence: AuditEvidence::None,
        revocation: Revocation::ProcessExit,
        invalidation_inputs: NETWORK_INPUTS,
        claim_revision: 1,
        supported_probe_versions: PROBE_VERSIONS,
    },
    EdgeDefinition {
        id: EdgeId::WorkflowEgress,
        key: EdgeKey {
            source: TrustDomain::Cell,
            destination: TrustDomain::ExternalNetwork,
            direction: Direction::Bidirectional,
            carrier: Carrier::Socket,
            process_class: ProcessClass::Workflow,
            effect: EffectClass::Invoke,
            policy_mode: PolicyMode::NotApplicable,
        },
        authentication: Authentication::None,
        declared_envelopes: NO_ENVELOPES,
        mediation: MediationPoint::Unmediated,
        scoping: EdgeScoping::Unscoped,
        mutability: Mutability::Mutable,
        persistence: Persistence::Process,
        audit_evidence: AuditEvidence::None,
        revocation: Revocation::ProcessExit,
        invalidation_inputs: NETWORK_INPUTS,
        claim_revision: 1,
        supported_probe_versions: PROBE_VERSIONS,
    },
    EdgeDefinition {
        id: EdgeId::DevInstanceEgress,
        key: EdgeKey {
            source: TrustDomain::Cell,
            destination: TrustDomain::ExternalNetwork,
            direction: Direction::Bidirectional,
            carrier: Carrier::Socket,
            process_class: ProcessClass::DevInstance,
            effect: EffectClass::Invoke,
            policy_mode: PolicyMode::NotApplicable,
        },
        authentication: Authentication::None,
        declared_envelopes: NO_ENVELOPES,
        mediation: MediationPoint::Unmediated,
        scoping: EdgeScoping::Unscoped,
        mutability: Mutability::Mutable,
        persistence: Persistence::Process,
        audit_evidence: AuditEvidence::None,
        revocation: Revocation::ProcessExit,
        invalidation_inputs: NETWORK_INPUTS,
        claim_revision: 1,
        supported_probe_versions: PROBE_VERSIONS,
    },
    EdgeDefinition {
        id: EdgeId::BatchSpawnEnvironmentInheritance,
        key: EdgeKey {
            source: TrustDomain::Runner,
            destination: TrustDomain::Cell,
            direction: Direction::DestToGuest,
            carrier: Carrier::ProcessEnv,
            process_class: ProcessClass::BatchSpawn,
            effect: EffectClass::Write,
            policy_mode: PolicyMode::NotApplicable,
        },
        authentication: Authentication::None,
        declared_envelopes: NO_ENVELOPES,
        mediation: MediationPoint::Unmediated,
        scoping: EdgeScoping::Runner,
        mutability: Mutability::Mutable,
        persistence: Persistence::Process,
        audit_evidence: AuditEvidence::None,
        revocation: Revocation::ProcessExit,
        invalidation_inputs: ENVIRONMENT_INPUTS,
        claim_revision: 1,
        supported_probe_versions: PROBE_VERSIONS,
    },
    EdgeDefinition {
        id: EdgeId::PipedResidentEnvironmentInheritance,
        key: EdgeKey {
            source: TrustDomain::Runner,
            destination: TrustDomain::Cell,
            direction: Direction::DestToGuest,
            carrier: Carrier::ProcessEnv,
            process_class: ProcessClass::PipedResident,
            effect: EffectClass::Write,
            policy_mode: PolicyMode::NotApplicable,
        },
        authentication: Authentication::None,
        declared_envelopes: NO_ENVELOPES,
        mediation: MediationPoint::Unmediated,
        scoping: EdgeScoping::Runner,
        mutability: Mutability::Mutable,
        persistence: Persistence::Process,
        audit_evidence: AuditEvidence::None,
        revocation: Revocation::ProcessExit,
        invalidation_inputs: ENVIRONMENT_INPUTS,
        claim_revision: 1,
        supported_probe_versions: PROBE_VERSIONS,
    },
    EdgeDefinition {
        id: EdgeId::PtyResidentEnvironmentInheritance,
        key: EdgeKey {
            source: TrustDomain::Runner,
            destination: TrustDomain::Cell,
            direction: Direction::DestToGuest,
            carrier: Carrier::ProcessEnv,
            process_class: ProcessClass::PtyResident,
            effect: EffectClass::Write,
            policy_mode: PolicyMode::NotApplicable,
        },
        authentication: Authentication::None,
        declared_envelopes: NO_ENVELOPES,
        mediation: MediationPoint::Unmediated,
        scoping: EdgeScoping::Runner,
        mutability: Mutability::Mutable,
        persistence: Persistence::Process,
        audit_evidence: AuditEvidence::None,
        revocation: Revocation::ProcessExit,
        invalidation_inputs: ENVIRONMENT_INPUTS,
        claim_revision: 1,
        supported_probe_versions: PROBE_VERSIONS,
    },
    EdgeDefinition {
        id: EdgeId::ReplEnvironmentInheritance,
        key: EdgeKey {
            source: TrustDomain::Runner,
            destination: TrustDomain::Cell,
            direction: Direction::DestToGuest,
            carrier: Carrier::ProcessEnv,
            process_class: ProcessClass::Repl,
            effect: EffectClass::Write,
            policy_mode: PolicyMode::NotApplicable,
        },
        authentication: Authentication::None,
        declared_envelopes: NO_ENVELOPES,
        mediation: MediationPoint::Unmediated,
        scoping: EdgeScoping::Runner,
        mutability: Mutability::Mutable,
        persistence: Persistence::Process,
        audit_evidence: AuditEvidence::None,
        revocation: Revocation::ProcessExit,
        invalidation_inputs: ENVIRONMENT_INPUTS,
        claim_revision: 1,
        supported_probe_versions: PROBE_VERSIONS,
    },
    EdgeDefinition {
        id: EdgeId::WorkflowEnvironmentInheritance,
        key: EdgeKey {
            source: TrustDomain::Runner,
            destination: TrustDomain::Cell,
            direction: Direction::DestToGuest,
            carrier: Carrier::ProcessEnv,
            process_class: ProcessClass::Workflow,
            effect: EffectClass::Write,
            policy_mode: PolicyMode::NotApplicable,
        },
        authentication: Authentication::None,
        declared_envelopes: NO_ENVELOPES,
        mediation: MediationPoint::Unmediated,
        scoping: EdgeScoping::Runner,
        mutability: Mutability::Mutable,
        persistence: Persistence::Process,
        audit_evidence: AuditEvidence::None,
        revocation: Revocation::ProcessExit,
        invalidation_inputs: ENVIRONMENT_INPUTS,
        claim_revision: 1,
        supported_probe_versions: PROBE_VERSIONS,
    },
    EdgeDefinition {
        id: EdgeId::DevInstanceEnvironmentInheritance,
        key: EdgeKey {
            source: TrustDomain::Runner,
            destination: TrustDomain::Cell,
            direction: Direction::DestToGuest,
            carrier: Carrier::ProcessEnv,
            process_class: ProcessClass::DevInstance,
            effect: EffectClass::Write,
            policy_mode: PolicyMode::NotApplicable,
        },
        authentication: Authentication::None,
        declared_envelopes: NO_ENVELOPES,
        mediation: MediationPoint::Unmediated,
        scoping: EdgeScoping::Runner,
        mutability: Mutability::Mutable,
        persistence: Persistence::Process,
        audit_evidence: AuditEvidence::None,
        revocation: Revocation::ProcessExit,
        invalidation_inputs: ENVIRONMENT_INPUTS,
        claim_revision: 1,
        supported_probe_versions: PROBE_VERSIONS,
    },
    EdgeDefinition {
        id: EdgeId::DependencyCatalogPublish,
        key: EdgeKey {
            source: TrustDomain::Cell,
            destination: TrustDomain::MachineShared,
            direction: Direction::GuestToDest,
            carrier: Carrier::ObjectChannel,
            process_class: ProcessClass::BatchSpawn,
            effect: EffectClass::Publish,
            policy_mode: PolicyMode::NotApplicable,
        },
        authentication: Authentication::None,
        declared_envelopes: NO_ENVELOPES,
        mediation: MediationPoint::DependencyCatalog,
        scoping: EdgeScoping::Machine,
        mutability: Mutability::Mutable,
        persistence: Persistence::Machine,
        audit_evidence: AuditEvidence::ArtifactJournal,
        revocation: Revocation::ConfigurationChange,
        invalidation_inputs: CATALOG_INPUTS,
        claim_revision: 1,
        supported_probe_versions: PROBE_VERSIONS,
    },
    EdgeDefinition {
        id: EdgeId::DependencyCatalogConsume,
        key: EdgeKey {
            source: TrustDomain::MachineShared,
            destination: TrustDomain::Cell,
            direction: Direction::DestToGuest,
            carrier: Carrier::ObjectChannel,
            process_class: ProcessClass::BatchSpawn,
            effect: EffectClass::Consume,
            policy_mode: PolicyMode::NotApplicable,
        },
        authentication: Authentication::None,
        declared_envelopes: NO_ENVELOPES,
        mediation: MediationPoint::DependencyCatalog,
        scoping: EdgeScoping::Machine,
        mutability: Mutability::Mutable,
        persistence: Persistence::Process,
        audit_evidence: AuditEvidence::ArtifactJournal,
        revocation: Revocation::ProcessExit,
        invalidation_inputs: CATALOG_INPUTS,
        claim_revision: 1,
        supported_probe_versions: PROBE_VERSIONS,
    },
    EdgeDefinition {
        id: EdgeId::CargoRegistryCache,
        key: EdgeKey {
            source: TrustDomain::MachineShared,
            destination: TrustDomain::Cell,
            direction: Direction::Bidirectional,
            carrier: Carrier::PackageManager,
            process_class: ProcessClass::BatchSpawn,
            effect: EffectClass::Consume,
            policy_mode: PolicyMode::NotApplicable,
        },
        authentication: Authentication::None,
        declared_envelopes: NO_ENVELOPES,
        mediation: MediationPoint::PackageManager,
        scoping: EdgeScoping::Machine,
        mutability: Mutability::Mutable,
        persistence: Persistence::Machine,
        audit_evidence: AuditEvidence::None,
        revocation: Revocation::ConfigurationChange,
        invalidation_inputs: PACKAGE_INPUTS,
        claim_revision: 1,
        supported_probe_versions: PROBE_VERSIONS,
    },
    EdgeDefinition {
        id: EdgeId::NpmCache,
        key: EdgeKey {
            source: TrustDomain::MachineShared,
            destination: TrustDomain::Cell,
            direction: Direction::Bidirectional,
            carrier: Carrier::PackageManager,
            process_class: ProcessClass::BatchSpawn,
            effect: EffectClass::Consume,
            policy_mode: PolicyMode::NotApplicable,
        },
        authentication: Authentication::None,
        declared_envelopes: NO_ENVELOPES,
        mediation: MediationPoint::PackageManager,
        scoping: EdgeScoping::Machine,
        mutability: Mutability::Mutable,
        persistence: Persistence::Machine,
        audit_evidence: AuditEvidence::None,
        revocation: Revocation::ConfigurationChange,
        invalidation_inputs: PACKAGE_INPUTS,
        claim_revision: 1,
        supported_probe_versions: PROBE_VERSIONS,
    },
    EdgeDefinition {
        id: EdgeId::BunCache,
        key: EdgeKey {
            source: TrustDomain::MachineShared,
            destination: TrustDomain::Cell,
            direction: Direction::Bidirectional,
            carrier: Carrier::PackageManager,
            process_class: ProcessClass::BatchSpawn,
            effect: EffectClass::Consume,
            policy_mode: PolicyMode::NotApplicable,
        },
        authentication: Authentication::None,
        declared_envelopes: NO_ENVELOPES,
        mediation: MediationPoint::PackageManager,
        scoping: EdgeScoping::Machine,
        mutability: Mutability::Mutable,
        persistence: Persistence::Machine,
        audit_evidence: AuditEvidence::None,
        revocation: Revocation::ConfigurationChange,
        invalidation_inputs: PACKAGE_INPUTS,
        claim_revision: 1,
        supported_probe_versions: PROBE_VERSIONS,
    },
    EdgeDefinition {
        id: EdgeId::ToolchainCache,
        key: EdgeKey {
            source: TrustDomain::MachineShared,
            destination: TrustDomain::Cell,
            direction: Direction::Bidirectional,
            carrier: Carrier::Filesystem,
            process_class: ProcessClass::BatchSpawn,
            effect: EffectClass::Write,
            policy_mode: PolicyMode::NotApplicable,
        },
        authentication: Authentication::None,
        declared_envelopes: NO_ENVELOPES,
        mediation: MediationPoint::Unmediated,
        scoping: EdgeScoping::Machine,
        mutability: Mutability::Mutable,
        persistence: Persistence::Machine,
        audit_evidence: AuditEvidence::None,
        revocation: Revocation::ConfigurationChange,
        invalidation_inputs: PACKAGE_INPUTS,
        claim_revision: 1,
        supported_probe_versions: PROBE_VERSIONS,
    },
    EdgeDefinition {
        id: EdgeId::CellScratchWithinResidency,
        key: EdgeKey {
            source: TrustDomain::Cell,
            destination: TrustDomain::Cell,
            direction: Direction::Bidirectional,
            carrier: Carrier::Filesystem,
            process_class: ProcessClass::BatchSpawn,
            effect: EffectClass::Write,
            policy_mode: PolicyMode::NotApplicable,
        },
        authentication: Authentication::None,
        declared_envelopes: NO_ENVELOPES,
        mediation: MediationPoint::CellLease,
        scoping: EdgeScoping::Cell,
        mutability: Mutability::Mutable,
        persistence: Persistence::Residency,
        audit_evidence: AuditEvidence::CellJournal,
        revocation: Revocation::CellRelease,
        invalidation_inputs: LEASE_INPUTS,
        claim_revision: 1,
        supported_probe_versions: PROBE_VERSIONS,
    },
    // Retained pool state crosses occupants: matching affinity preserves scratch,
    // while another claim stages scratch after quiescence. Ignored checkout
    // products remain a residual mutable channel, not an isolation guarantee.
    // At each pipe/PTY materialization, claim_bound_resident_policy resolves a
    // fresh fenced current (cell_id, holder, cell_epoch) snapshot and derives the
    // policy from its checkout and scratch plus the arriving process spec. No
    // policy is cached across occupants or assumed known at claim acquisition.
    EdgeDefinition {
        id: EdgeId::CellScratchAcrossOccupants,
        key: EdgeKey {
            source: TrustDomain::Cell,
            destination: TrustDomain::PeerCell,
            direction: Direction::GuestToDest,
            carrier: Carrier::Filesystem,
            process_class: ProcessClass::BatchSpawn,
            effect: EffectClass::Write,
            policy_mode: PolicyMode::NotApplicable,
        },
        authentication: Authentication::None,
        declared_envelopes: NO_ENVELOPES,
        mediation: MediationPoint::CellLease,
        scoping: EdgeScoping::Machine,
        mutability: Mutability::Mutable,
        persistence: Persistence::Machine,
        audit_evidence: AuditEvidence::CellJournal,
        revocation: Revocation::CellRelease,
        invalidation_inputs: LEASE_INPUTS,
        claim_revision: 2,
        supported_probe_versions: PROBE_VERSIONS,
    },
    EdgeDefinition {
        id: EdgeId::SiblingCellTree,
        key: EdgeKey {
            source: TrustDomain::Cell,
            destination: TrustDomain::PeerCell,
            direction: Direction::Bidirectional,
            carrier: Carrier::Filesystem,
            process_class: ProcessClass::BatchSpawn,
            effect: EffectClass::Read,
            policy_mode: PolicyMode::NotApplicable,
        },
        authentication: Authentication::None,
        declared_envelopes: NO_ENVELOPES,
        mediation: MediationPoint::CellLease,
        scoping: EdgeScoping::Machine,
        mutability: Mutability::Mutable,
        persistence: Persistence::Machine,
        audit_evidence: AuditEvidence::CellJournal,
        revocation: Revocation::CellRelease,
        invalidation_inputs: LEASE_INPUTS,
        claim_revision: 1,
        supported_probe_versions: PROBE_VERSIONS,
    },
    EdgeDefinition {
        id: EdgeId::McpRelayPerBatchCapability,
        key: EdgeKey {
            source: TrustDomain::Cell,
            destination: TrustDomain::Runner,
            direction: Direction::Bidirectional,
            carrier: Carrier::McpRelay,
            process_class: ProcessClass::BatchSpawn,
            effect: EffectClass::Invoke,
            policy_mode: PolicyMode::NotApplicable,
        },
        authentication: Authentication::ExecutorEnrollment,
        declared_envelopes: BATCH_MCP_ENVELOPES,
        mediation: MediationPoint::McpRelay,
        scoping: EdgeScoping::Batch,
        mutability: Mutability::Mutable,
        persistence: Persistence::Batch,
        audit_evidence: AuditEvidence::CallbackJournal,
        revocation: Revocation::BatchCompletion,
        invalidation_inputs: RELAY_INPUTS,
        claim_revision: 1,
        supported_probe_versions: PROBE_VERSIONS,
    },
    EdgeDefinition {
        id: EdgeId::McpRelayResidentRunnerWide,
        key: EdgeKey {
            source: TrustDomain::Cell,
            destination: TrustDomain::Runner,
            direction: Direction::Bidirectional,
            carrier: Carrier::McpRelay,
            process_class: ProcessClass::PtyResident,
            effect: EffectClass::Invoke,
            policy_mode: PolicyMode::NotApplicable,
        },
        authentication: Authentication::ExecutorEnrollment,
        declared_envelopes: NO_ENVELOPES,
        mediation: MediationPoint::Unmediated,
        scoping: EdgeScoping::Runner,
        mutability: Mutability::Mutable,
        persistence: Persistence::Runner,
        audit_evidence: AuditEvidence::CallbackJournal,
        revocation: Revocation::RunnerRestart,
        invalidation_inputs: RELAY_INPUTS,
        claim_revision: 1,
        supported_probe_versions: PROBE_VERSIONS,
    },
    EdgeDefinition {
        id: EdgeId::ArtifactGuestToPublisher,
        key: EdgeKey {
            source: TrustDomain::Cell,
            destination: TrustDomain::ExecutorHost,
            direction: Direction::GuestToDest,
            carrier: Carrier::Filesystem,
            process_class: ProcessClass::BatchSpawn,
            effect: EffectClass::Publish,
            policy_mode: PolicyMode::NotApplicable,
        },
        authentication: Authentication::None,
        declared_envelopes: NO_ENVELOPES,
        mediation: MediationPoint::ArtifactPublisher,
        scoping: EdgeScoping::Execution,
        mutability: Mutability::Mutable,
        persistence: Persistence::Execution,
        audit_evidence: AuditEvidence::ArtifactJournal,
        revocation: Revocation::ExecutionCompletion,
        invalidation_inputs: ARTIFACT_INPUTS,
        claim_revision: 1,
        supported_probe_versions: PROBE_VERSIONS,
    },
    EdgeDefinition {
        id: EdgeId::ArtifactPublisherToStore,
        key: EdgeKey {
            source: TrustDomain::ExecutorHost,
            destination: TrustDomain::Runner,
            direction: Direction::GuestToDest,
            carrier: Carrier::ExecutorLink,
            process_class: ProcessClass::ExecutorItself,
            effect: EffectClass::Publish,
            policy_mode: PolicyMode::NotApplicable,
        },
        authentication: Authentication::ExecutorEnrollment,
        declared_envelopes: ARTIFACT_ENVELOPES,
        mediation: MediationPoint::ArtifactPublisher,
        scoping: EdgeScoping::Execution,
        mutability: Mutability::AppendOnly,
        persistence: Persistence::Workspace,
        audit_evidence: AuditEvidence::ArtifactJournal,
        revocation: Revocation::ExecutionCompletion,
        invalidation_inputs: ARTIFACT_INPUTS,
        claim_revision: 1,
        supported_probe_versions: PROBE_VERSIONS,
    },
    EdgeDefinition {
        id: EdgeId::ArtifactStoreToConsumer,
        key: EdgeKey {
            source: TrustDomain::Runner,
            destination: TrustDomain::PeerAgent,
            direction: Direction::DestToGuest,
            carrier: Carrier::ObjectChannel,
            process_class: ProcessClass::ExecutorItself,
            effect: EffectClass::Consume,
            policy_mode: PolicyMode::NotApplicable,
        },
        authentication: Authentication::ExecutorEnrollment,
        declared_envelopes: NO_ENVELOPES,
        mediation: MediationPoint::ArtifactPublisher,
        scoping: EdgeScoping::Workspace,
        mutability: Mutability::Mutable,
        persistence: Persistence::Workspace,
        audit_evidence: AuditEvidence::ArtifactJournal,
        revocation: Revocation::RunnerRestart,
        invalidation_inputs: ARTIFACT_INPUTS,
        claim_revision: 1,
        supported_probe_versions: PROBE_VERSIONS,
    },
    EdgeDefinition {
        id: EdgeId::ExecutorLinkControl,
        key: EdgeKey {
            source: TrustDomain::ExecutorHost,
            destination: TrustDomain::Runner,
            direction: Direction::Bidirectional,
            carrier: Carrier::ExecutorLink,
            process_class: ProcessClass::ExecutorItself,
            effect: EffectClass::Invoke,
            policy_mode: PolicyMode::NotApplicable,
        },
        authentication: Authentication::ExecutorEnrollment,
        declared_envelopes: HOST_ENVELOPES,
        mediation: MediationPoint::ExecutorLink,
        scoping: EdgeScoping::Executor,
        mutability: Mutability::Mutable,
        persistence: Persistence::Connection,
        audit_evidence: AuditEvidence::ProtocolJournal,
        revocation: Revocation::Disconnect,
        invalidation_inputs: LINK_INPUTS,
        claim_revision: 1,
        supported_probe_versions: PROBE_VERSIONS,
    },
    EdgeDefinition {
        id: EdgeId::CellLeaseControl,
        key: EdgeKey {
            source: TrustDomain::ExecutorHost,
            destination: TrustDomain::MachineShared,
            direction: Direction::Bidirectional,
            carrier: Carrier::Filesystem,
            process_class: ProcessClass::ExecutorItself,
            effect: EffectClass::Invoke,
            policy_mode: PolicyMode::NotApplicable,
        },
        authentication: Authentication::ExecutorEnrollment,
        declared_envelopes: HOST_ENVELOPES,
        mediation: MediationPoint::CellLease,
        scoping: EdgeScoping::Machine,
        mutability: Mutability::Mutable,
        persistence: Persistence::Machine,
        audit_evidence: AuditEvidence::CellJournal,
        revocation: Revocation::CellRelease,
        invalidation_inputs: LEASE_INPUTS,
        claim_revision: 1,
        supported_probe_versions: PROBE_VERSIONS,
    },
    EdgeDefinition {
        id: EdgeId::BuildServiceClientEnv,
        key: EdgeKey {
            source: TrustDomain::Cell,
            destination: TrustDomain::MachineShared,
            direction: Direction::Bidirectional,
            carrier: Carrier::ProcessEnv,
            process_class: ProcessClass::BatchSpawn,
            effect: EffectClass::Invoke,
            policy_mode: PolicyMode::NotApplicable,
        },
        authentication: Authentication::None,
        declared_envelopes: NO_ENVELOPES,
        mediation: MediationPoint::BuildService,
        scoping: EdgeScoping::Service,
        mutability: Mutability::Mutable,
        persistence: Persistence::Machine,
        audit_evidence: AuditEvidence::None,
        revocation: Revocation::ConfigurationChange,
        invalidation_inputs: SERVICE_INPUTS,
        claim_revision: 1,
        supported_probe_versions: PROBE_VERSIONS,
    },
    EdgeDefinition {
        id: EdgeId::BuildServiceStateDir,
        key: EdgeKey {
            source: TrustDomain::Cell,
            destination: TrustDomain::MachineShared,
            direction: Direction::GuestToDest,
            carrier: Carrier::Filesystem,
            process_class: ProcessClass::BatchSpawn,
            effect: EffectClass::Write,
            policy_mode: PolicyMode::NotApplicable,
        },
        authentication: Authentication::None,
        declared_envelopes: NO_ENVELOPES,
        mediation: MediationPoint::BuildService,
        scoping: EdgeScoping::Service,
        mutability: Mutability::Mutable,
        persistence: Persistence::Machine,
        audit_evidence: AuditEvidence::None,
        revocation: Revocation::ConfigurationChange,
        invalidation_inputs: SERVICE_INPUTS,
        claim_revision: 1,
        supported_probe_versions: PROBE_VERSIONS,
    },
    EdgeDefinition {
        id: EdgeId::DevInstanceExternalIngress,
        key: EdgeKey {
            source: TrustDomain::ExternalNetwork,
            destination: TrustDomain::Cell,
            direction: Direction::DestToGuest,
            carrier: Carrier::Socket,
            process_class: ProcessClass::DevInstance,
            effect: EffectClass::Invoke,
            policy_mode: PolicyMode::NotApplicable,
        },
        authentication: Authentication::None,
        declared_envelopes: NO_ENVELOPES,
        mediation: MediationPoint::Unmediated,
        scoping: EdgeScoping::Unscoped,
        mutability: Mutability::Mutable,
        persistence: Persistence::Process,
        audit_evidence: AuditEvidence::None,
        revocation: Revocation::ProcessExit,
        invalidation_inputs: INGRESS_INPUTS,
        claim_revision: 1,
        supported_probe_versions: PROBE_VERSIONS,
    },
    EdgeDefinition {
        id: EdgeId::DesktopAutomationControl,
        key: EdgeKey {
            source: TrustDomain::Cell,
            destination: TrustDomain::ExecutorHost,
            direction: Direction::GuestToDest,
            carrier: Carrier::Socket,
            process_class: ProcessClass::BatchSpawn,
            effect: EffectClass::Invoke,
            policy_mode: PolicyMode::NotApplicable,
        },
        authentication: Authentication::None,
        declared_envelopes: NO_ENVELOPES,
        mediation: MediationPoint::Unmediated,
        scoping: EdgeScoping::Machine,
        mutability: Mutability::Mutable,
        persistence: Persistence::Machine,
        audit_evidence: AuditEvidence::None,
        revocation: Revocation::ConfigurationChange,
        invalidation_inputs: DESKTOP_INPUTS,
        claim_revision: 1,
        supported_probe_versions: PROBE_VERSIONS,
    },
    // The runner's executor listener: the only Cairn socket bound off loopback.
    // Reaching it is unauthenticated by construction (anyone who can route to
    // the address can open a TCP connection), so what narrows it is the router
    // it serves — the executor link and its object plane, nothing else — and the
    // enrollment credential each request must carry. Revoked by configuration:
    // the listener exists only while an operator has configured one.
    EdgeDefinition {
        id: EdgeId::RunnerExecutorListenerIngress,
        key: EdgeKey {
            source: TrustDomain::ExternalNetwork,
            destination: TrustDomain::Runner,
            direction: Direction::GuestToDest,
            carrier: Carrier::Socket,
            process_class: ProcessClass::ExecutorItself,
            effect: EffectClass::Invoke,
            policy_mode: PolicyMode::NotApplicable,
        },
        authentication: Authentication::ExecutorEnrollment,
        declared_envelopes: NO_ENVELOPES,
        mediation: MediationPoint::ExecutorLink,
        scoping: EdgeScoping::Runner,
        mutability: Mutability::Mutable,
        persistence: Persistence::Runner,
        audit_evidence: AuditEvidence::ProtocolJournal,
        revocation: Revocation::ConfigurationChange,
        invalidation_inputs: LISTENER_INPUTS,
        claim_revision: 1,
        supported_probe_versions: PROBE_VERSIONS,
    },
    // The executor's dial-out terminates at the runner authenticated by a pinned
    // certificate fingerprint. This is not ambient external reach; that narrow
    // destination is exactly why the credential must never reach an unpinned peer.
    EdgeDefinition {
        id: EdgeId::ExecutorDialOutCredential,
        key: EdgeKey {
            source: TrustDomain::ExecutorHost,
            destination: TrustDomain::Runner,
            direction: Direction::GuestToDest,
            carrier: Carrier::Socket,
            process_class: ProcessClass::ExecutorItself,
            effect: EffectClass::Invoke,
            policy_mode: PolicyMode::NotApplicable,
        },
        authentication: Authentication::ExecutorEnrollment,
        declared_envelopes: NO_ENVELOPES,
        mediation: MediationPoint::ExecutorLink,
        scoping: EdgeScoping::Executor,
        mutability: Mutability::Mutable,
        persistence: Persistence::Connection,
        audit_evidence: AuditEvidence::ProtocolJournal,
        revocation: Revocation::Disconnect,
        invalidation_inputs: LISTENER_INPUTS,
        claim_revision: 1,
        supported_probe_versions: PROBE_VERSIONS,
    },
    // The executor pulls its own artifact from the runner's narrow listener and
    // verifies its size and sha256 before it replaces anything. This is not
    // ambient executor egress: the pinned listener and enrollment credential are
    // the same authenticated boundary as the executor link.
    EdgeDefinition {
        id: EdgeId::ExecutorArtifactPull,
        key: EdgeKey {
            source: TrustDomain::ExecutorHost,
            destination: TrustDomain::Runner,
            direction: Direction::GuestToDest,
            carrier: Carrier::Socket,
            process_class: ProcessClass::ExecutorItself,
            effect: EffectClass::Consume,
            policy_mode: PolicyMode::NotApplicable,
        },
        authentication: Authentication::ExecutorEnrollment,
        declared_envelopes: NO_ENVELOPES,
        mediation: MediationPoint::ArtifactPublisher,
        scoping: EdgeScoping::Executor,
        mutability: Mutability::Mutable,
        persistence: Persistence::Machine,
        audit_evidence: AuditEvidence::ArtifactJournal,
        revocation: Revocation::ConfigurationChange,
        invalidation_inputs: ARTIFACT_INPUTS,
        claim_revision: 1,
        supported_probe_versions: PROBE_VERSIONS,
    },
    // The runner is the fleet's sole external artifact ingress. It resolves a
    // release-cache entry, verifies the published manifest, and only then serves
    // those bytes to an enrolled executor over the separately inventoried pull.
    EdgeDefinition {
        id: EdgeId::RunnerArtifactReleaseFetch,
        key: EdgeKey {
            source: TrustDomain::Runner,
            destination: TrustDomain::ExternalNetwork,
            direction: Direction::GuestToDest,
            carrier: Carrier::Socket,
            process_class: ProcessClass::Runner,
            effect: EffectClass::Consume,
            policy_mode: PolicyMode::NotApplicable,
        },
        authentication: Authentication::None,
        declared_envelopes: NO_ENVELOPES,
        mediation: MediationPoint::ArtifactPublisher,
        scoping: EdgeScoping::Runner,
        mutability: Mutability::Mutable,
        persistence: Persistence::Machine,
        audit_evidence: AuditEvidence::ArtifactJournal,
        revocation: Revocation::ConfigurationChange,
        invalidation_inputs: ARTIFACT_INPUTS,
        claim_revision: 1,
        supported_probe_versions: PROBE_VERSIONS,
    },
    // Offering one queue entry to one machine. Narrower than
    // `ExecutorLinkControl`, which is the whole bidirectional, mutable link this
    // rides on: an offer travels one way, confers nothing, and appends to a
    // journal rather than mutating anything the machine can reach. The grant
    // that DOES confer access is a separate message on the link edge, which is
    // the distinction this row exists to make legible.
    EdgeDefinition {
        id: EdgeId::ExecutionQueueOffer,
        key: EdgeKey {
            source: TrustDomain::Runner,
            destination: TrustDomain::ExecutorHost,
            direction: Direction::DestToGuest,
            carrier: Carrier::ExecutorLink,
            process_class: ProcessClass::Runner,
            effect: EffectClass::Consume,
            policy_mode: PolicyMode::NotApplicable,
        },
        authentication: Authentication::ExecutorEnrollment,
        declared_envelopes: HOST_ENVELOPES,
        mediation: MediationPoint::ExecutionQueue,
        scoping: EdgeScoping::Execution,
        mutability: Mutability::AppendOnly,
        audit_evidence: AuditEvidence::ProtocolJournal,
        persistence: Persistence::Connection,
        revocation: Revocation::Disconnect,
        invalidation_inputs: LINK_INPUTS,
        claim_revision: 1,
        supported_probe_versions: PROBE_VERSIONS,
    },
];
// CONTAINMENT_INVENTORY_END

pub const fn containment_edge_definition(edge: EdgeId) -> &'static EdgeDefinition {
    &CONTAINMENT_EDGE_DEFINITIONS[edge as usize]
}

pub const CONTAINMENT_EDGE_IDS: [EdgeId; EdgeId::COUNT] = [
    EdgeId::BatchSpawnFilesystemConfined,
    EdgeId::BatchSpawnFilesystemReadOnlyCheckout,
    EdgeId::BatchSpawnFilesystemUnconfined,
    EdgeId::PipedResidentFilesystemConfined,
    EdgeId::PipedResidentFilesystemReadOnlyCheckout,
    EdgeId::PipedResidentFilesystemUnconfined,
    EdgeId::PtyResidentFilesystemConfined,
    EdgeId::PtyResidentFilesystemReadOnlyCheckout,
    EdgeId::PtyResidentFilesystemUnconfined,
    EdgeId::ReplFilesystemConfined,
    EdgeId::ReplFilesystemReadOnlyCheckout,
    EdgeId::ReplFilesystemUnconfined,
    EdgeId::WorkflowFilesystemConfined,
    EdgeId::WorkflowFilesystemReadOnlyCheckout,
    EdgeId::WorkflowFilesystemUnconfined,
    EdgeId::DevInstanceFilesystemConfined,
    EdgeId::DevInstanceFilesystemReadOnlyCheckout,
    EdgeId::DevInstanceFilesystemUnconfined,
    EdgeId::BatchSpawnEgress,
    EdgeId::PipedResidentEgress,
    EdgeId::PtyResidentEgress,
    EdgeId::ReplEgress,
    EdgeId::WorkflowEgress,
    EdgeId::DevInstanceEgress,
    EdgeId::BatchSpawnEnvironmentInheritance,
    EdgeId::PipedResidentEnvironmentInheritance,
    EdgeId::PtyResidentEnvironmentInheritance,
    EdgeId::ReplEnvironmentInheritance,
    EdgeId::WorkflowEnvironmentInheritance,
    EdgeId::DevInstanceEnvironmentInheritance,
    EdgeId::DependencyCatalogPublish,
    EdgeId::DependencyCatalogConsume,
    EdgeId::CargoRegistryCache,
    EdgeId::NpmCache,
    EdgeId::BunCache,
    EdgeId::ToolchainCache,
    EdgeId::CellScratchWithinResidency,
    EdgeId::CellScratchAcrossOccupants,
    EdgeId::SiblingCellTree,
    EdgeId::McpRelayPerBatchCapability,
    EdgeId::McpRelayResidentRunnerWide,
    EdgeId::ArtifactGuestToPublisher,
    EdgeId::ArtifactPublisherToStore,
    EdgeId::ArtifactStoreToConsumer,
    EdgeId::ExecutorLinkControl,
    EdgeId::CellLeaseControl,
    EdgeId::BuildServiceClientEnv,
    EdgeId::BuildServiceStateDir,
    EdgeId::DevInstanceExternalIngress,
    EdgeId::DesktopAutomationControl,
    EdgeId::RunnerExecutorListenerIngress,
    EdgeId::ExecutorDialOutCredential,
    EdgeId::ExecutorArtifactPull,
    EdgeId::RunnerArtifactReleaseFetch,
    EdgeId::ExecutionQueueOffer,
];
const _: () = assert!(CONTAINMENT_EDGE_IDS.len() == EdgeId::COUNT);

/// Render the canonical table from the same exhaustive definitions used by the wire model.
pub fn containment_inventory_markdown() -> String {
    let mut output = String::from(
        "| Edge | Source | Destination | Direction | Carrier | Process | Effect | Mediation | Invalidation |\n|---|---|---|---|---|---|---|---|---|\n",
    );
    for id in CONTAINMENT_EDGE_IDS {
        let edge = id.definition();
        let invalidation = edge
            .invalidation_inputs
            .iter()
            .map(|input| format!("{input:?}"))
            .collect::<Vec<_>>()
            .join(", ");
        output.push_str(&format!(
            "| {id:?} | {:?} | {:?} | {:?} | {:?} | {:?} | {:?} | {:?} | {invalidation} |\n",
            edge.key.source,
            edge.key.destination,
            edge.key.direction,
            edge.key.carrier,
            edge.key.process_class,
            edge.key.effect,
            edge.mediation,
        ));
    }
    output
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Fingerprint {
    pub algorithm: FingerprintAlgorithm,
    pub digest: String,
    /// Exact ordered set of inputs included in the digest. A positive observation
    /// is invalid unless this equals the edge definition's complete declaration.
    pub inputs: Vec<InvalidationInput>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum FingerprintAlgorithm {
    Sha256,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PositiveScope {
    pub executor_build_id: String,
    pub boot_generation: String,
    pub platform: RemotePlatform,
    pub process_class: ProcessClass,
    pub carrier: Carrier,
    /// The runtime policy the probe actually exercised. A measurement of the
    /// confined path says nothing about the unconfined one even at the same
    /// seam, so this must equal the edge's own mode.
    pub policy_mode: PolicyMode,
    pub probe_version: ProbeVersion,
    pub claim_revision: u32,
    pub invalidation_fingerprint: Fingerprint,
}

/// What a probe actually attempted, and what came back.
///
/// `positive_control_passed` is what separates a contained machine from a
/// broken one. A probe that reports "the out-of-bounds write was refused" has
/// established nothing unless the same probe also performed the in-bounds write
/// it was supposed to be allowed: a harness that cannot write anywhere at all,
/// or that never ran its command, produces exactly the same refusal as a
/// working sandbox. Placement therefore rejects a positive disposition whose
/// positive control did not pass, rather than reading a broken seam as a strong
/// one.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ObservedEffect {
    pub effect: EffectClass,
    /// Whether the in-bounds control crossing this probe was supposed to be
    /// permitted actually succeeded.
    pub positive_control_passed: bool,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum EdgeDisposition {
    Refused {
        scope: PositiveScope,
        observed: ObservedEffect,
    },
    Constrained {
        scope: PositiveScope,
        envelope: EnvelopeId,
        observed: ObservedEffect,
    },
    Ambient {
        observed: ObservedEffect,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        scope: Option<PositiveScope>,
    },
    Unmeasured {
        reason: String,
    },
}

impl EdgeDisposition {
    pub fn positive_scope(&self) -> Option<&PositiveScope> {
        match self {
            Self::Refused { scope, .. } | Self::Constrained { scope, .. } => Some(scope),
            Self::Ambient { scope, .. } => scope.as_ref(),
            Self::Unmeasured { .. } => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EdgeObservation {
    pub edge: EdgeId,
    pub disposition: EdgeDisposition,
    pub observed_at_unix_ms: u64,
}

/// One edge's invalidation fingerprint as it stands *now*, recomputed when this
/// advertisement was assembled.
///
/// `fingerprint: None` means an input the edge declares could not be read or
/// enumerated on this machine at this moment, so no positive disposition on
/// that edge can be shown to be current. It is not the same as an edge with no
/// declared inputs, which fingerprints over an empty set and is always current.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EdgeFingerprint {
    pub edge: EdgeId,
    pub fingerprint: Option<Fingerprint>,
}

/// The containment matrix one machine advertises about itself.
///
/// The build, boot, and platform fields are the *live* identity of the process
/// that assembled this advertisement, restated beside the observations so a
/// measurement can be checked for currency rather than trusted for existing. A
/// probe result is only evidence about the machine it is read on if it was
/// taken by this build, during this boot, on this platform, over inputs that
/// have not changed since. Anything else is history.
///
/// This is defence against staleness, not against dishonesty. Evidence arriving
/// over the executor link is a claim a machine makes about itself, authenticated
/// by enrolment identity; a compromised executor is outside what this model
/// contains, and `docs/security/executor-containment.md` says so plainly.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ContainmentObservations {
    /// The build of the executor that assembled this advertisement.
    pub executor_build_id: String,
    /// This executor process's boot generation.
    pub boot_generation: String,
    /// The platform this machine is, from the closed published vocabulary. An
    /// executor on a target Cairn does not publish reports no observations at
    /// all rather than inventing a platform name.
    pub platform: Option<RemotePlatform>,
    /// Current value of each edge's declared invalidation inputs.
    pub current_fingerprints: Vec<EdgeFingerprint>,
    pub observations: Vec<EdgeObservation>,
}

impl ContainmentObservations {
    fn current_fingerprint(&self, edge: EdgeId) -> Option<&Fingerprint> {
        self.current_fingerprints
            .iter()
            .find(|entry| entry.edge == edge)
            .and_then(|entry| entry.fingerprint.as_ref())
    }

    /// Whether this machine's measured graph satisfies a requested envelope,
    /// and if not, which edge failed and why.
    ///
    /// Every check exists because skipping it would let a weaker fact stand in
    /// for a stronger one. Scope must match the seam requested, so a batch
    /// measurement never certifies a terminal. It must match the live build,
    /// boot, and platform, so evidence that survived an upgrade or a reboot is
    /// not replayed as current. Its fingerprint must equal the machine's
    /// current one, so a changed profile, mount, helper, or policy invalidates
    /// the result rather than inheriting its verdict. The positive control must
    /// have passed, so a broken harness cannot masquerade as a strong boundary.
    ///
    /// `Unmeasured` is never weighed against the bound. It is incomparable and
    /// refuses, including when the omission is merely an older executor.
    pub fn satisfies(&self, envelope: &WorkloadEnvelope) -> Result<(), ContainmentRefusal> {
        for requirement in &envelope.edges {
            self.satisfies_edge(requirement)
                .map_err(|deficiency| ContainmentRefusal {
                    edge: requirement.edge,
                    deficiency,
                })?;
        }
        Ok(())
    }

    fn satisfies_edge(&self, requirement: &EdgeRequirement) -> Result<(), ContainmentDeficiency> {
        let edge = requirement.edge;
        let definition = edge.definition();
        let Some(observation) = self
            .observations
            .iter()
            .find(|observation| observation.edge == edge)
        else {
            return Err(ContainmentDeficiency::NotObserved);
        };

        let observed_bound = match &observation.disposition {
            EdgeDisposition::Unmeasured { reason } => {
                return Err(ContainmentDeficiency::Unmeasured(reason.clone()))
            }
            EdgeDisposition::Refused { .. } => MaximumDisposition::Refused,
            EdgeDisposition::Constrained { envelope, .. } => MaximumDisposition::Constrained {
                envelope: *envelope,
            },
            EdgeDisposition::Ambient { .. } => MaximumDisposition::Ambient,
        };

        // Ambient is the weakest claim there is, so it needs no scope to be
        // believed. It still has to be permitted by the request.
        if let Some(scope) = observation.disposition.positive_scope() {
            if scope.process_class != definition.key.process_class
                || scope.carrier != definition.key.carrier
                || scope.policy_mode != definition.key.policy_mode
            {
                return Err(ContainmentDeficiency::NotObserved);
            }
            if scope.executor_build_id != self.executor_build_id {
                return Err(ContainmentDeficiency::StaleBuild);
            }
            if scope.boot_generation != self.boot_generation {
                return Err(ContainmentDeficiency::PriorBoot);
            }
            if Some(scope.platform) != self.platform {
                return Err(ContainmentDeficiency::PlatformMismatch);
            }
            match self.current_fingerprint(edge) {
                None => return Err(ContainmentDeficiency::FingerprintUncomputable),
                Some(current) if *current != scope.invalidation_fingerprint => {
                    return Err(ContainmentDeficiency::FingerprintChanged)
                }
                Some(_) => {}
            }
        }

        if let EdgeDisposition::Refused { observed, .. }
        | EdgeDisposition::Constrained { observed, .. } = &observation.disposition
        {
            if !observed.positive_control_passed {
                return Err(ContainmentDeficiency::PositiveControlFailed);
            }
        }

        if maximum_disposition_permits(&observed_bound, &requirement.maximum_disposition) {
            Ok(())
        } else {
            Err(ContainmentDeficiency::TooPermissive(format!(
                "{observed_bound:?}"
            )))
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        let mut seen = std::collections::HashSet::new();
        for observation in &self.observations {
            if !seen.insert(observation.edge) {
                return Err(format!(
                    "duplicate containment observation for {:?}",
                    observation.edge
                ));
            }
            let definition = observation.edge.definition();
            if let Some(scope) = observation.disposition.positive_scope() {
                if scope.process_class != definition.key.process_class
                    || scope.carrier != definition.key.carrier
                    || scope.policy_mode != definition.key.policy_mode
                {
                    return Err(format!(
                        "containment observation for {:?} carries scope for another seam",
                        observation.edge
                    ));
                }
                if scope.claim_revision != definition.claim_revision {
                    return Err(format!(
                        "containment observation for {:?} carries a stale claim revision",
                        observation.edge
                    ));
                }
                if !definition
                    .supported_probe_versions
                    .contains(&scope.probe_version)
                {
                    return Err(format!(
                        "containment observation for {:?} carries an unsupported probe version",
                        observation.edge
                    ));
                }
                if scope.invalidation_fingerprint.inputs != definition.invalidation_inputs {
                    return Err(format!(
                        "containment observation for {:?} has an incomplete invalidation fingerprint",
                        observation.edge
                    ));
                }
            }
            if let EdgeDisposition::Constrained { envelope, .. } = &observation.disposition {
                if !definition.declared_envelopes.contains(envelope) {
                    return Err(format!(
                        "envelope {envelope:?} is not declared for {:?}",
                        observation.edge
                    ));
                }
            }
        }
        Ok(())
    }
}

/// Whether an observed disposition is no more permissive than a requested bound.
///
/// The order is per edge rather than global, because envelopes are edge-owned:
/// two edges may both declare an envelope named for a broker without those
/// envelopes meaning the same thing, so there is no fleet-wide ladder to sort
/// them on. Today no edge declares a nested envelope family, so containment
/// between two envelopes is equality; the moment one does, its ordering is
/// stated here rather than inferred from a name.
fn maximum_disposition_permits(
    observed: &MaximumDisposition,
    allowed: &MaximumDisposition,
) -> bool {
    match (observed, allowed) {
        (MaximumDisposition::Refused, _) => true,
        (MaximumDisposition::Constrained { .. }, MaximumDisposition::Refused) => false,
        (
            MaximumDisposition::Constrained { envelope: observed },
            MaximumDisposition::Constrained { envelope: allowed },
        ) => observed == allowed,
        (MaximumDisposition::Constrained { .. }, MaximumDisposition::Ambient) => true,
        (MaximumDisposition::Ambient, MaximumDisposition::Ambient) => true,
        (MaximumDisposition::Ambient, _) => false,
    }
}

/// Why one edge failed to satisfy a requested envelope.
///
/// These are deliberately distinct rather than one "not contained" verdict,
/// because they send an operator to different places: a missing probe is a gap
/// in Cairn, a stale build is a machine that needs to re-advertise, a changed
/// fingerprint is a host whose configuration moved, and a failed positive
/// control is a broken probe harness reporting a strength it never measured.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContainmentDeficiency {
    /// This executor advertises no containment matrix at all.
    NotReported,
    /// The matrix carries no observation for this edge.
    NotObserved,
    /// The edge was probed and the probe could not conclude.
    Unmeasured(String),
    /// The probe ran under a different executor build than the one advertising.
    StaleBuild,
    /// The probe ran during an earlier boot of this executor.
    PriorBoot,
    /// The probe ran on a different platform than this machine reports.
    PlatformMismatch,
    /// The machine can no longer compute this edge's invalidation fingerprint,
    /// naming the inputs it could not read.
    FingerprintUncomputable,
    /// A declared invalidation input changed since the probe.
    FingerprintChanged,
    /// The probe's in-bounds control crossing did not succeed, so its refusal
    /// is not evidence of containment.
    PositiveControlFailed,
    /// The measured disposition is more permissive than the envelope allows.
    TooPermissive(String),
}

impl ContainmentDeficiency {
    pub fn describe(&self) -> String {
        match self {
            Self::NotReported => {
                "this executor predates containment reporting, so no edge has evidence".into()
            }
            Self::NotObserved => "no probe attempts this seam yet".into(),
            Self::Unmeasured(reason) => format!("Unmeasured ({reason})"),
            Self::StaleBuild => {
                "measured by a different executor build than the one now advertising".into()
            }
            Self::PriorBoot => "measured during an earlier boot of this executor".into(),
            Self::PlatformMismatch => "measured on a different platform than this machine".into(),
            Self::FingerprintUncomputable => {
                "this machine cannot currently read every input that could widen this edge".into()
            }
            Self::FingerprintChanged => {
                "a declared invalidation input changed since the probe, so the result is history"
                    .into()
            }
            Self::PositiveControlFailed => {
                "the probe's in-bounds control crossing failed, so its refusal measures a broken seam rather than a contained one"
                    .into()
            }
            Self::TooPermissive(observed) => {
                format!("observed {observed}, which is more permissive than this workload allows")
            }
        }
    }
}

/// A placement refusal that names the edge it failed on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContainmentRefusal {
    pub edge: EdgeId,
    pub deficiency: ContainmentDeficiency,
}

impl ContainmentRefusal {
    pub fn describe(&self) -> String {
        format!("{:?} is {}", self.edge, self.deficiency.describe())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EdgeRequirement {
    pub edge: EdgeId,
    pub maximum_disposition: MaximumDisposition,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum MaximumDisposition {
    Refused,
    Constrained { envelope: EnvelopeId },
    Ambient,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkloadEnvelope {
    pub process_class: ProcessClass,
    pub edges: Vec<EdgeRequirement>,
}

impl WorkloadEnvelope {
    pub fn validate(&self) -> Result<(), String> {
        if self.edges.is_empty() {
            return Err("a workload containment envelope must state at least one edge".into());
        }
        let mut seen = std::collections::HashSet::new();
        for requirement in &self.edges {
            if !seen.insert(requirement.edge) {
                return Err(format!(
                    "duplicate containment requirement for {:?}",
                    requirement.edge
                ));
            }
            let definition = requirement.edge.definition();
            if definition.key.process_class != self.process_class
                && definition.key.process_class != ProcessClass::ExecutorItself
            {
                return Err(format!(
                    "containment requirement for {:?} belongs to another process class",
                    requirement.edge
                ));
            }
            if let MaximumDisposition::Constrained { envelope } = &requirement.maximum_disposition {
                if !definition.declared_envelopes.contains(envelope) {
                    return Err(format!(
                        "envelope {envelope:?} is not declared for {:?}",
                        requirement.edge
                    ));
                }
            }
        }
        Ok(())
    }

    /// Whether this envelope's required edges are about the policy the work
    /// will actually run under.
    ///
    /// Splitting the filesystem edges by mode makes confined and unconfined
    /// evidence different edges, but that only helps if something ties the
    /// requested edge to the mode the spawn actually applies. Without this,
    /// a perfectly current and satisfiable `Confined` requirement could be
    /// attached to a batch that runs with no policy at all: every check would
    /// pass, because each one would be asking about the confined edge while the
    /// process ran unconfined. The evidence would be true and the conclusion
    /// false.
    ///
    /// Edges whose mode is `NotApplicable` are unaffected, since network,
    /// relay, and control edges do not run under a filesystem policy.
    pub fn matches_policy_mode(&self, actual: PolicyMode) -> Result<(), String> {
        for requirement in &self.edges {
            let required = requirement.edge.definition().key.policy_mode;
            if required != PolicyMode::NotApplicable && required != actual {
                return Err(format!(
                    "{:?} is evidence about {required:?} work, but this batch runs {actual:?}",
                    requirement.edge
                ));
            }
        }
        Ok(())
    }

    pub fn describe(&self) -> String {
        format!(
            "{:?} containment envelope across {} edge(s)",
            self.process_class,
            self.edges.len()
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", tag = "kind", content = "config")]
pub enum ResidencyRuntimeConfig {
    Install(ExecutorConfig),
    NotRequired,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ContentionEstimate {
    pub co_resident_compile_jobs: u32,
    pub co_resident_light_jobs: u32,
    /// Fixed-point multiplier where 1000 means no slowdown.
    pub multiplier_millis: u32,
    pub source: ContentionEvidence,
    pub sample_count: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback: Option<ContentionFallback>,
}

impl ContentionEstimate {
    pub fn adjusted_ms(&self, base_ms: u64) -> u64 {
        base_ms
            .saturating_mul(self.multiplier_millis as u64)
            .saturating_add(999)
            / 1_000
    }

    pub fn describe(&self) -> String {
        format!(
            "{} compile-class and {} light co-resident job(s), {:.2}x slowdown from {}",
            self.co_resident_compile_jobs,
            self.co_resident_light_jobs,
            self.multiplier_millis as f64 / 1_000.0,
            self.source.describe()
        )
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ContentionEvidence {
    Machine,
    Global,
    Prior,
}

impl ContentionEvidence {
    pub fn describe(self) -> &'static str {
        match self {
            Self::Machine => "this machine's curve",
            Self::Global => "the fleet curve",
            Self::Prior => "the labeled conservative prior",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ContentionFallback {
    NoMachineCurve,
    NoGlobalCurve,
    ProfileLookupFailed,
}

/// Co-resident work and measured CPU and storage pressure at the instant an
/// execution began.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionLoadContext {
    pub co_resident_compile_jobs: u32,
    pub co_resident_light_jobs: u32,
    /// CPU utilization in thousandths, preserving an integer wire shape.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_utilization_millis: Option<u32>,
    /// [`IoPressure::full_avg10_millis`], or `None` where it was not measured,
    /// which is never the same as zero.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub io_pressure_full_millis: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PlacementPolicyEvidence {
    pub profile_name: String,
    pub work_class: PlacementWorkClass,
    pub stance: String,
    pub max_preference_delay_seconds: u64,
    /// True when policy selected a different machine from the earliest forecast.
    pub changed_earliest_winner: bool,
    /// True when a hard mobility boundary left policy no machine choice.
    pub constrained_by_mobility: bool,
}

/// The operator-facing class used to choose placement policy for a unit of work.
///
/// This is deliberately separate from [`CellCommandClass`], which describes the
/// command for duration learning. Placement classes describe why work is being
/// run and therefore remain stable when its command changes.
#[derive(
    Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash,
)]
#[serde(rename_all = "camelCase")]
pub enum PlacementWorkClass {
    ReviewChecks,
    WriteChecks,
    /// The conservative protocol default. A request from an older peer must not
    /// become newly spillable merely because it predates placement profiles.
    #[default]
    AgentSessions,
    DevInstances,
    Services,
}

/// Cadence and freshness bound for measured CPU admission decisions.
pub const CPU_ADMISSION_SAMPLE_INTERVAL_MS: u64 = 5_000;

/// How long an executor that has measured its own CPU may go without a new
/// reading before admission treats it as blind.
///
/// Three sample intervals, the same allowance the runner gives a link before
/// calling it stale. The bound has to tolerate ordinary jitter — a sampler that
/// misses one tick on a busy machine is busy, not broken — while still being
/// far shorter than the minutes a genuinely starved sampler goes quiet for.
pub const CPU_ADMISSION_BLIND_AFTER_MS: u64 = 3 * CPU_ADMISSION_SAMPLE_INTERVAL_MS;

/// How old an executor's machine reading may be before a reader must treat it
/// as history rather than as the machine's current state.
///
/// Memory, process, and CPU readings come from one sampler on one cadence, so
/// they go stale together and share the CPU bound. Past it, a reading says
/// more about when the sampler stopped than about the machine. That matters
/// most at the end of a generation: a sampler starves under exactly the
/// pressure that kills the process, so the last size it recorded is the one
/// most likely to be far below the truth (CAIRN-4627).
pub const MACHINE_READING_STALE_AFTER_MS: u64 = CPU_ADMISSION_BLIND_AFTER_MS;

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum CpuAdmissionState {
    /// No utilization reading has ever been taken on this machine — the first
    /// sample is still establishing its baseline, or this platform has no CPU
    /// probe at all. Reachable only before the first successful sample: nothing
    /// afterwards returns here, because a machine cannot stop having measured.
    #[default]
    Unknown,
    Accepting,
    Pressured,
}

// There is deliberately no `Blind` state here, and the omission is the design.
//
// Whether a machine has gone blind is a question about the age of its last
// reading, and only the sampler could set a state saying so — but the failure
// being described is the sampler not running. A transported `Blind` would
// therefore be set by a party that cannot observe the condition and trusted by
// parties that can: every consumer already holds `measured_at_unix_ms` and a
// clock, which is the whole answer. `measured_at_unix_ms.is_none()` means never
// measured; an age past `CPU_ADMISSION_BLIND_AFTER_MS` means blind; otherwise
// the state below is the verdict. One definition, derived wherever it is read.

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CpuAdmissionHealth {
    pub state: CpuAdmissionState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub utilization: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_since_unix_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub measured_at_unix_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ResidentProcessCwdRoot {
    #[default]
    Checkout,
    ResidencyScratch,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ResidentProcessStream {
    Pty,
    Stdout,
    Stderr,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ResidentSandboxPolicy {
    pub worktree: String,
    #[serde(default)]
    pub writable_extra: Vec<String>,
    #[serde(default)]
    pub deny_read: Vec<String>,
    #[serde(default)]
    pub writable_regex: Vec<String>,
    pub worktree_writable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CatalogPackDescriptor {
    pub catalog_id: String,
    pub content_hash: String,
    pub byte_count: u64,
    pub pack_checksum: String,
    pub base_commit: Option<String>,
    pub tip_commit: String,
    pub grant: CloudObjectGrant,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CatalogFetchResponse {
    pub packs: Vec<CatalogPackDescriptor>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MutationDeltaUploadRequest {
    pub coordinate: ObjectTransferCoordinate,
    pub base_commit: String,
    pub delta_commit: String,
    pub content_hash: String,
    pub byte_count: u64,
    pub pack_checksum: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum CellExecutionStage {
    #[serde(rename = "materializing")]
    CheckingOut,
    PreparingSetup,
    Running,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum CellCommandClass {
    CargoCheck,
    CargoTest,
    CargoClippy,
    Vitest,
    Typecheck,
    Build,
    #[default]
    Other,
}

impl CellCommandClass {
    pub fn is_compile_class(self) -> bool {
        matches!(
            self,
            Self::CargoCheck | Self::CargoTest | Self::CargoClippy | Self::Build
        )
    }

    pub fn classify(command: &str) -> Self {
        let command = command.to_ascii_lowercase();
        if command.contains("cargo clippy") || command.contains("check:rust") {
            Self::CargoClippy
        } else if command.contains("cargo test") || command.contains("test:rust") {
            Self::CargoTest
        } else if command.contains("cargo check") {
            Self::CargoCheck
        } else if command.contains("vitest") || command.contains("test:frontend") {
            Self::Vitest
        } else if command.contains("tsc ") || command.contains("typecheck") {
            Self::Typecheck
        } else if command.contains("vite build") || command.contains("bun run build") {
            Self::Build
        } else {
            Self::Other
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::CargoCheck => "cargoCheck",
            Self::CargoTest => "cargoTest",
            Self::CargoClippy => "cargoClippy",
            Self::Vitest => "vitest",
            Self::Typecheck => "typecheck",
            Self::Build => "build",
            Self::Other => "other",
        }
    }
}

/// How much of the work a command would otherwise redo was already on the
/// machine when it started.
///
/// This is a fact the executor states about an execution that happened, not a
/// score and not a discount. It exists so that warm and cold runs of the same
/// command on the same machine are learned as two profiles rather than averaged
/// into one number that describes neither: a Rust compilation front against a
/// populated target directory and the same front against an empty one are
/// different work, and a prediction that cannot tell them apart is a prediction
/// about nothing.
///
/// Nothing here is a percentage applied to a duration. Warmth reaches a
/// prediction only by selecting which observations speak for it.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub enum ExecutionWarmth {
    /// A retained cell for this project and repository had already completed
    /// this work class, so whatever that class writes into the tree — a target
    /// directory, a build cache, installed modules — was on disk before this
    /// command began.
    PreparedWarmSlot,
    /// The repository objects for the requested commit were already verified
    /// present, but no retained cell had completed this work class. Object
    /// warmth is not compile warmth, and conflating them is how a machine that
    /// merely holds the commit came to look as fast as one that has built it.
    RepositoryOnly,
    /// Neither claim holds.
    #[default]
    Cold,
}

impl ExecutionWarmth {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PreparedWarmSlot => "preparedWarmSlot",
            Self::RepositoryOnly => "repositoryOnly",
            Self::Cold => "cold",
        }
    }

    pub fn describe(self) -> &'static str {
        match self {
            Self::PreparedWarmSlot => "a retained cell had already run this work class",
            Self::RepositoryOnly => "the repository objects were present, no prepared cell was",
            Self::Cold => "neither the objects nor a prepared cell were present",
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CellOwnerRef {
    pub project_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub issue_number: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_seq: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_kind: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LearnedResourceEstimate {
    pub sample_count: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upper_duration_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upper_peak_rss_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upper_disk_growth_bytes: Option<u64>,
}

/// Bumped to 59 for CAIRN-4217: `DiskHealth` gained `reclamation` and
/// `accountingCadence`. Both are additive and defaulted, so an older peer's
/// payload still decodes — as `None`, which reads as "this peer does not report
/// it" rather than as a clean bill of health. The number moves anyway because
/// the executor's behavior did: it now persists permanently obstructed
/// reclamations across restarts and schedules its categorized walk from that
/// walk's own cost, and a runner should roll the artifact that does so.
///
/// Bumped to 25 for CAIRN-3306: machine telemetry became a first-class,
/// individually timestamped section on `ExecutorSubstrateReport`. The
/// overlapping un-timestamped scalars are gone rather than deprecated —
/// `HostHealth` lost `availableMemoryBytes`, `processRssBytes`,
/// `processPhysicalFootprintBytes`, and `cpuLoadOne`, and `DiskHealth` lost
/// `totalBytes`, `freeBytes`, `usedBytes`, `categories`, and the two
/// `accounting*` fields, all of which now live under `machine` as
/// [`Measurement`]s. A build that still spells them the old way would read every
/// machine number as absent while claiming to be a peer, so the number moves.
///
/// Bumped to 24 for CAIRN-3268: a queued request now carries the REQUESTER'S
/// wait horizon rather than a per-attempt queue budget. `CellRequest` and
/// `ResidencyAcquireRequest` spell that field `waitHorizonUnixMs` instead of
/// `deadlineUnixMs`, both carry `waitingSinceUnixMs` so seniority survives a
/// re-presentation, and `ExecutorMessage::WaitingRequests` is the runner→executor
/// frame that keeps a long horizon from leaking a queue slot. The renames change
/// the wire (these structs are `rename_all = "camelCase"`), so a build that
/// spells the field the old way must not also claim 23: it would decode a
/// horizon as absent, read it as zero, and evict every queued request the
/// instant it arrived.
///
/// Bumped to 23 for CAIRN-3258: `ResourceReservationSource::ZeroKnowledgePrior`
/// is now `Unmeasured`, which changes the string that variant serializes to.
/// `ResourceReservation` travels in both directions — inside `CellRequest` on
/// the way out and inside every `FleetSnapshot` on the way back — so two builds
/// that disagree about the spelling must not both claim 22 and then fail to
/// decode each other's reservations.
///
/// Bumped to 26 for CAIRN-3330: `SkippedEntryReason` lost its `vanished`
/// variant, and `DiskAccounting` gained `vanishedEntries`. An entry that stops
/// existing mid-scan is no longer reported as a skip at all, so an executor that
/// still sends `"vanished"` is describing a shape this build cannot decode. A
/// protocol number pins a wire shape, so two builds that disagree about it must
/// not both claim 25 and then silently mis-decode each other. What is
/// deliberately left in snake_case, and why, is in the note below.
///
/// Bumped to 27 for CAIRN-3327: `CellRequest.constraints` became
/// `CellRequest.executor`, carrying an [`ExecutorSelector`] that addresses a
/// machine by public name or platform instead of by opaque identity, and the
/// runner's home pin moved to its own `pinnedExecutorId`. The old key is not
/// accepted as an alias: a build that still sends `constraints` would have its
/// placement silently dropped and run wherever the fleet felt like, which is
/// exactly the failure a version number exists to make loud.
///
/// Bumped to 28 for CAIRN-3323: `CellRequest` carries `placementMobility`, the
/// fact that states whether policy may choose among machines for this request.
/// It is a new key with a conservative default, so an older peer decodes it as
/// [`PlacementMobility::PinnedOrColocated`] and nothing moves — but a build that
/// omits the field while claiming to be a peer of one that reads it would have
/// its work silently treated as immobile in one direction and mobile in the
/// other, which is the disagreement a version number exists to prevent.
///
/// Bumped to 31 for CAIRN-3385: enrolled executors can relay MCP callback
/// requests over their authenticated protocol connection. The request and typed
/// response frames are not understood by older peers, so this is a hard wire
/// compatibility boundary.
///
/// Bumped to 29 for CAIRN-3352: [`ResidencyOperation::MaterializeConflict`] and
/// its [`ResidencyResult::ConflictMaterialized`] reply. An executor that does not
/// know the operation would decode it as an unknown variant and fail the whole
/// message, and — worse — a runner that could not decode the reply would have no
/// way to distinguish "markers were written" from "nothing happened", which is
/// precisely the claim the wake is forbidden to make without confirmation.
///
/// Bumped to 32 for CAIRN-3430: [`CellUnavailableReason::SlotUnhealthy`], the
/// reason an executor gives when it retired the slot that could not take a
/// batch. A runner that does not know the variant fails to decode the whole
/// [`CellOutcome`], turning a batch that should be placed again into a decode
/// error — and the two builds disagree in the way that matters most, since the
/// entire point of the variant is that it is waited on rather than refused.
///
/// Bumped to 33 for CAIRN-3435: [`ResidentProcessKind::Service`] names the
/// subsystem that placed a process in words, under `name`, where it previously
/// carried the lease id under `service`. [`ResidencyOperation::StartProcess`]
/// carries this enum, so a v32 executor requires the retired key and fails to
/// decode the operation outright — a hard wire boundary, and exactly the
/// disagreement the handshake exists to refuse before any work is placed.
///
/// The same rename is *tolerated* rather than refused on disk, and the
/// asymmetry is deliberate: a live peer can renegotiate at the handshake and a
/// persisted cell cannot, so state written by an older executor decodes with
/// absent words rather than being skipped and orphaning its processes.
///
/// Bumped to 34 for CAIRN-3462: check batch items declare the environment
/// variables that affect their verdict and outcomes carry the executor-composed
/// fingerprint. Older peers cannot preserve safe cache reuse across this wire.
///
/// Bumped to 35 for CAIRN-3412: placement ranks on predicted time to a verdict
/// rather than on idleness, and the facts that prediction is built from are new
/// wire shape in both directions. `PersistentCellState` carries
/// `warmCommandClasses` and `CellExecutionMeta` carries `warmth`, so an executor
/// that does not report them leaves every candidate looking cold and every
/// observation unattributable. `PlacementReason::measuredIdle` is gone rather
/// than aliased because the two policies do not coexist.
///
/// Bumped to 36 for CAIRN-3514: measured CPU admission adds policy in the
/// runner-to-executor direction and publishes the executor's hysteresis verdict
/// and numeric pressure evidence in the other direction.
/// Bumped to 37 for CAIRN-3610: completed executions carry start-time load context.
///
/// Bumped to 38 for CAIRN-3626: process batches can carry files that the executor
/// materializes inside the selected cell before spawning commands.
///
/// Bumped to 39 for CAIRN-3635: runtime policy and inventory health carry a
/// machine-wide idle-cell budget. Older executors enforce only per-project
/// limits, which cannot bound a machine serving many projects.
///
/// Bumped to 40 for CAIRN-3649: categorized storage includes Cairn-managed
/// caches outside the executor home.
///
/// Bumped to 43 for CAIRN-3705 because the conservative protocol guard covers
/// check execution internals even though failed-verdict reuse and manual retry do
/// not alter executor wire semantics.
///
/// Bumped to 44 for CAIRN-3706: check request and process-item shaping moved into
/// the guarded physical wire boundary. The move preserves payload semantics but
/// deliberately requires one executor artifact rollout under the fail-closed policy.
///
/// Bumped to 45 for CAIRN-3738: capacity held by a resident process is now named
/// as held. An executor that reported no command-occupied cell used to leave the
/// generic evidence installed when the request joined the queue — "queued for
/// admission evaluation", naming no holder and no position — so a request queued
/// behind a dev instance waited without being told what held it.
/// `ExecutorSubstrateEvidence` now fills that silence with the active-cell count
/// and the request's ranked admission position.
///
/// Bumped to 46 for CAIRN-3824: an executor now scrubs registered credentials
/// out of the output it emits, so the bytes on the wire are no longer verbatim
/// child output. The per-batch relay capability it injects as `CAIRN_MCP_SECRET`
/// is registered for the life of the batch, which means an executor at this
/// version cannot hand a batch's own capability back through that batch's
/// stdout. An older executor emits the value unredacted; the runner scrubs again
/// on ingestion, so the pairing is safe in both directions, and the bump exists
/// because the wire content itself changed, not because a field did.
///
/// Bumped to 47 for CAIRN-3934: runtime configuration is now part of each cell
/// submission and residency request, so an executor installs it atomically
/// before dispatching work after a restart or connection-generation change.
///
/// Bumped to 48 for CAIRN-3954: cell-root migration decides whether two roots
/// are two roots by asking the filesystem rather than by comparing path text,
/// so a case-insensitive filesystem no longer reports the destination back to
/// itself as a competing inventory. No wire field moved; the bump is how the
/// fix reaches the machines it is for, since an executor at 47 on macOS or
/// Windows cannot provision a formerly-uppercase project at all.
///
/// Bumped to 49 for CAIRN-4065: a batch and a resident process now carry the
/// Cairn-owned JavaScript packages they need, and the executor installs them
/// above the checkout. The fields are additive and defaulted, so an executor at
/// 48 would decode the message and simply not install anything -- and the only
/// symptom would be bun reporting `@cairn/sdk` as a missing project dependency,
/// which is precisely the misleading failure this change exists to remove.
/// Refusing that peer at the handshake states the incompatibility where it can
/// be read, rather than deferring it to an import error blamed on the project.
///
/// Bumped to 50 for CAIRN-4081: runtime concurrency policy is now a downward
/// override of the executor's finite, role-aware advertised capacity. Older
/// executors subtract the colocated reserve again and can interpret the legacy
/// unlimited value as billions of units, so mixed peers must not silently agree
/// on the same policy response with incompatible admission semantics.
///
/// Bumped to 56 for the combined wire boundaries below.
///
/// CAIRN-4166 adds the dependency-catalog execution contract. CAIRN-4162
/// materializes a build cell only to a commit the current operation pinned and
/// repairs an adopted cell that cannot be reconciled instead of latching its slot.
///
/// Placement decisions carry deterministic tie-break evidence, suspended process
/// items and unavailable callback context use explicit backend control flow, and
/// ordinary command batches persist distinct physical PID/start-token ownership.
///
/// Bumped to 57 for attributable executor-memory telemetry: process telemetry
/// now carries [`ExecutorRetainedState`], naming which of the daemon's own
/// in-memory owners holds its resident set, and the daemon bounds the callback
/// queue and per-attempt output capture that could previously grow without
/// limit behind a slow runner.
///
/// Bumped to 58 for linux/aarch64 remote executors: [`RemotePlatform`] gained a
/// `LinuxArm64` variant, so the runner now builds install, launch, cleanup, and
/// inspection scripts for a host whose artifact target is
/// `aarch64-unknown-linux-gnu`. The distribution manifest a runner at 57 reads
/// carries no such artifact, so an ARM host it enrolled would be sent an x86
/// binary; moving the version is what keeps the manifest a peer reads and the
/// platforms it can install onto describing one release.
///
/// Bumped to 61 for remote connection timelines on executor inspections and
/// enrolled remotes. A peer at 60 never reports the transition history used to
/// diagnose attach and link failures, so the handshake must not present the two
/// wire contracts as equivalent.
///
/// Bumped to 63 because an execution environment is now a lease against a live
/// executor session rather than a permanent binding. A runner at 63 rebuilds an
/// environment whose executor is gone instead of refusing the owner's work
/// forever, and it sends a returning executor a [`ResidencyOperation::Release`]
/// for a residency that moved while it was away. An executor talking to a runner
/// at 62 would keep that superseded cell, leaving two machines answering for one
/// owner, so the two behaviors must not be presented as one contract.
///
/// Bumped to 64 for a binary-safe Windows CLI transfer. A runner at 63 streamed
/// the CLI's bytes into a PowerShell `-EncodedCommand` script over the SSH stdin
/// channel, which hosts a child PowerShell with serialized text streams and does
/// not carry a binary payload intact; the CLI now arrives by `scp` and is
/// verified where it lands, exactly as the executor beside it already was. The
/// two runners therefore install the same file by different means, and the
/// published artifact set a peer resolves is keyed by this number.
///
/// Also at 64, [`RemoteAttachAttempt`] carries [`RemoteArtifactFault`], so a
/// target with nothing published reads as unpublished rather than as a bare
/// mismatch, and is paced accordingly.
///
/// Bumped to 65 because reclaiming a build cell's target is a disk-pressure tier
/// rather than something a hold release does. An executor at 64 trims the slot
/// it just released against a fixed ceiling and ignores
/// [`ExecutorRuntimePolicy::slot_target_max_bytes`] entirely, so an operator who
/// sets that ceiling on a fleet at 64 changes nothing while the machine keeps
/// deleting the build it just finished. The behavior is what has to roll, not
/// only the field: the two must not be presented as one contract.
///
/// Bumped to 66 because adopting a retained cell now degrades to eviction. An
/// executor at 65 died on the first retained cell it could not validate, so it
/// reported nothing at all; one at 66 reaches Ready without that cell and names
/// it in [`ExecutorSubstrateReport::evicted_cells`]. A runner reading the older
/// report cannot tell an environment that was released from one that never
/// existed.
///
/// Bumped to 67 because a stale CPU reading now brakes admission instead of
/// releasing it. An executor at 66 wipes [`CpuAdmissionHealth`] to its default
/// on any missed sample and on any policy write, so a machine that had been
/// measuring for hours reports `measuredAtUnixMs: null` — indistinguishable
/// from a platform with no CPU probe, and admitted against as if idle. One at
/// 67 keeps the last reading and its collection time through both, which is
/// what lets any reader derive blindness from the reading's age and hold
/// non-interactive admission with [`HostPressureCondition::TelemetryBlind`].
/// A runner reading the older report cannot recover that distinction, because
/// the older executor no longer holds the evidence.
///
/// Telemetry-stall remediation deliberately did NOT bump this. The runner now
/// recycles a link that beats on time while the facts its beats carry stop
/// being recomputed, but every part of that is runner-owned: the bound, the
/// decision, the pacing, and the [`RemoteConnectionPhase`] it records. The
/// evidence it decides on is `liveness_observed_at_unix_ms`, which a v67
/// executor already publishes. Nothing an executor sends or accepts changed, so
/// a v67 executor and a runner carrying that remediation are fully compatible.
///
/// Bumping it anyway would have been actively harmful rather than merely
/// conservative. The version is compared for exact equality when a connection
/// is accepted, so a bump refuses every deployed executor until a matching
/// artifact has rolled to every machine — and the fleet state this remediation
/// exists to repair is precisely when that dependency is least affordable.
///
/// Runner-held draining did not bump it either, for the same reason and with
/// the same care. A drain became durable runner state that outlives the
/// connection it was set on, and the runner now states it on every attach and
/// refuses placement on a drained machine itself — but `DrainModeRequest` is a
/// message a v67 executor already accepts, its handling is unchanged, and
/// everything else that moved (a machine's drain on the fleet projection, the
/// placement rejection that names it) is runner-local and never crosses to an
/// executor. Refusing every deployed executor until an artifact rolls, in
/// order to ship the lever that disables a machine nobody can reach, would
/// withhold the fix exactly where it is needed.
///
/// The survivor precondition on a cell tree deliberately did NOT bump this
/// either. [`CellOccupancy::draining`] and
/// [`CommandProcessHealth::survivor_blocked_count`] are both `#[serde(default)]`
/// on structs that accept unknown keys, so nothing an executor sends or accepts
/// changed: an older peer ignores them, and this one defaults them from an
/// older peer's payload. Which cells this executor is willing to hand out did
/// change, and it says so in the vocabulary every peer already understands —
/// [`CellUnavailableReason::Provisioning`].
///
/// The one place they are more than additive is a cell's own state file, which
/// an executor lacking them drops on rewrite. That is a persisted-format
/// concern, which this codebase answers with tolerant readers rather than with
/// this number, and the hazard needs one machine's binary rolled backwards onto
/// state a newer one wrote — far less costly than refusing every deployed
/// executor for it.
///
/// The build-prerequisite verdict did not bump it, and the test is the same
/// one: what does a runner get wrong if the executor it is talking to is
/// older? Nothing. [`ExecutorCapabilities::build_prerequisites`] is additive
/// and defaulted, an older executor omits it, and the runner renders that
/// omission as the honest "not reported by this executor" rather than as a
/// machine that failed — the same shape [`ExecutorCapabilities::toolchain_detection`]
/// already has. Nothing an executor sends or accepts changed, and no decision
/// anywhere reads the new field. Bumping would refuse every attached machine
/// until an artifact rolled, in exchange for a fact that arrives on its own as
/// each machine's binary is next replaced.
///
/// Bumped to 68 because the available-memory floor now holds the claims of
/// resident work whose demand has never been measured, and
/// [`HostPressureCondition::MemoryAvailable`] carries that hold as its own
/// number. An executor at 67 admits every cold arrival against a floor of
/// two gigabytes, which is how eight unprofiled workspace builds walked into a
/// 121 GiB machine and crushed it (CAIRN-4365). A runner reading the older
/// report sees a floor with no explanation for its size.
///
/// The blocked-refresh marker did not bump it either, and the reasoning is the
/// one above about `build_prerequisites` rather than a new argument.
/// [`ExecutorAdvertisement::facts_stale_refresh_blocked`] is additive and
/// `#[serde(default)]`: an older executor omits it and the runner reads the
/// omission as the absence of an explanation, which is precisely how every
/// runner behaved before the field existed. Nothing an executor accepts
/// changed, and no older peer is misled by a newer one — which is what lets the
/// runner judge an unexplained freeze exactly as it always did, rather than
/// needing to know its peer's vintage (CAIRN-4420).
///
/// That the number below moved anyway does not retire the argument, and it is
/// worth being exact about what it costs. The marker exists so a wedged
/// liveness refresh is answered by a report rather than by bouncing the link,
/// and a bounced machine can only return through an artifact matching the
/// runner's version — so this remedy is worth the least when it is gated behind
/// a roll. It is not gated behind one it caused: the release carries a bump of
/// its own for the change recorded below, and this field rides in on it for
/// free. Had it been the only change here, the number would not have moved.
///
/// Bumped to 70 because macOS teardown now treats a failed process-group signal
/// as verified completion when a census finds no token-matched owned identity
/// (CAIRN-4437). This changes whether the executor admits the next batch after a
/// numeric process-group identifier is reused, so deployed executors and their
/// runner-visible placement behavior must move together.
///
/// Bumped to 71 because what losing the link means changed on both halves at
/// once. An executor before this treats a failed runner read as the end of the
/// process and takes its running cells down with it within a millisecond; a 71
/// executor keeps them running and dials back within
/// [`EXECUTOR_RECONNECT_MAX_DELAY_MS`]. The runner side is sized from exactly
/// that: a link gap is long enough for the machine to remain working and dial
/// back on its own, because the machine behind it is expected to
/// be working and coming back. Pair a 71 runner with an older executor and both
/// halves are wrong in the same window — the runner waits a minute and a half
/// for a process that died on the read, holding a machine out of placement
/// while the work it is waiting for no longer exists. Unlike an additive field,
/// this is not something an older peer can ignore: it is a claim about what its
/// silence means.
///
/// Version 72 makes [`ProcessBatch::execution_residency`] authoritative at the
/// executor's drain gate too. A 72 runner deliberately admits a batch pinned to
/// a drained resident environment; an older executor rejects the same batch
/// downstream, contradicting the runner's placement verdict and preventing the
/// drain from ever emptying. The field shape did not change, but its admission
/// meaning did, so mixed peers are not compatible.
///
/// Version 74 makes conflict-scaffolding rejection a pre-admission executor
/// behavior. An older executor can park the same permanently unrunnable commit
/// until its progress window expires and report capacity-shaped silence, while a
/// 74 executor returns the causal checkout refusal before queueing it.
///
/// Version 75 adds the runner-visible permanent-refusal substrate state. A
/// runner that understands this state stops presenting a protocol-incompatible
/// local executor as though it were actively respawning.
///
/// Version 76 makes a residency's command batches participate in the same
/// operation fence as checkout refresh and release. An older executor can move
/// the checkout under a batch after a newer runner has accepted the refresh, so
/// the behavioral contract is not safe across mixed peers even though no field
/// changed shape.
///
/// Version 77 reruns configured setup before every new command occupancy. An
/// older executor treats matching tracked inputs as proof that ignored setup
/// outputs still exist and can therefore run a JavaScript suite without its
/// dependencies. The behavior must roll to every executor even though the wire
/// shape is unchanged.
///
/// Version 79 adds request-scoped in-flight residency operation evidence to
/// fleet snapshots. Runners use it to distinguish a slow checkout from a silent
/// executor after acquisition leaves the admission queue.
///
/// Version 78 makes the default shell and its command flag spawn-side intents.
/// Older executors try to execute the program intent as a literal path, while
/// older runners send their own platform's shell path and invocation syntax.
///
/// Version 81 makes dependency-catalog discovery follow the Cargo workspace
/// and target directory resolved from the command instead of assuming both are
/// at repository root. Mixed versions otherwise disagree about whether and
/// where a detached command may materialize reusable dependency artifacts.
///
/// Version 82 carries stored-procedure positional arguments after a native
/// shell item's command. Older executors require exactly one argument and would
/// reject an otherwise valid procedure batch at launch.
///
/// Version 83 confines detached resident-process cleanup to executor-owned
/// roots. Older executors can treat an operator process whose current directory
/// is an externally owned checkout as residency residue and signal its process
/// group during release, so mixed versions do not preserve the ownership
/// boundary even though the wire shape is unchanged.
///
/// Version 90 adds the closed containment edge vocabulary, observations, workload
/// envelopes, and selector, advertisement, and execution carriage. Mixed peers
/// otherwise disagree about a placement security requirement.
///
/// Version 87 returns a quarantined cell to service once its prior-epoch
/// survivor is proven gone, clearing the drain and the `Quarantined` lifecycle
/// as one persisted transition. Older executors clear only the drain, so the
/// cell is never selected again: a version 86 machine permanently loses one
/// build cell per escaped process, and mixed versions therefore disagree about
/// how much of a machine's advertised pool is reachable at all.
///
/// Version 86 makes a prior-epoch survivor a quarantined, retryable slot-local
/// refusal. Older executors report the same physical condition as structural
/// provisioning failure, so mixed versions do not preserve pool availability.
///
/// Version 84 makes dependency-catalog generations convergent across Cargo
/// command families. Mixed executor versions otherwise retain incompatible
/// cache layouts on the same machine-local executor home.
///
/// Version 80 makes one executor process generation authoritative before it
/// attaches. An older executor can otherwise survive a runner rebuild and
/// reconnect while the replacement also advertises the same machine, leaving
/// cell-level authority contention to surface only when a command provisions.
///
/// The infrastructure-avoidance placement input did not move it, and the test
/// is the one applied to `build_prerequisites` above: what does a runner get
/// wrong if the executor it is talking to is older? Nothing.
/// [`CellRequest::avoided_executor_ids`] is additive and `#[serde(default)]`,
/// an older executor ignores the key, and no executor-side decision reads it —
/// avoidance is resolved entirely on the runner, before dispatch, by the same
/// eligibility relation that already refuses a drained machine. Bumping would
/// refuse every attached machine until an artifact rolled, in exchange for a
/// field the executor has no use for. The version moved to 90 for containment,
/// which changes what placement will admit at all; that the two arrived
/// together is not a reason to charge one with the other's cost.
///
/// Version 88 adds [`ResidencyOperation::MaterializeAttachment`] and its
/// [`ResidencyResult::AttachmentMaterialized`] reply, so a durable composer
/// attachment can be copied into whichever cell the reading run actually holds.
/// An executor that does not know the operation decodes it as an unknown variant
/// and fails the whole message, and a runner that could not decode the reply
/// would have no way to distinguish "the bytes landed at this path" from
/// "nothing happened" — which is precisely the claim a materialize summary is
/// forbidden to make without confirmation.
///
/// Version 89 reports the allocator's split of the executor heap into live and
/// freed-but-retained bytes. An older executor reports the reading as absent,
/// so a fleet holding both cannot be read for whether a machine's growth is a
/// leak or allocator retention — the two want opposite fixes, and the whole
/// point of the reading is that no other field distinguishes them.
///
/// The manifest entries added for the headless runner install did not move the
/// version. `tools`, `resources`, `host_requirements`, and `source_commit` on
/// [`ExecutorDistributionManifest`] are additive and defaulted, and the manifest
/// is a release document, not a message: the runner and `scripts/` read it from
/// GitHub, and no executor ever parses one or receives one over the link. So the
/// test above has a plain answer — a runner talking to an older executor gets
/// nothing wrong, because the executor was never party to these fields. Bumping
/// would strand every attached machine until an artifact rolled, in exchange for
/// nothing a peer could disagree about.
///
/// Version 91 inverts the transport. The executor is now a resident daemon that
/// dials the runner's own TLS listener with a pinned certificate fingerprint,
/// asserts its presence by heartbeat, and reconnects on its own; the runner
/// stops holding an SSH carrier open per machine, and `Ready` carries a
/// [`ExecutorListenerAdvertisement`] naming the endpoints and fingerprints that
/// link may be re-established on. What an older executor gets wrong is not one
/// field but the direction of the relationship: it cannot be resident, it has no
/// pinned identity for the runner it answers, and it treats a link gap as
/// something the runner will repair. Presence on the runner side moved with it —
/// [`RemoteLinkState`] now distinguishes a machine that was bootstrapped and has
/// never dialed from one that dialed and went absent, which are the same state
/// only in an architecture where the runner is the one reaching in.
///
/// Version 92 makes a protocol mismatch self-repairing and listener rotation
/// durable. A mismatch now names the authenticated artifact manifest, update
/// outcomes travel on hello and heartbeat, and listener advertisements are
/// acknowledged after persistence. An older executor refuses at the mismatch
/// instead of installing the runner's distribution or accepting a new pin, so
/// mixed peers cannot provide either guarantee.
///
/// Version 93 removes the final runner-side SSH-carrier transition vocabulary.
/// Older peers disagree about the serialized connection-history shape, so the
/// version moves even though steady-state executor messages do not use it.
///
/// Widening the listener rotation cut-over gate to cover every connected
/// non-colocated machine, and not only declared enrolled remotes, did not move
/// it, and the test is the one applied to the infrastructure-avoidance input
/// above: what does a runner get wrong if the executor it is talking to is
/// older? Nothing. The gate is `Fleet::listener_rotation_gate_executor_ids`,
/// which no executor compiles; it decides only which acknowledgements the
/// runner waits for before it prunes an old pin. No message gains a field and
/// none changes shape — a machine of any version is sent the same overlap and
/// answers with the same acknowledgement. The change is strictly the
/// conservative direction: a machine that dialed in before its enrollment
/// declaration landed is now waited for rather than cut over behind, so an
/// older executor is helped by a runner that has this and loses nothing to one
/// that does not. The rotation contract itself already moved, at 92 and 93; a
/// runner-only correction to whom that contract waits for is not a second
/// incompatibility. The durable record backing it, `ListenerRotation`, gained a
/// field, but that file is the runner's own on-disk state and crosses no link;
/// it is `#[serde(default)]`, so a rotation staged before this change still
/// loads.
///
/// Version 94 acknowledges a credential issued in `Ready`. An older executor
/// persists that credential but never confirms receipt, leaving the predecessor
/// slot occupied; its next rotation therefore depends on the expired-previous
/// fallback instead of proof that delivery completed. The credential-recovery
/// connection phase itself remains runner-local history, and the containment
/// edit only documents the distinct link and artifact admission predicates.
///
/// Version 96 moves the moment work binds. The runner no longer sends `Submit`
/// and lets the machine park it; it offers one entry to the one machine it
/// chose, and the machine answers immediately from its own admission with a
/// claim or a truthful decline. What mixed peers disagree about is not a field
/// but whether binding happens at dispatch or at claim, so there is no
/// compatible subset of this. An older executor answers a `Submit` that no
/// longer exists and cannot claim anything, so every offer this runner makes to
/// it goes unanswered; a newer executor posts allocation, predicted relief, and
/// its slot inventory to an older runner that reads none of them, and then waits
/// for orders that runner will never issue because it is still parking work on a
/// queue this executor no longer keeps for commands. The gap at 95 is not a
/// missing protocol: this change first took 95, and the contended-identifier
/// resolver moved it to 96 during a base advance while 94 was landing. No peer
/// ever spoke 95.
///
/// Extracting the runner's one-attempt offer helper does not move the version:
/// an older executor receives the same `Offer` summary and answers the same
/// claim or decline. Selection, entry transitions, link-failure handling, and
/// grant dispatch retain their order; only the runner-local function boundary
/// changes. Neither peer interprets any field differently.
///
/// Version 97 makes retained slots a project-scoped warm pool: claim verifies and
/// switches the tree, while release leaves its contents alone. Persistent slot
/// lifecycle collapses to materializing, idle, active, deleting, and quarantined;
/// identity and state files migrate before typed decoding, while residency phases
/// are unchanged. SlotState adds a target-labelled last measured cache distance
/// for reporting, never as a placement ranking input. Mixed peers disagree on
/// lifecycle spellings (for example running versus active) and on whether an idle
/// tree has already been cleaned at release or still awaits claim-time verification;
/// ignoring an unfamiliar optional field cannot reconcile either disagreement.
///
/// Version 99 binds residency acquisition at a queue claim. An older executor
/// ignores `grantedEntryId` and admits again, charging a second time the
/// reservation the claim already charged and holding a queue slot for work the
/// runner has already bound. A newer executor refuses an older runner's
/// un-granted fresh acquisition by name because that runner never claimed.
/// Mixed peers disagree about whether the machine or the claim admits the
/// acquisition, so there is no compatible subset. This change first took 98,
/// and the contended-identifier resolver moved it to 99 during a base advance
/// while 97 was landing. No peer ever spoke 98.
///
/// Adding CPU time and allocation labels to execution metadata does not move
/// the version, by the same test: what does either peer get wrong if the other
/// is older? `CellExecutionMeta` and `ProcessBatchItemOutcome` gain only
/// optional fields, and neither denies unknown fields. An older runner ignores
/// the labels and records the observation as it did before; a newer runner
/// reading an older executor sees no labels, which is what it had. Nothing in
/// admission or placement consults them. No label claims a kernel-enforced cap,
/// so the change that writes one, and the offer fields that let a machine
/// refuse greedy work beside a compile, carries its own bump.
///
/// Version 100 makes the bootstrap ceremony depend on a subcommand the resident
/// binary must carry. Before `executor.json` is written, the runner runs
/// `cairn-exec reach` on the machine, which proves from the machine's own
/// vantage that at least one advertised endpoint answers with a pinned
/// certificate, and reports each endpoint as a [`ReachVerdict`]. A v99 binary
/// has no such subcommand, so a runner that installed one would fail that probe
/// on every machine; the ceremony therefore must never reuse or keep an older
/// binary, which is exactly what a version mismatch already guarantees. The
/// resident dial changed with it: an executor now races its endpoints instead of
/// trying them in turn, so the runner can advertise one fleet-wide candidate
/// list that some machines only partly resolve.
///
/// Version 112 makes `executor.json` carry the enrollment grant, and makes the
/// ceremony depend on `cairn-exec service status --json` and the exit record a
/// resident daemon keeps beside its config. The document is now one type,
/// [`ResidentExecutorConfig`], which refuses unknown keys. An older executor
/// refuses the `enrollmentGrant` key that every ceremony now writes, and exits
/// 78 on every start under its service manager, which is exactly the failure
/// this change removes; it also has no `--json` status for the ceremony to read
/// after a missed dial. There is no compatible subset, so the ceremony must
/// never keep or reuse an older binary. The link messages themselves are
/// unchanged. This change was assigned 112 behind 110 and 111 but merged ahead
/// of them, and neither number was ever used on main: the changes that held
/// them took 115 and 114, and 113 was never used either.
///
/// Version 114 gives the per-project provisioning hold an owner and a
/// lifetime. An earlier executor can leak a claimed admission on an early exit
/// before its lease and then refuse every new cell for that project until its
/// link drops, which on the colocated link is never; a v114 executor gives
/// every such admission back, releases a claim the runner's `WaitingRequests`
/// report has stopped naming, and audits for tickets nothing owns. Equivalent
/// machines on the two versions would report different usable capacity for
/// the same interaction, and rollout could not be fenced on one number. The
/// hold also gets its own decline reason, `DeclineReason::ProjectProvisioning`,
/// which an earlier peer cannot decode. `FleetSnapshot` gains
/// `provisioningHolds` and `orphanedProvisioningReleases`, both defaulted and
/// omitted when empty; those alone would not have moved the number.
///
/// Version 115 changes what an existing-checkout residency can do after one of
/// its terminals exits. An older executor treated the operator's live checkout
/// as an escaped cleanup root, retired the residency on the first stop or
/// release of an exited checkout-rooted process, and refused every later
/// terminal in it until restart. A newer executor keeps the residency usable,
/// and a retirement it does perform moves the residency to `Releasing` so the
/// next acquisition provisions a fresh one. The two differ in usable residency
/// capacity, so they may not share a number. This change was assigned 110 and
/// then 113, and took 115 after 112 and 114 merged ahead of it; no peer ever
/// spoke 110 or 113 with this meaning.
///
/// Version 116 makes an executor's handshake bearer the identity it holds now
/// rather than the one it started with. An earlier executor that enrolled with
/// a grant kept presenting that spent grant in its Authorization header after
/// the runner issued its credential, so its first reconnect, and every one
/// after, was refused until its daemon restarted, even though its Hello named
/// the credential correctly. No frame changes. The number moves because it is
/// the rollout fence: executors replace their binary only on a mismatch, and
/// the bootstrap ceremony keeps an installed binary that already matches, so
/// without it every installed executor, and every ceremony on a machine that
/// still holds one, would keep stranding itself on its first link drop.
///
/// Versions 101 to 111, 113, and 117 to 134 denote no protocol change of their
/// own. Only two of them ever sat on main, and both were published: 109, from
/// cairn/4259, is wire-identical to 100, and 134, from cairn/4761, is
/// wire-identical to 116. Neither branch changed the protocol. The
/// contended-identifier resolver minted both during base advances, by reading a
/// branch's fork point from a stale record, counting the base's own increments
/// as the branch's, and then holding that count as a floor. 110, 111 and 113
/// were held by changes that landed under other numbers (see 112 and 115), and
/// the rest never reached main. A version is now minted only by a paragraph
/// headed like the ones above, which the merge gate and
/// `check:executor-protocol` both enforce, so the next real change takes 135.
///
/// Version 135 changes what the storage sweep reclaims and how much headroom
/// it reports. An earlier executor measured every retired tree before acting
/// on it, and a tree its measurer could not finish (a full checkout exhausted
/// its descriptors) was dropped from the sweep silently: an expired quarantine
/// was never reclaimed and the pressure path trimmed warm build targets in its
/// place. A v135 executor deletes expired quarantines and discards without
/// measuring them, so equivalent machines report different usable disk for the
/// same interaction. [`DiskHealth`] also gains `cleanupSkipped` and
/// `cleanupSkippedTruncated`, both defaulted, [`SkippedEntryOperation`] gains
/// `measure` and `remove`, and [`SkippedEntryReason`] gains `other`, all of
/// which appear only in that list. An earlier runner cannot decode any of the
/// three and would reject the whole disk-health report that carries one, so
/// the variants need the fence as well.
///
/// Version 136 is a rollout trigger: executors replace their binary only on a
/// version mismatch. The wire is unchanged, and by the test above a runner
/// talking to a 134 executor gets nothing wrong. The executor now bounds its
/// own snapshot path (CAIRN-4627). It holds one pending snapshot instead of an
/// unbounded queue, publishes only when state changed, and sheds dead
/// descendants from command ownership. All of that is internal, and a snapshot
/// is level-triggered, so a runner that receives fewer of them reads the same
/// state. Stall judgement reads the connection's own progress stamp, which
/// every frame refreshes. `ExecutorRetainedState` gains `pendingSnapshot` and
/// `outgoingControlQueue`, both defaulted, so each peer decodes the other, and
/// `MACHINE_READING_STALE_AFTER_MS` is a shared constant, not a frame. The
/// number moves for the reason 116 did: the runner keeps an installed binary
/// that matches exactly, and an earlier executor leaks its link queue until the
/// kernel kills it. Without a mismatch no machine running a published earlier
/// version would ever take this fix, and a missing artifact for this version
/// is a loud fault rather than a silent skip. This change was assigned 135 and
/// took 136 behind cairn/4858, which merged first.
///
/// Honoring an explicit executor selector against a job's live residency
/// (cairn/4712) does not move the version, by the same test: what does a runner
/// get wrong if the executor it is talking to is older? Nothing. Its fleet half
/// is runner-only. The pin a job batch carries now resolves to the residency
/// route whose executor holds a live connection before falling back to any
/// route, the decision taken before acquisition reads the recorded route (and
/// an absent machine's enrollment record) where it used to guess the colocated
/// machine, and the placement refusal and ledger text name the machine holding
/// the job's working tree. [`CellRequest::pinned_executor_id`] is runner-side
/// placement state that no executor reads, and the strings are diagnostics the
/// runner composes. A batch that now detaches instead of pinning reaches the
/// executor as an unpinned batch with no residency fence, a shape every
/// executor already receives.
///
/// [`PlacementRejectionReason::Declined`] (cairn/4843) moves nothing. The enum
/// travels only in the runner's own placement records: `PlacementDecision`,
/// `ExecutorInspection`, and `QueuedCellRequest::placement`, which an executor
/// always sends empty and the runner fills after receipt for readable
/// surfaces. No executor build names the type, and nothing the runner sends an
/// executor carries it. By the test above, a runner talking to an older
/// executor gets nothing wrong: the executor declines exactly as before, and
/// the runner now waits for its next posted state instead of giving up.
///
/// Version 137 is a rollout trigger, for the reason 116 and 136 were. The wire
/// is unchanged. An earlier resident executor composed its PATH around the
/// default `~/.cairn` home rather than the home its config names, so the
/// `cairn` CLI the runner installs into `<executor home>/bin` was never on it,
/// and a cell resolved whatever `~/.cairn/bin/cairn` a person had installed, at
/// whatever build (`cairn-cmd 0.12.1`, with no `check`), or nothing at all
/// (cairn/4875). A v137 executor leads its PATH with its own home's `bin`, and
/// probes the CLI it resolves against its own build, advertising `cairn-cli`
/// only when the two agree. That name rides the existing `toolchains` list and
/// probe record, so a runner talking to a 136 executor decodes everything and
/// simply never sees `cairn-cli`, which places a batch that requires it nowhere
/// such an executor is, correctly. The number moves because the runner keeps an
/// installed binary that matches exactly: without a mismatch, no machine running
/// a published earlier executor would ever stop shadowing the CLI.
///
/// Version 138 makes storage pressure reclamation byte-driven and visible
/// (CAIRN-4860). [`DiskHealthStatus`] gains `tight` (free space at or below
/// the pressure line and above the admission watermark), and an earlier runner
/// cannot decode it, so it would reject every disk-health report from a tight
/// machine. [`ExecutorRuntimePolicy`] gains `storageCriticalFreeBytes`, which
/// is defaulted and which `validate` now orders between the watermark and the
/// pressure line. [`DiskHealth`] gains the three thresholds its verdict was
/// computed against, the last pressure pass (`pressure`), and the quarantine
/// build outputs stripped under pressure. [`DiskAccounting`] gains `otherTop`,
/// [`SlotState`] gains `measuredBytes`/`measuredAtUnixMs`,
/// [`CellInventoryHealth`] gains `heldCount`, and the `decline` frame and
/// [`BindingEvent::Declined`] gain a typed `disk` refusal that names the floor
/// and whether reclamation can still relieve it. All of those are defaulted,
/// so each peer decodes the other. The behaviour that needs the fence is the
/// executor's: an interactive request on an existing checkout is now refused
/// below the watermark, the verdict and admission agree at the watermark, and
/// the pressure tier strips in-window quarantine build output, so equivalent
/// machines on 137 and 138 admit and reclaim differently. This change was
/// assigned 137 and took 138 behind cairn/4875, which merged first.
///
/// Version 139 (cairn/4877) changes which quarantines the pressure tier may
/// strip. The occupancy probe now leaves out a process whose holdings the
/// kernel withholds by permission when its lineage container (a macOS resource
/// coalition, a Linux cgroup v2 path) is not one the executor has run in since
/// boot, where 138 made every such process — Terminal.app's `login`, a systemd
/// user session's `sd-pam` — poison every tree with "occupancy unknown". So
/// equivalent machines on 138 and 139 reclaim differently: on 138 the
/// quarantine-target rung is inert on any host with a session wrapper, on 139
/// it runs. The wire additions are defaulted: [`OccupiedTreeSkip`] gains the
/// [`OccupancyRule`] that fired and the process it names,
/// [`QuarantineTargetStrip`] gains the withheld processes the strip did not
/// consult, and [`DiskHealth`] gains the lineage the executor counts as its
/// own ([`OccupancyLineageReport`]).
///
/// cairn/4899 moves runner-side ingest off the transport workers without a
/// bump. The runner now decodes frames of 64 KiB or more, and applies every
/// `snapshotUpdated`/`snapshotResponse`, on the blocking pool. The pump awaits
/// each one before reading the next frame, so a connection's frames still land
/// in the order they were sent, and `set_executor_snapshot` runs exactly as
/// before. The fleet also frees a replaced snapshot after releasing its
/// connection table, and builds the telemetry frame's rows without cloning
/// snapshots, from the same facts as before. No frame, field, cadence, or
/// meaning changed in either direction. A runner talking to an older executor
/// receives the same frames and treats them the same way, so it gets nothing
/// wrong; only the thread doing the work and the length of a lock hold differ.
///
/// cairn/4898 changes what the runner learns from a completion without a bump.
/// The runner now reads a completion's exit code and timeout flag, or a batch
/// item's `succeeded`/`timedOut`/`suspended` and its `CAIRN_CHECK_RESULT_JSON=`
/// body line, to decide which of its own duration and contention profiles the
/// run may teach. Every one of those fields already crossed the wire and keeps
/// its meaning; nothing is sent to the executor that was not before. An older
/// executor that attaches no result marker only means an empty-selection run is
/// recorded as a successful one there, which is what every runner did before
/// this change, so a runner talking to it gets nothing new wrong.
///
/// Version 140 (cairn/4569) binds a colocated executor to the runner launch
/// that started it. [`ExecutorEnrollmentIdentity::Colocated`] now carries the
/// supervision nonce the runner handed that launch, and the runner answers a
/// colocated Hello whose nonce it did not issue with `Shutdown`, which the
/// executor obeys by stopping its resident work and exiting. Before this an
/// executor whose runner was killed reattached to the next runner on the same
/// port and token, kept the generation lock its successor needed, and served
/// as "generation 1" while the successor crash-looped behind it. A 139
/// executor left running that way presents no nonce, and the version refusal
/// it receives instead ends it the same way, which is why the field decodes
/// when absent. The executor also stops on SIGTERM and exits
/// [`EXECUTOR_GENERATION_HELD_EXIT_CODE`] when the generation is held.
///
/// cairn/4906 changes how the runner reads a snapshot, without a bump. A
/// [`PersistentCellState::cell_id`] has always been unique only within its
/// project on one executor, and the executor already scopes its materialization
/// read by `(project_id, cell_id)`. The runner now builds a
/// `MaterializationReadRequest` fence from the cell it selected instead of
/// re-finding that cell by ID alone, and recovers its connection table from a
/// panic instead of propagating the poison. The request carries the same fields
/// with the meaning they always had; it now names the right holder where it
/// could name another project's. An older executor answers that request
/// exactly as before, so a runner talking to it gets nothing wrong.
///
/// cairn/4929 changes the executor's Cargo dependency catalog without a bump.
/// Publication now replaces the producer's target path inside build-script
/// output with a placeholder that materialization resolves to the consumer's
/// target, under a new catalog key and manifest format. The catalog lives in
/// the executor's own home and is read and written only by that executor; no
/// frame, field, or telemetry value crossing the link changes. A runner
/// talking to an older executor gets nothing wrong: that executor still
/// publishes slot-bound generations under the old key, which this executor
/// never reads, and its storage sweep removes partitions in an older catalog
/// format ahead of any other managed cache. That reclamation is also local to
/// the executor's own disk; the cleanup report fields carrying it are unchanged.
///
/// Version 141 (cairn/4798) adds
/// [`CellUnavailableReason::PreparationCommandNotFound`]: a setup command that
/// failed because a host toolchain it runs is not on the executor's PATH now
/// comes back as its own reason, naming the program, instead of as text under
/// `Preparation`. An earlier runner cannot decode the variant,
/// so it would fail the whole outcome frame. The runner records the reason
/// against (executor, project) and stops placing that project there, which is
/// the decision an older executor's flattened `Preparation` cannot drive. The
/// other additions ride without a bump on their own: [`CellRequest`] gains
/// `projectRequirements` and [`PlacementRejectionReason`] gains three
/// runner-side reasons, all read only by the runner's placement and its
/// surfaces. The executor's toolchain probe and prerequisite script now read
/// their names and paths from `toolchain_requirements`, unchanged in value.
///
/// cairn/4925 changes what the runner reports about a grant that produced no
/// result, without a bump. When a granted request ends `Unavailable` or in a
/// storage failure, whether the runner composed that (watchdog, closed result
/// channel, machine declared gone) or the executor sent it, the runner now
/// keeps which executor held the grant, when it was sent, how many of that
/// machine's connections ended under the claim (read from the runner's own
/// queue chain), and the watchdog budget by its parts, and its watchdog
/// diagnostic names that budget and renders its instants on the host clock.
/// All of it is runner-side bookkeeping about frames that already crossed. No
/// frame, field, cadence, or meaning changed in either direction, so a runner
/// talking to an older executor gets nothing wrong.
///
/// cairn/4907 changes how the runner retires a colocated executor, without a
/// bump. The executor's on-disk cell-state decoder moved, unchanged, into
/// `executor_generation` so the runner reads `cairn-build-slot-state.json` the
/// way adoption does, and after the executor is gone the runner SIGKILLs the
/// workload those files record. The retirement summary's new workload fields
/// and the ledger (survivors and unresolved cells) live in
/// [`ColocatedSupervision`], which is runner-side
/// state rendered by `cairn://executors` and never sent to an executor. No
/// frame or field crossing the link changes. A runner talking to an older
/// executor gets nothing wrong: that executor persists the same cell state
/// (the reader decodes every older lifecycle the executor does), and a cell
/// recorded before resident ownership existed names nothing verifiable, so it
/// is never signalled.
///
/// cairn/4937 changes how long the executor waits on the runner, without a
/// bump. No frame, field, variant, or meaning changes; [`RunnerCallback`] gains
/// only an inherent `kind()` used for logging. The executor now takes each
/// callback's bound from `callback_contract`: a `SandboxDenied` round trip
/// waits the runner's relayed fence budget plus a margin instead of a flat
/// 30 s, and the in-cell MCP relay waits per verb above the host's own bound
/// instead of a flat 120 s. An older executor keeps its flat 30 s; the runner
/// now answers a relayed fence wait within 20 s, inside that bound, so even an
/// older executor receives the suspend marker, and withdraws a fence wait whose
/// batch context has closed rather than parking the turn. A newer executor
/// against an older runner waits past that runner's 45 s answer, so it
/// receives the same reply it always did.
///
/// cairn/4931 adds runner-side placement vocabulary without a bump.
/// [`MemoryCeiling`] and [`PlacementSelection::memory_ceiling`],
/// [`PlacementRejectionReason::ExceedsMemoryCeiling`],
/// [`CellUnavailableReason::HomeCannotHold`], and
/// [`RemoteConnectionPhase::EnvironmentEvicted`] are produced by the runner
/// alone and read only by its own surfaces: no executor build constructs,
/// decodes, or matches any of them, and nothing the runner sends an executor
/// carries them (a `CellOutcome` travels executor to runner only). The
/// behaviour behind them, which residency routes count as live, when a silent
/// home is evicted, how placement ranks a machine whose memory ceiling the
/// work exceeds, and when a pinned batch is refused for it, is decided
/// entirely on the runner from facts executors already send (progress stamps,
/// `memory_budget_bytes`, snapshots). The only message the change sends an
/// executor is the existing residency `Release`, which any version answers the
/// same way. A runner talking to an older executor therefore gets nothing
/// wrong, and two executors of different versions behave identically under it.
///
/// cairn/4977 settles running terminals whose process is gone without a bump.
/// The runner counts the snapshots each executor link delivers (runner state,
/// never sent) and confirms a suspected loss either from the executor's answer
/// to the existing fenced `Renew`, which every version computes from its own
/// state the same way (`State`, `StaleEpoch`, `NotFound`, `InvalidState`), or
/// from absence persisting across two snapshots received after the suspicion.
/// The second relies only on the executor holding a single outbound snapshot
/// that is superseded rather than queued, which every executor version that
/// speaks this protocol already does. Nothing new crosses the boundary, so a
/// runner talking to an older executor gets nothing wrong.
///
/// cairn/4981 changes how long the runner waits for a claimed attempt's
/// result, without a bump. The end-to-end watchdog, a fixed budget summed from
/// declared item timeouts and executor phase budgets, is replaced by a wait
/// that ends only when the claiming machine has given no account of the
/// attempt for a bound: its link down or silent, or its snapshots no longer
/// listing it in `grantedAttempts`, `executingRequests`, or `queuedRequests`.
/// [`CellUnavailableReason::ClaimedAttemptSilent`] and
/// [`ClaimedAttemptAccount`] are produced by the runner alone and read only by
/// its own surfaces; a `CellOutcome` travels executor to runner only. The
/// account is read from facts every 142 executor already sends, and the only
/// frame the change sends is the existing `Cancel`, so a runner talking to
/// any 142 executor gets nothing wrong.
///
/// Version 142 (cairn/4930) makes the runner settle work held across a link
/// gap against the reattached executor's first snapshot in production, which
/// until now only its tests did: the transport stored snapshots without that
/// accounting, so an executor that died mid-command and came back left its
/// submitter waiting out the end-to-end watchdog. Settling against
/// `executingRequests` alone would resolve live work as lost whenever a link
/// blipped while an attempt was acquiring its cell, materializing its checkout
/// or between batch items, because only a spawned process appears there. So
/// [`FleetSnapshot::granted_attempts`] now names every attempt the executor
/// process holds a grant for, from the handled grant until its result is
/// queued, and the executor writes queued results ahead of any snapshot. The
/// runner reads absence from that account as loss. A 141 executor never sends
/// it, and a 142 runner reconciling against such a snapshot would settle that
/// pre-spawn work wrongly, which is why the number moves.
///
/// Version 143 (cairn/4980) changes the usable capacity of a machine with a
/// build cell whose tree still holds processes from an ended epoch, and which
/// of those processes may be signalled. No frame or field changes. Storage-sweep
/// retirement, admission-time headroom reclaim, and release now fence such a
/// cell in place (the `quarantined` lifecycle, a quarantine reason, and a drain
/// record, the values the claim path has written since cairn/4601) and carry on
/// with other cells. A 142 executor in the same state latches
/// `StorageCleanupFailed` and refuses every admission, so the runner would see
/// a whole machine refuse structurally where a 143 one admits, the kind of
/// disagreement about usable capacity v86 and v87 were minted for. And
/// survivors found only by scanning the tree, recorded under
/// [`PRIOR_EPOCH_SURVIVOR_NONCE`], are no longer signalled by the executor's
/// drain pass (a 142 executor kills them on its next sweep) nor by the runner's
/// colocated retirement, which skips them in any version's cell state.
///
/// cairn/4982 adds a runner-only fleet question without a bump. The runner's
/// fleet now asks, before submitting a check, whether any machine it knows of
/// (attached, or enrolled and detached) could ever satisfy the check's
/// selector and verdict platforms, and refuses the lane as not placeable when
/// none could. The answer is computed from capabilities executors already
/// advertise and enrollment records the runner already keeps; nothing new is
/// sent to an executor and no executor build constructs or decodes anything
/// the change touches. A refused lane is simply never submitted, which an
/// executor of any version cannot distinguish from a lane that was not
/// planned, so a runner talking to an older executor gets nothing wrong.
///
/// cairn/4815 changes the launch context the executor resolves a check's
/// reported `toolchainFingerprint` in, without a bump. The executor now probes
/// rustc and bun in the command's own working directory and launch environment,
/// so under a repository `rust-toolchain` pin (or an item's `RUSTUP_TOOLCHAIN`)
/// it reports the compiler the check actually ran under rather than the
/// machine's default. The field, its type, and its `rustc=…;bun=…` shape
/// are unchanged, and nothing the runner keys, matches, or reuses on reads it:
/// the environment fingerprint still hashes the host identity. An older
/// executor reports the host default, which is exactly what every runner has
/// recorded until now, so a runner talking to it gets nothing wrong beyond the
/// stale vintage this change exists to correct on newer executors.
///
/// cairn/4997 changes how the executor carries its link and what it reports
/// about the link and the host, without a bump. Every wire addition is an
/// optional field defaulted on decode, and no struct it lands in refuses
/// unknown fields: [`ExecutorAdvertisement::last_refresh`],
/// [`HostHealth::io_pressure`] (a `NotSampled` gap when absent, never a zero),
/// [`ExecutionLoadContext::io_pressure_full_millis`], and
/// [`ExecutorInspection::link_path`], which only the runner produces. An older
/// executor leaves the first three absent, and a runner reads that as "not
/// reported", which is what every runner assumed until now; an older runner
/// ignores them. The executor's other changes are internal to it: it ranks
/// direct routes ahead of tailnet routes when dialing, handles frames that
/// need pool state on a worker off the link in the same arrival order, builds
/// the opening snapshot there rather than before its first beat, and sets TCP
/// keepalive and `TCP_USER_TIMEOUT` on its socket
/// ([`EXECUTOR_LINK_TRANSPORT_GIVE_UP_MS`]). None of that changes a frame's
/// shape or meaning: a heartbeat could already precede the opening snapshot,
/// because the writer never ordered the two, so a runner talking to an older
/// executor gets nothing wrong.
///
/// Version 144 (cairn/4998) changes what the pressure tier reclaims and whether
/// a full volume can latch admission. The managed-cache rung is now handed the
/// volume's own deficit below the pressure line, down to a floor in the
/// pressure band and to nothing in the critical band, where 143 trimmed caches
/// only above their budget or for an admission's ask and wiped warm build
/// targets instead. And a sweep on a full volume now releases a reserve file
/// the executor keeps for its own bookkeeping, and survives an out-of-space
/// error on one entry, where a 143 executor latches `StorageCleanupFailed` and
/// declines every admission. So equivalent machines on 143 and 144 reclaim and
/// admit differently, the disagreement about usable capacity v139 and v143
/// were minted for. The wire addition is defaulted:
/// [`StoragePressureReport::withheld`] names, by [`StorageWithheldReason`],
/// what a pass left in place, and `heldBytes` is now its sum, which counts
/// cells fenced in place and managed caches (including the roots whose
/// writers keep their own retention) as well as in-window quarantines.
pub const EXECUTOR_PROTOCOL_VERSION: u32 = 144;

// Why some enums below carry `rename_all_fields`, and why not all of them do.
//
// `rename_all` on an enum renames its *variants*, not the fields inside its
// struct variants — those keep their Rust snake_case unless the variant is
// renamed individually or the container also says `rename_all_fields`. Plain
// structs are unaffected by that gap, so this module's key names are
// deliberately mixed: struct fields are camelCase throughout, while an enum's
// struct-variant fields are camelCase only where a container asks for it. The
// executor-internal messages keep theirs in snake_case — `CellOutcome::Completed`
// ships `mutation_delta`, pinned by
// `request_and_delta_round_trip_and_cancellation_is_separate` — because renaming
// them would change a wire and persisted shape that no reader benefits from.
//
// Where the rename is load-bearing is `SubstrateHealthSnapshot`, which the
// desktop UI decodes directly: `get_substrate_health` returns this type, and the
// frontend's TypeScript declares its shape by hand. A key reachable from that
// snapshot must be camelCase or it arrives as a key nothing looks for, which is
// how the Running panel came to hide every live terminal —
// `ResidentProcessStatus::Running` shipped `started_at_unix_ms` while the hook
// read `startedAtUnixMs`, so the row was dropped as though no work were running.
// `ui_facing_snapshot_serializes_only_camel_case_keys` enforces exactly that
// boundary: the snapshot projection, not this module as a whole.
//
// `RepositoryLocator` and `ResidencyEventKind` are renamed because the snapshot
// reaches them through a cell's residency, not because the enums around them
// were. `RemoteLinkState` is renamed for the same reason and arrived at it the
// hard way: it carried `rename_all` alone, so its variant names were camelCase
// while `last_beat_unix_ms`, `last_reason`, and `since_bootstrap_unix_ms` inside
// them were not — three keys reaching the desktop under names nothing reads.
//
// The snake_case `alias`es beside some renamed fields are not a second wire
// shape. They let a `cairn-build-slot-state.json` or a
// `build-slot-residency-routes.json` written before this change still
// deserialize, so an upgrade adopts its held cells instead of failing to decode
// them — which would orphan their PTY process groups and quarantine their warm
// checkouts. `persisted_cell_state_predating_camel_case_fields_still_decodes`
// pins that against the shape found on disk.

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ExecutorDistributionInfo {
    pub protocol_version: u32,
    pub target: String,
    pub distribution_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ExecutorArtifact {
    pub target: String,
    pub url: String,
    pub sha256: String,
    pub size: u64,
    pub distribution_id: String,
}

/// One prebuilt workspace sidecar published alongside the executor.
///
/// [`ExecutorArtifact`] is the runner's own dependency — the executor it installs
/// on a remote machine. These are a different thing that rides the same release:
/// the `externalBin` binaries (`cairn-cmd`, `cairn-executor`, `cairn-orch`) that
/// tauri-build demands before `cargo build` of the app crate will run at all. A
/// checkout without them compiles them from source, which is a full release build
/// — half an hour on a fresh remote build cell, paid before a single test runs.
///
/// They ride the executor's channel rather than one of their own because a second
/// publication of the same binaries from the same commit is a second thing to keep
/// in step. `version` is the workspace version stamped into the bytes
/// (`cairn_common::sidecar_version_stamp!`), so a consumer can name what it got
/// instead of trusting a file name.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SidecarArtifact {
    /// Workspace crate this binary was built from, e.g. `cairn-runner`.
    #[serde(rename = "crate")]
    pub crate_name: String,
    pub target: String,
    pub url: String,
    pub sha256: String,
    pub size: u64,
    pub version: String,
}

/// One third-party tool a headless runner runs beside itself (`jj`, `bun`,
/// `uv`), re-hosted from the upstream release that `scripts/fetch-{jj,bun,uv}.ts`
/// pins and verifies at publish time.
///
/// The bytes are a raw, already-extracted executable, never an archive. The
/// digest lives here and nowhere else a checkout-less host could read, so the
/// pins are never copied into Rust or shell. `version` is the upstream version;
/// these binaries carry no Cairn stamp, so a consumer checks size and digest only.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ToolArtifact {
    pub name: String,
    pub target: String,
    pub url: String,
    pub sha256: String,
    pub size: u64,
    pub version: String,
}

/// The target-independent resource tree a headless runner needs beside its
/// executable: `packs/**`, `resources/schemas/*.json`, and the
/// `runtime/node_modules/@cairn/*` packages, rooted exactly like the Tauri
/// resource dir. A gzip-compressed tar; `version` is the workspace version.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ResourceBundle {
    pub url: String,
    pub sha256: String,
    pub size: u64,
    pub version: String,
}

/// What a host must provide for a target's published binaries to load at all.
/// `glibc` is the highest `GLIBC_x.y` symbol version any of them references,
/// measured by the publish leg rather than asserted.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct HostRequirement {
    pub target: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub glibc: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ExecutorDistributionManifest {
    pub protocol_version: u32,
    pub artifacts: Vec<ExecutorArtifact>,
    /// Prebuilt source sidecars for this publication.
    ///
    /// Defaulted rather than required: manifests published before sidecars rode
    /// this channel carry no such key, and a runner meeting one must still
    /// resolve its executor rather than failing to parse the document.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sidecars: Vec<SidecarArtifact>,
    /// Third-party tools for a headless runner install. Defaulted for the same
    /// reason as `sidecars`; only Linux targets are expected to carry them.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tools: Vec<ToolArtifact>,
    /// The resource tree for a headless runner install, identical in every
    /// per-target manifest of one publication.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<ResourceBundle>,
    /// Host floors, per target, that the published binaries need to load.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub host_requirements: Vec<HostRequirement>,
    /// The commit whose tree produced a per-target manifest's sidecars, tools,
    /// and resources. Read only by the publisher's own planning
    /// (`scripts/plan-executor-publication.ts`); declared here so this struct
    /// and the TypeScript mirror in `scripts/lib/sidecars.ts` stay one shape.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_commit: Option<String>,
}
/// Whole-request budget for managed object downloads and uploads.
///
/// A fetch includes runner-side Git pack construction before headers and body
/// transfer afterward. This must therefore be a transfer-scale bound; treating it
/// as a short RPC deadline can interrupt the body at the deadline even while the
/// executor link and HTTP connection remain healthy.
pub const MANAGED_OBJECT_REQUEST_TIMEOUT_SECONDS: u64 = 600;
pub const EXECUTOR_PROGRESS_FRESHNESS_MS: u64 = 75_000;

/// How often the executor asserts that it is alive, and how often it recomputes
/// what that assertion carries.
///
/// Shared because three windows are sized from it and must move together: the
/// runner's stall remediation bound below, the requester-liveness window that
/// reaps a queued entry nobody is waiting for, and the runner's own cadence for
/// reporting which requests it still wants — the runner answers each beat with a
/// [`ExecutorMessage::WaitingRequests`] frame rather than keeping a second clock.
pub const EXECUTOR_HEARTBEAT_INTERVAL_MS: u64 = 30_000;

/// How long the runner tolerates total silence from an executor link before it
/// stops waiting and bounces the connection.
///
/// Derived from the executor's heartbeat: `cairn-executor` beats every 30
/// seconds and every beat bumps runner-side progress, independent of whatever
/// its cells are doing. Five consecutive missed beats therefore means the link
/// itself is dead, not that the work is slow — a check running for an hour keeps
/// the link fresh the whole time. This bounds silence, never duration.
///
/// Strictly greater than [`EXECUTOR_PROGRESS_FRESHNESS_MS`] so `ConnectedStalled`
/// stays observable to subscribers well before remediation fires; the two
/// thresholds must not collapse into each other.
pub const EXECUTOR_LINK_STALL_REMEDIATION_MS: u64 = 150_000;

const _: () = assert!(EXECUTOR_LINK_STALL_REMEDIATION_MS >= 5 * EXECUTOR_HEARTBEAT_INTERVAL_MS);

/// How long either end's kernel keeps an executor link's TCP connection open
/// once its peer has stopped answering.
///
/// The application's evidenced-silence verdict always decides first, because it
/// is the verdict that records a reason: the runner's stall bound above for a
/// link that beats nothing. The kernel is the backstop for a peer that is gone
/// and so cannot be heard from at all, and a path stall shorter than the stall
/// bound is a pause, not a bounce. Before this was stated the give-up was each
/// operating system's unstated default: Linux retransmits unacknowledged data
/// for roughly fifteen minutes, and without keepalive an idle half-open
/// connection was never noticed by either end.
///
/// One heartbeat past the stall bound, so the kernel never ends a link the
/// application would still call merely quiet.
///
/// Applied by [`crate::link_transport::apply_give_up`] to the executor's dialed
/// socket and the runner's accepted one: keepalive on both, plus
/// `TCP_USER_TIMEOUT` (the bound on unacknowledged data) on Linux. macOS has no
/// per-socket knob for its unacknowledged-data budget in the portable socket
/// API, so a macOS end gives up on unacknowledged data at its kernel's
/// retransmit exhaustion, which this change does not measure; keepalive still
/// bounds the idle half-open case there.
///
/// Ending a link sooner is not a kill path: an executor that loses its runner
/// keeps its cells running and redials, and the runner settles held work
/// against the next session's first snapshot.
pub const EXECUTOR_LINK_TRANSPORT_GIVE_UP_MS: u64 =
    EXECUTOR_LINK_STALL_REMEDIATION_MS + EXECUTOR_HEARTBEAT_INTERVAL_MS;

/// How long an executor link sits idle before its kernel sends the first
/// keepalive probe. Two beats: a link that is beating is never idle this long,
/// so keepalive only ever probes a connection whose peer has fallen silent.
pub const EXECUTOR_LINK_KEEPALIVE_IDLE_MS: u64 = 2 * EXECUTOR_HEARTBEAT_INTERVAL_MS;

/// The spacing between keepalive probes once they start.
pub const EXECUTOR_LINK_KEEPALIVE_INTERVAL_MS: u64 = 15_000;

/// How many unanswered keepalive probes end the connection, derived so that
/// idle plus every probe interval lands exactly on the give-up.
pub const EXECUTOR_LINK_KEEPALIVE_COUNT: u32 = ((EXECUTOR_LINK_TRANSPORT_GIVE_UP_MS
    - EXECUTOR_LINK_KEEPALIVE_IDLE_MS)
    / EXECUTOR_LINK_KEEPALIVE_INTERVAL_MS) as u32;

const _: () = assert!(EXECUTOR_LINK_TRANSPORT_GIVE_UP_MS > EXECUTOR_LINK_STALL_REMEDIATION_MS);
const _: () = assert!(
    EXECUTOR_LINK_KEEPALIVE_IDLE_MS
        + EXECUTOR_LINK_KEEPALIVE_INTERVAL_MS * EXECUTOR_LINK_KEEPALIVE_COUNT as u64
        == EXECUTOR_LINK_TRANSPORT_GIVE_UP_MS
);
const _: () = assert!(EXECUTOR_LINK_KEEPALIVE_IDLE_MS > EXECUTOR_HEARTBEAT_INTERVAL_MS);
// Kernels take keepalive timings in whole seconds.
const _: () = assert!(
    EXECUTOR_LINK_KEEPALIVE_IDLE_MS.is_multiple_of(1_000)
        && EXECUTOR_LINK_KEEPALIVE_INTERVAL_MS.is_multiple_of(1_000)
);

/// How long the runner tolerates a link that beats on time while the facts
/// those beats carry stay frozen, before it bounces the connection.
///
/// An executor emits beats from one task and recomputes what they carry on
/// another. When the second task wedges, the first keeps beating: the link is
/// perfect, progress is fresh, and every number the runner holds about that
/// machine is history. Placement correctly refuses such a machine, and without
/// this bound it refuses it forever — the fleet quietly collapses to whichever
/// machines attached most recently, with no disconnect anywhere to explain it.
///
/// Ten refresh intervals. The payload recomputes on the beat's own cadence, so
/// this is ten consecutive passes that produced nothing while the link stayed
/// demonstrably alive — far past a slow pass, and not something an executor
/// grinding through a storage walk reaches, because a slow pass still finishes
/// and stamps.
///
/// Strictly greater than [`EXECUTOR_LINK_STALL_REMEDIATION_MS`] so silence is
/// always the earlier and stronger verdict: a link the runner has already given
/// up on for going quiet must never be re-diagnosed as a telemetry problem.
pub const EXECUTOR_TELEMETRY_STALL_REMEDIATION_MS: u64 = 10 * EXECUTOR_HEARTBEAT_INTERVAL_MS;

const _: () = assert!(EXECUTOR_TELEMETRY_STALL_REMEDIATION_MS > EXECUTOR_LINK_STALL_REMEDIATION_MS);

/// How long the executor's liveness refresher waits for one pass before it
/// publishes what it already has, naming the phase the pass is stuck in.
///
/// This bounds a *wait*, never a run. Nothing is cancelled when a pass crosses
/// it, no work is charged for crossing it, and a pass that returns afterwards
/// publishes its facts normally. What ends at the bound is the refresher's own
/// blocking on it, because exactly one refresher exists: a pass that never
/// returns otherwise freezes a healthy machine's advertised facts permanently,
/// with no evidence anywhere of what it is waiting on (CAIRN-4420).
///
/// Two heartbeat intervals — generous by orders of magnitude for a pass that
/// normally takes milliseconds — and far enough below
/// [`EXECUTOR_TELEMETRY_STALL_REMEDIATION_MS`] that the explanation reaches the
/// runner several beats before the freeze it explains could be remediated.
pub const LIVENESS_REFRESH_BOUND_MS: u64 = 2 * EXECUTOR_HEARTBEAT_INTERVAL_MS;

const _: () = assert!(
    LIVENESS_REFRESH_BOUND_MS + 2 * EXECUTOR_HEARTBEAT_INTERVAL_MS
        < EXECUTOR_TELEMETRY_STALL_REMEDIATION_MS
);

/// A refresh phase slower than this is named in the executor's log along with
/// how long it took.
///
/// The phases take different locks and touch different subsystems, so which one
/// is slow is most of why it is slow. One production pass over this threshold
/// locates a wedge that a frozen advertisement can only report the existence
/// of.
pub const LIVENESS_REFRESH_PHASE_WARN_MS: u64 = 5_000;

/// How long a queued entry survives without the runner confirming that somebody
/// is still waiting for it.
///
/// This is what makes a long wait horizon safe. A queued entry consumes a
/// `max_queue_entries` slot, and a horizon measured in hours would let an entry
/// whose requesting core died hold that slot for hours. Elapsed time cannot tell
/// the difference between a patient requester and a dead one, so the executor
/// stops inferring and observes instead: the runner reports the set of requests
/// it still has a live waiter for, and an entry unconfirmed for this long is
/// dropped without an answer, because there is nobody to answer.
///
/// Three heartbeat intervals. The runner reports on each beat, so one lost frame
/// must not reap live work; and the window sits below
/// [`EXECUTOR_LINK_STALL_REMEDIATION_MS`] so a link the runner has already given
/// up on is not still being honoured here.
pub const REQUESTER_LIVENESS_WINDOW_MS: u64 = 3 * EXECUTOR_HEARTBEAT_INTERVAL_MS;

const _: () = assert!(REQUESTER_LIVENESS_WINDOW_MS > EXECUTOR_HEARTBEAT_INTERVAL_MS);
const _: () = assert!(REQUESTER_LIVENESS_WINDOW_MS < EXECUTOR_LINK_STALL_REMEDIATION_MS);

/// The longest a detached `cairn-executor` waits before reaching for its runner
/// again.
///
/// A gap is not an error state on the executor's side: its cells keep running
/// through one, so this paces reconnection to spare a restarting runner a
/// reconnect storm rather than to give anything time to heal.
pub const EXECUTOR_RECONNECT_MAX_DELAY_MS: u64 = 30_000;

/// How long a stopping `cairn-executor` waits for its own in-flight blocking
/// work before it exits.
///
/// A build cell's authority is a kernel file lock held by the executor process,
/// and the kernel releases it only when that process is gone. So this is not a
/// courtesy extended to background maintenance — it is the length of the outage
/// the successor inherits, because until the outgoing process exits no
/// replacement can adopt a single held cell. A storage sweep walks the whole
/// executor home and routinely runs for minutes; waiting for one to finish once
/// cost the fleet 75 seconds of "no machines are enrolled" (CAIRN-3420).
/// Reclaim is best effort and its staging protocol already tolerates being
/// interrupted, so the exit is bounded and a sweep in flight is abandoned
/// rather than awaited.
pub const EXECUTOR_SHUTDOWN_BUDGET_MS: u64 = 3_000;

/// The total time a starting `cairn-executor` spends waiting for build cell
/// authorities its predecessor has not finished releasing.
///
/// Contention here is the ordinary shape of a generational handoff rather than
/// a fault. A kernel lock is never left behind by a dead process, so a contended
/// authority always means a live holder — and after a runner restart that holder
/// is the outgoing executor on its way out. Waiting is therefore the correct
/// response, and it is spent inside one process: a supervisor's restart cadence
/// is not a retry policy.
///
/// One budget for the whole tree rather than one per authority. Forty
/// authorities each worth a few seconds is how a bounded handoff becomes an
/// unbounded outage.
///
/// For a colocated executor this wait is a fence, not the handoff. The runner
/// retires the outgoing executor and verifies the generation lock is free before
/// it starts a successor ([`EXECUTOR_RETIREMENT_GRACE_MS`]), so a successor that
/// still has to wait here means something the runner does not own holds the
/// home (cairn/4569).
pub const EXECUTOR_ADOPTION_HANDOFF_BUDGET_MS: u64 = 8_000;

/// How long the runner's supervisor waits for a spawned `cairn-executor` to
/// reach readiness before it declares the startup failed.
///
/// Readiness is announced after adoption, so this has to contain a worst-case
/// handoff wait with room to spare. Sized below it, the supervisor would kill a
/// successor for patiently doing the right thing.
pub const EXECUTOR_STARTUP_READY_BUDGET_MS: u64 = 12_000;

// The three budgets above are one chain, and each link must clear the next: an
// outgoing executor has to be gone before its successor stops waiting for the
// locks it holds, and that wait has to finish before the supervisor stops
// waiting for the successor. Ordered any other way the chain manufactures
// exactly the crash-loop it exists to prevent, which is why the ordering is
// asserted here rather than trusted to three files that never mention each
// other.
const _: () = assert!(EXECUTOR_SHUTDOWN_BUDGET_MS < EXECUTOR_ADOPTION_HANDOFF_BUDGET_MS);
const _: () = assert!(EXECUTOR_ADOPTION_HANDOFF_BUDGET_MS < EXECUTOR_STARTUP_READY_BUDGET_MS);

/// How long a stopping `cairn-executor` spends stopping its resident processes
/// (terminals, services, dev instances, workflow runtimes) before it gives up on
/// the stragglers and exits anyway.
///
/// Each resident process lives in its own process group, so a group that is not
/// stopped here outlives the executor and is found only by the next
/// generation's adoption. Bounded because the runner retiring this executor
/// escalates to SIGKILL after [`EXECUTOR_RETIREMENT_GRACE_MS`], and a stop that
/// is still running then is the one that leaves the most behind.
pub const EXECUTOR_RESIDENT_STOP_BUDGET_MS: u64 = 3_000;

/// How long the runner waits for an outgoing colocated executor to exit on its
/// own after asking it to (a protocol `Shutdown`, then SIGTERM).
///
/// It has to contain the executor's whole orderly exit, which is its resident
/// stop followed by its bounded runtime shutdown. Sized below that, the runner
/// would SIGKILL an executor that was in the middle of stopping its resident
/// groups, and those groups are not in the executor's own group.
pub const EXECUTOR_RETIREMENT_GRACE_MS: u64 = 7_000;

/// The one window everything after the graceful wait shares: SIGKILL of the
/// executor and its own group, the verified SIGKILL of every workload process
/// it recorded in groups of their own, and waiting for the kernel to report all
/// of them gone and every cell lock released.
///
/// One deadline rather than one per step, so the assertion below bounds the
/// whole ceremony and not just its first half.
pub const EXECUTOR_RETIREMENT_FORCE_MS: u64 = 3_000;

/// launchd's default exit timeout, the window between the SIGTERM a service
/// manager sends the runner and the SIGKILL that skips every destructor.
/// Nothing in the generated launchd plist overrides it.
pub const SERVICE_MANAGER_EXIT_TIMEOUT_MS: u64 = 20_000;

const _: () = assert!(
    EXECUTOR_RESIDENT_STOP_BUDGET_MS + EXECUTOR_SHUTDOWN_BUDGET_MS < EXECUTOR_RETIREMENT_GRACE_MS
);
// The whole retirement must finish well inside the service manager's window, or
// a runner stopped by its service manager is killed mid-ceremony and the
// executor it was retiring is orphaned: the founding shape of cairn/4569.
const _: () = assert!(
    2 * (EXECUTOR_RETIREMENT_GRACE_MS + EXECUTOR_RETIREMENT_FORCE_MS)
        <= SERVICE_MANAGER_EXIT_TIMEOUT_MS
);

/// The attempt id an execution-environment acquisition carries.
///
/// Both sides derive it, because both sides have to name the same queue entry:
/// the executor mints it when it enqueues the acquisition, and the runner uses it
/// to ask "is this acquisition still being worked on" through exactly the probe a
/// submitted request uses. An acquisition is one wait rather than a series of
/// attempts, so this is a constant instead of an identity.
///
/// This replaces the answer margin that used to sit here. That margin existed to
/// let an executor which had missed a flat deadline still answer with a diagnosis,
/// and to cap how long the executor could pause the same deadline. Neither job
/// survives: nothing pauses a horizon, and the runner's wait now bounds silence,
/// so at the instant an executor answers its own horizon expiry the runner has
/// just observed progress and is still listening.
pub const RESIDENCY_ACQUIRE_ATTEMPT_ID: &str = "acquire";

/// The public name reserved for the executor the runner supervises inside its
/// own process tree. Every fleet has exactly one, and it is addressable under
/// this name whatever the machine happens to be called.
pub const LOCAL_EXECUTOR_NAME: &str = "local";

/// What a public executor name is, in one sentence, for every message that has
/// to reject one.
pub const EXECUTOR_NAME_RULE: &str = "an executor name is hostname-like: lowercase ASCII letters, digits, and hyphens, beginning and ending with a letter or digit";

/// Fold an operator-supplied label into the public name that addresses it.
///
/// Names are the address space agents type, so they are normalized on the way
/// in rather than validated and refused: `"BG Lab (Ubuntu)"` and `"bg-lab-ubuntu"`
/// are the same machine, and an agent that reads one in `cairn://executors` and
/// writes the other in a placement request must reach it. `None` means the label
/// contains nothing that can be part of an address.
pub fn normalize_executor_name(raw: &str) -> Option<String> {
    let mut name = String::with_capacity(raw.len());
    for character in raw.chars() {
        let mapped = if character.is_ascii_alphanumeric() {
            character.to_ascii_lowercase()
        } else {
            '-'
        };
        if mapped == '-' && (name.is_empty() || name.ends_with('-')) {
            continue;
        }
        name.push(mapped);
    }
    while name.ends_with('-') {
        name.pop();
    }
    (!name.is_empty()).then_some(name)
}

/// Whether two labels address the same machine.
pub fn executor_names_match(left: &str, right: &str) -> bool {
    match (
        normalize_executor_name(left),
        normalize_executor_name(right),
    ) {
        (Some(left), Some(right)) => left == right,
        _ => false,
    }
}

/// Which machine a request is asking for.
///
/// One vocabulary for every surface that requests placement — the run tool, a
/// project check, the SDK, the protocol. A machine is addressed by its public
/// name or by the platform it must provide, never by both and never by an
/// opaque identity: names are what [`crate::uri::CairnResource::Executors`]
/// publishes, so everything this accepts is discoverable before it is used.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ExecutorSelector {
    /// The executor's public name, as `cairn://executors` lists it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// The operating system the work needs. Mutually exclusive with `name`:
    /// naming a machine already settles its platform, so accepting both would
    /// let a request contradict itself.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub os: Option<String>,
    /// Toolchains the chosen machine must advertise. Refines either selector,
    /// and stands alone as "anywhere that can build this".
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_toolchains: Vec<String>,
    /// On macOS, require proof that the executor parent can apply a nested
    /// Seatbelt profile and write inside its explicit allowance. Other
    /// platforms are unaffected.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub requires_macos_nested_sandbox_write: bool,
    /// Maximum edge authority this workload permits.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub containment: Option<WorkloadEnvelope>,
}

impl ExecutorSelector {
    pub fn is_empty(&self) -> bool {
        self.name.is_none()
            && self.os.is_none()
            && self.required_toolchains.is_empty()
            && !self.requires_macos_nested_sandbox_write
            && self.containment.is_none()
    }

    /// Reject a selector that asks for nothing, or for two contradictory things.
    pub fn validate(&self) -> Result<(), String> {
        if self.name.is_some() && self.os.is_some() {
            return Err(
                "an executor selector states name or os, never both: naming a machine already settles its platform"
                    .into(),
            );
        }
        if self.is_empty() {
            return Err(
                "an executor selector must state at least one of name, os, requiredToolchains, requiresMacosNestedSandboxWrite, or containment"
                    .into(),
            );
        }
        if let Some(containment) = &self.containment {
            containment.validate()?;
        }
        if let Some(name) = &self.name {
            if normalize_executor_name(name).is_none() {
                return Err(format!(
                    "executor name {name:?} addresses no machine: {EXECUTOR_NAME_RULE}"
                ));
            }
        }
        Ok(())
    }

    /// What this selector asked for, in the words a refusal has to use.
    pub fn describe(&self) -> String {
        let mut parts = Vec::new();
        if let Some(name) = &self.name {
            parts.push(format!("executor {name}"));
        }
        if let Some(os) = &self.os {
            parts.push(format!("os {os}"));
        }
        if !self.required_toolchains.is_empty() {
            parts.push(format!(
                "toolchains {}",
                self.required_toolchains.join(", ")
            ));
        }
        if self.requires_macos_nested_sandbox_write {
            parts.push("measured macOS nested Seatbelt write capability".into());
        }
        if let Some(containment) = &self.containment {
            parts.push(containment.describe());
        }
        if parts.is_empty() {
            "any executor".to_string()
        } else {
            parts.join(" with ")
        }
    }
}

/// Whether placement policy may choose among machines for a request, stated by
/// the submitter as a fact about the work rather than inferred from the absence
/// of a selector.
///
/// Three separate things travel on a [`CellRequest`] and they are deliberately
/// not collapsed into one another. [`ExecutorSelector`] is what the caller
/// *asked for*. `pinnedExecutorId` is where the work *already lives*. Mobility
/// is whether policy is *allowed to decide*.
///
/// Deriving mobility from an empty selector is the failure this type exists to
/// prevent: an agent's tree-bound `run` batch states no selector and is
/// nonetheless home-bound, because its working tree is a leased cell on one
/// machine. "Untargeted" and "free to move" are different properties, and a
/// policy that confuses them ships a job's tree to a machine that does not have
/// it. The map of which request classes are which lives in
/// `docs/execution-fabric.md`.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum PlacementMobility {
    /// The conservative default. This request runs where its pin says, where its
    /// selector says, or — stating neither — on the colocated executor that
    /// holds the runner's own checkout. Policy chooses nothing.
    #[default]
    PinnedOrColocated,
    /// This work is disposable, produces a verdict rather than a mutation, needs
    /// no runner-local ignored content, and can be materialized from managed
    /// objects on any enrolled machine. Policy may rank the fleet and place it
    /// on measured-idle capacity.
    SpillEligible,
}

impl PlacementMobility {
    pub fn may_spill(self) -> bool {
        matches!(self, Self::SpillEligible)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::PinnedOrColocated => "pinnedOrColocated",
            Self::SpillEligible => "spillEligible",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ExecutorIdentity {
    pub device_id: String,
    pub executor_id: String,
    pub display_name: String,
}

/// What one toolchain probe ran, and what came back.
///
/// An advertised toolchain set is a claim about a machine, and a claim with no
/// evidence behind it cannot be checked. This carries the evidence so that
/// "this machine has no Rust" is distinguishable from "the probe never worked",
/// which are the same empty list without it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ToolchainProbe {
    /// The toolchain this probe decides, spelled as a placement selector's
    /// `requiredToolchains` spells it.
    pub toolchain: String,
    /// The command that was run, verbatim.
    pub command: String,
    /// Whether the toolchain ended up advertised.
    pub detected: bool,
    /// Where the program resolved on the executor's own PATH. Absent means it
    /// was not on PATH, which on Windows does not by itself mean unspawnable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_path: Option<String>,
    /// The verdict in the machine's own words: the version line that proved the
    /// toolchain works, or the failure the OS or the tool reported.
    pub detail: String,
}

/// The evidence behind an executor's advertised toolchain set.
///
/// The account is here because it is frequently the entire answer. A per-user
/// toolchain install belongs to one account, so a machine can hold a working
/// toolchain that the account the executor runs as cannot reach — a state that
/// is invisible to anyone reading only the resulting empty list, and which cost
/// CAIRN-3407 a live investigation to establish.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ToolchainDetection {
    /// The OS account the executor runs as, whose PATH and per-user installs
    /// the probes actually see.
    pub account: String,
    /// The home directory the executor's PATH composition was built around.
    pub home: String,
    pub probes: Vec<ToolchainProbe>,
}

impl ToolchainDetection {
    /// The toolchain names to advertise: exactly those a probe confirmed.
    ///
    /// Deriving the advertised set from the probes rather than assembling it
    /// separately is what keeps the claim and its evidence from disagreeing.
    pub fn advertised(&self) -> Vec<String> {
        self.probes
            .iter()
            .filter(|probe| probe.detected)
            .map(|probe| probe.toolchain.clone())
            .collect()
    }
}

/// What a prerequisite script concluded about the machine it ran on.
///
/// A missing package and an unrunnable check are opposite facts, and both
/// collapse into "the lane failed" without a vocabulary that separates them —
/// the same distinction [`ToolchainDetection`] draws between a machine with no
/// toolchain and a probe that never worked.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum BuildPrerequisiteStatus {
    /// The script ran and found nothing missing.
    Satisfied,
    /// The script ran and named what this machine does not have.
    Missing,
    /// The script could not be run at all, so nothing is known either way.
    /// Deliberately not `Missing`: an unprovisioned machine and one whose check
    /// is broken need different repairs.
    Unchecked,
}

/// Whether a machine carries the system packages Cairn's workspace needs in
/// order to compile, judged by the repository's own provisioning script.
///
/// This exists because the toolchain vocabulary is coarser than the workspace's
/// real requirement. `cargo --version` succeeds on a Linux machine with no
/// `libwebkit2gtk-4.1-dev`, so that machine honestly advertises `rust` and is
/// still structurally unable to build the desktop crate: every lane whose crate
/// cone reaches it dies inside a `glib-sys` build script before one first-party
/// crate compiles, and returns an infrastructure exit instead of a verdict. A
/// real code failure underneath is masked completely.
///
/// A `Missing` verdict is enforced by placement, narrowly: it excludes the
/// machine from work that needs `rust` (by selector or by the project's setup)
/// for a project whose checkout declares the same script that produced the
/// verdict. Work that needs no Rust build keeps flowing there, and a project
/// that does not declare the script is described by nothing this measured.
/// `Unchecked` and an absent verdict gate nothing, because nothing is known.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BuildPrerequisiteCheck {
    /// The script that decided this, spelled as the repository spells it, so a
    /// verdict names the command that would repair the machine.
    pub script: String,
    pub status: BuildPrerequisiteStatus,
    /// The script's own diagnostic, verbatim: on failure it names the host, the
    /// packages it could not find, and the provisioning command to run. Carried
    /// rather than summarized because the summary is what makes an
    /// infrastructure exit unactionable in the first place.
    pub detail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ExecutorCapabilities {
    pub os: String,
    pub arch: String,
    pub logical_cores: usize,
    /// Effective machine-wide CPU admission capacity after role-specific reserves.
    /// Older executors omit it, in which case peers fall back to logical cores.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub concurrency_capacity: Option<u32>,
    #[serde(default)]
    pub toolchains: Vec<String>,
    #[serde(default)]
    pub projects_served: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk_budget_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_budget_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sandbox: Option<SandboxCapabilityEvidence>,
    /// Edge-granular evidence. Absence means this executor predates reporting.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub containment: Option<ContainmentObservations>,
    /// The evidence behind `toolchains`, absent from an executor built before
    /// probes were reported.
    ///
    /// Additive and defaulted on purpose, so this is not a wire compatibility
    /// boundary and does not bump [`EXECUTOR_PROTOCOL_VERSION`]: an older peer
    /// omits the key, a newer runner renders that omission honestly, and
    /// nothing about placement changes either way. `None` (this peer cannot
    /// explain itself) and `Some` with no probes (this peer probed nothing) are
    /// deliberately different facts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub toolchain_detection: Option<ToolchainDetection>,
    /// Whether this machine can actually build the workspace, as distinct from
    /// whether it has a Rust toolchain. Absent from an executor built before
    /// the prerequisite check existed.
    ///
    /// Additive and defaulted for the same reason [`Self::toolchain_detection`]
    /// is: an older peer omits the key and a newer runner renders the omission
    /// honestly. An absent verdict changes nothing about placement; a
    /// `Missing` one excludes the machine from Rust work for projects that
    /// declare the script (see [`BuildPrerequisiteCheck`]).
    ///
    /// The verdict is taken once, when the executor attaches, so a machine
    /// provisioned or degraded while it stays attached keeps reporting the
    /// verdict from its last attach. That is a known seam rather than an
    /// oversight: refreshing a capability fact needs a producer for
    /// [`ExecutorMessage::AdvertisementUpdated`], which this executor does not
    /// yet have, and inventing a second refresh path for this one fact would
    /// have to be torn out when that producer lands.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub build_prerequisites: Option<BuildPrerequisiteCheck>,
}

impl ExecutorCapabilities {
    /// The machine-wide concurrency budget this executor admits against.
    pub const fn admission_concurrency_budget(&self) -> u32 {
        match self.concurrency_capacity {
            Some(capacity) => capacity,
            None => admission_concurrency_budget(self.logical_cores),
        }
    }
}

/// The machine-wide concurrency budget an executor admits against, derived from
/// the logical core count it advertises.
///
/// One function rather than one formula written twice. The executor publishes
/// this budget and the runner must resolve a whole-machine reservation against
/// it, and while those were separate expressions they drifted: the runner
/// clamped saturating demand to `logical_cores.max(1)` while the executor
/// admitted against `logical_cores.max(2)`. On a single-core executor that made
/// a lane which had declared the whole machine reserve one of two units, so a
/// second lane could run beside a check that must not overlap anything.
///
/// The floor of two exists so a single-core host can still make progress on work
/// that arrives as a pair rather than deadlocking on its own budget.
pub const fn admission_concurrency_budget(logical_cores: usize) -> u32 {
    let units = if logical_cores < 2 { 2 } else { logical_cores };
    if units > u32::MAX as usize {
        u32::MAX
    } else {
        units as u32
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub enum GitObjectFormat {
    Sha1,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub struct RepositoryIdentity {
    pub project_id: String,
    pub repository_id: String,
    pub object_format: GitObjectFormat,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum RepositoryLocator {
    ScratchOnly {
        #[serde(alias = "owner_id")]
        owner_id: String,
    },
    ColocatedPath {
        #[serde(alias = "project_id")]
        project_id: String,
        #[serde(alias = "repository_id")]
        repository_id: String,
        #[serde(alias = "absolute_path")]
        absolute_path: String,
    },
    ExistingCheckout {
        #[serde(alias = "project_id")]
        project_id: String,
        #[serde(alias = "repository_id")]
        repository_id: String,
        #[serde(alias = "absolute_path")]
        absolute_path: String,
    },
    ManagedObjects {
        #[serde(alias = "project_id")]
        project_id: String,
        #[serde(alias = "repository_id")]
        repository_id: String,
        #[serde(alias = "object_format")]
        object_format: GitObjectFormat,
    },
}

impl RepositoryLocator {
    pub fn identity(&self) -> RepositoryIdentity {
        match self {
            Self::ScratchOnly { owner_id } => RepositoryIdentity {
                project_id: owner_id.clone(),
                repository_id: owner_id.clone(),
                object_format: GitObjectFormat::Sha1,
            },
            Self::ColocatedPath {
                project_id,
                repository_id,
                ..
            }
            | Self::ExistingCheckout {
                project_id,
                repository_id,
                ..
            } => RepositoryIdentity {
                project_id: project_id.clone(),
                repository_id: repository_id.clone(),
                object_format: GitObjectFormat::Sha1,
            },
            Self::ManagedObjects {
                project_id,
                repository_id,
                object_format,
            } => RepositoryIdentity {
                project_id: project_id.clone(),
                repository_id: repository_id.clone(),
                object_format: *object_format,
            },
        }
    }

    pub fn project_id(&self) -> &str {
        match self {
            Self::ScratchOnly { owner_id } => owner_id,
            Self::ColocatedPath { project_id, .. }
            | Self::ExistingCheckout { project_id, .. }
            | Self::ManagedObjects { project_id, .. } => project_id,
        }
    }

    pub fn repository_id(&self) -> &str {
        match self {
            Self::ScratchOnly { owner_id } => owner_id,
            Self::ColocatedPath { repository_id, .. }
            | Self::ExistingCheckout { repository_id, .. }
            | Self::ManagedObjects { repository_id, .. } => repository_id,
        }
    }

    pub fn colocated_path(&self) -> Option<&str> {
        match self {
            Self::ColocatedPath { absolute_path, .. }
            | Self::ExistingCheckout { absolute_path, .. } => Some(absolute_path),
            Self::ScratchOnly { .. } | Self::ManagedObjects { .. } => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct VerifiedWarmRoot {
    pub repository: RepositoryIdentity,
    pub commit: String,
}

/// The phases of one liveness refresh pass, in the order the pass runs them.
///
/// Named on the wire because a pass that stops returning can only be diagnosed
/// by where it stopped. Each phase waits on something different — residency
/// operation locks, the managed-object manifests, pool state, host probes, the
/// snapshot consumer — and an advertisement that merely stopped moving says
/// nothing about which of them the machine is inside.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum LivenessRefreshPhase {
    /// Reclaiming residencies whose holders stopped answering. Takes pool state
    /// and the per-holder residency operation locks.
    ExpireResidencies,
    /// Dropping in-memory managed-object manifests nothing is using.
    EvictIdleManifests,
    /// Reading pool state to build the fleet snapshot.
    Snapshot,
    /// Sampling host probes and queue health.
    SubstrateReport,
    /// Collecting the verified warm roots this machine can serve.
    WarmRoots,
    /// Swapping in the new report and handing the snapshot to its consumer.
    Publish,
}

impl LivenessRefreshPhase {
    /// The phases in the order one pass runs them.
    pub const ORDER: [Self; 6] = [
        Self::ExpireResidencies,
        Self::EvictIdleManifests,
        Self::Snapshot,
        Self::SubstrateReport,
        Self::WarmRoots,
        Self::Publish,
    ];

    /// What the machine is waiting on, for a log line or a resource a person
    /// reads.
    pub fn label(self) -> &'static str {
        match self {
            Self::ExpireResidencies => "expiring residencies",
            Self::EvictIdleManifests => "evicting idle manifests",
            Self::Snapshot => "reading pool state",
            Self::SubstrateReport => "sampling substrate health",
            Self::WarmRoots => "collecting warm roots",
            Self::Publish => "publishing the snapshot",
        }
    }
}

/// Why the facts a heartbeat carries are still the previous pass's.
///
/// Its presence is the whole point. A machine whose refresh wedged used to go
/// on beating with facts that quietly aged, indistinguishable from a machine
/// whose facts genuinely had not changed, and the only way to tell them apart
/// was to wait out the stall bound and bounce the link. An executor that says
/// *these facts are stale and here is the phase I am stuck in* has reported the
/// fault itself, and a report is not a reason to destroy the reporter
/// (CAIRN-4420).
///
/// Both stamps are the executor's own clock, like
/// [`ExecutorAdvertisement::liveness_observed_at_unix_ms`], so a reader derives
/// ages exactly as it already does for the facts themselves.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LivenessRefreshBlocked {
    /// The phase the pass entered and has not left.
    pub phase: LivenessRefreshPhase,
    /// When the blocked pass began.
    pub pass_started_unix_ms: u64,
    /// When it entered the phase it is stuck in. Carried separately because a
    /// pass that spent its budget across every phase and a pass that went into
    /// one phase and never came out are different faults.
    pub phase_entered_unix_ms: u64,
}

/// The two ends of one executor link's TCP connection.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ExecutorLinkPath {
    /// Where the executor connected from.
    pub executor: std::net::SocketAddr,
    /// The runner's address the executor reached.
    pub runner: std::net::SocketAddr,
}

impl ExecutorLinkPath {
    /// Whether the link rides a tailnet tunnel rather than a direct route.
    pub fn through_tunnel(&self) -> bool {
        crate::link_transport::is_tunnel_address(self.executor.ip())
            || crate::link_transport::is_tunnel_address(self.runner.ip())
    }
}

/// How long the executor's last completed liveness refresh pass took, phase by
/// phase, through the phase before it published.
///
/// Carried on every beat rather than logged only when slow, because a pass's
/// duration is how stale the facts it published were on arrival, and the slow
/// phase is most of the diagnosis: a pass that spent a minute waiting for pool
/// state behind a stalled write and one that spent a minute doing work look the
/// same in the advertisement's stamp alone.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LivenessRefreshTimings {
    pub pass_started_unix_ms: u64,
    /// From the pass's start to its publishing.
    pub pass_ms: u64,
    /// Every phase the pass completed before publishing, in the order it ran.
    #[serde(default)]
    pub phases: Vec<LivenessRefreshPhaseTiming>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LivenessRefreshPhaseTiming {
    pub phase: LivenessRefreshPhase,
    pub elapsed_ms: u64,
    /// Of `elapsed_ms`, how long the phase waited to acquire pool state, for a
    /// phase that takes it once. `None` for a phase whose wait was not
    /// measured, which is not a claim that it did not wait.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pool_state_wait_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ExecutorAdvertisement {
    pub identity: ExecutorIdentity,
    pub capabilities: ExecutorCapabilities,
    pub current_load: usize,
    #[serde(default)]
    pub warm_roots: Vec<VerifiedWarmRoot>,
    /// When this beat was sent. Heartbeat age, and nothing else: the runner's
    /// connection health is derived from it.
    pub observed_at_unix_ms: u64,
    /// When the facts this beat carries were *measured*, which is not when the
    /// beat was sent. The executor computes its heartbeat payload on a task
    /// separate from the one that emits beats, precisely so that housekeeping
    /// cannot silence it; the cost of that split is that a wedged producer
    /// keeps advertising, with facts that quietly age. This is the field that
    /// makes the ageing visible, so a fresh beat carrying hour-old load, disk,
    /// and warm-root facts cannot pass for a healthy executor.
    ///
    /// `None` from an executor that does not report it, which is read as "no
    /// claim" rather than as infinitely stale.
    #[serde(default)]
    pub liveness_observed_at_unix_ms: Option<u64>,
    /// Set while a refresh pass has overrun
    /// [`LIVENESS_REFRESH_BOUND_MS`], naming the phase it is stuck in. This is
    /// what makes the stamp above stand still *with an explanation* rather than
    /// silently, and the difference is what separates a machine reporting its
    /// own fault from a machine that has stopped saying anything true.
    ///
    /// `None` both from an executor whose last pass completed and from one too
    /// old to report it. Reading them the same way is deliberate: no
    /// explanation is the state every runner assumed before this field existed,
    /// and it is the state the stall watchdog was written for.
    #[serde(default)]
    pub facts_stale_refresh_blocked: Option<LivenessRefreshBlocked>,
    /// How long the pass that produced these facts took, and where the time
    /// went. `None` from an executor too old to report it, and before a
    /// machine's first pass completes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_refresh: Option<LivenessRefreshTimings>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "mode", rename_all = "camelCase")]
pub enum ExecutorEnrollmentIdentity {
    /// The runner's own supervised child, authenticated by the runner's MCP
    /// token.
    ///
    /// `supervision_nonce` is what the supervisor handed this launch through
    /// [`EXECUTOR_SUPERVISION_NONCE_ENV`]. The token alone survives the runner
    /// that issued it, so without the nonce an executor orphaned by a killed
    /// runner reattaches to the next one as though it were its child.
    ///
    /// Defaulted rather than required, so a Hello from an executor that
    /// predates the field still decodes and is refused by version. A Hello that
    /// failed to decode would read to that executor as a lost link, and it
    /// would redial forever.
    Colocated {
        #[serde(default)]
        supervision_nonce: String,
    },
    Grant {
        token: String,
        expected_runner_device_id: String,
    },
    Credential {
        credential: String,
        expected_runner_device_id: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum EnrollmentRejectionReason {
    Unenrolled,
    Expired,
    Revoked,
    Superseded,
    IdentityMismatch,
    RunnerIdentityMismatch,
    MalformedAdvertisement,
    /// The public name this executor presented already addresses a different
    /// enrolled machine. Names are the address space placement requests are
    /// written in, so letting a second machine answer to one would silently
    /// route work to the wrong host.
    NameConflict {
        /// The name that was refused.
        name: String,
        /// The executor already holding it, so an operator can tell which of the
        /// two to rename.
        holder: String,
    },
}

impl EnrollmentRejectionReason {
    /// The operator-facing sentence for a refusal, where the variant alone is
    /// not enough to act on.
    pub fn diagnostic(&self) -> Option<String> {
        match self {
            Self::Revoked => Some(
                "this executor enrollment was revoked; enroll the machine again if it should rejoin the fleet".into(),
            ),
            Self::Superseded => Some(
                "this credential was superseded by one the executor confirmed receiving; a restored disk image or copied credential file cannot reconnect, so restore the current credential or enroll the machine again".into(),
            ),
            Self::NameConflict { name, holder } => Some(format!(
                "the public name {name} already addresses enrolled executor {holder}; give one of them a different name with `cairn executor rename {name} <new-name>`"
            )),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "camelCase")]
pub enum CellPriority {
    ReviewCheck,
    WriteCheck,
    AgentInteractive,
}

/// How long a queued request waits before priority aging promotes it one tier.
///
/// Lives here rather than inside the executor because two parties now depend on
/// the same ordering: the executor's admission, which is the only authority
/// that may actually dequeue anything, and the runner's read-only forecast of
/// how much work sits ahead of a request it is considering placing. A forecast
/// computed under a different aging rule than the queue it describes would
/// predict a wait that machine was never going to have.
pub const PRIORITY_AGING_INTERVAL_MS: u64 = 5_000;

/// The tier a queued request currently ranks in, after aging.
///
/// Aging saturates at the top tier, so a request that has waited long enough
/// competes with interactive work however it was submitted. Higher is earlier.
pub fn aged_priority(priority: CellPriority, queued_at_unix_ms: u64, now_unix_ms: u64) -> u64 {
    let base = match priority {
        CellPriority::ReviewCheck => 0_u64,
        CellPriority::WriteCheck => 1,
        CellPriority::AgentInteractive => 2,
    };
    let promotions = now_unix_ms
        .saturating_sub(queued_at_unix_ms)
        .checked_div(PRIORITY_AGING_INTERVAL_MS)
        .unwrap_or(0);
    base.saturating_add(promotions).min(2)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum MutationPolicy {
    PureVerdict,
    AllowDelta,
}

pub const COMMAND_RESOURCE_IDENTITY_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CommandResourceIdentity {
    pub version: u32,
    pub key: String,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ResourceReservationSource {
    Learned,
    Declared,
    /// Nothing is known about what this work costs.
    ///
    /// The name matters: this is an ABSENCE of measurement, not a measurement
    /// that happens to be small. Reading it as a known-small quantity is how a
    /// headcount of zero-charged resident processes came to be reported as host
    /// pressure, starving admission exactly when the machine was busiest
    /// (CAIRN-3258). A consumer that cannot act on "unknown" must treat it as
    /// unpressured and rely on a real backstop, per CAIRN-3188.
    #[default]
    Unmeasured,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ResourceReservation {
    pub memory_bytes: u64,
    pub disk_growth_bytes: u64,
    #[serde(default = "default_concurrency_units")]
    pub concurrency_units: u32,
    #[serde(default)]
    pub source: ResourceReservationSource,
}

const fn default_concurrency_units() -> u32 {
    1
}

impl Default for ResourceReservation {
    fn default() -> Self {
        Self {
            memory_bytes: 0,
            disk_growth_bytes: 0,
            concurrency_units: default_concurrency_units(),
            source: ResourceReservationSource::Unmeasured,
        }
    }
}

impl ResourceReservation {
    /// Concurrency demand for a command that runs its own machine-wide job
    /// server and will fan out across every core it finds.
    ///
    /// A submitter cannot know which executor will be chosen, so whole-machine
    /// demand is declared as saturation and clamped to the selected executor's
    /// advertised capacity once one exists. The clamp is what keeps such a
    /// declaration admissible at all: a reservation larger than the host's
    /// budget can never fit, so an unclamped saturating demand would queue until
    /// its deadline instead of running.
    pub const WHOLE_MACHINE_CONCURRENCY: u32 = u32::MAX;
}

/// What a project needs of a machine before that machine can prepare a cell
/// for it, independent of what any one batch asks for.
///
/// Every cell runs the project's setup commands before its first command, so
/// a batch that selects nothing is still only runnable where those commands
/// can run. Both halves are derived on the runner
/// ([`crate::toolchain_requirements::project_placement_requirements`]) and
/// matched against the machine's attach-time advertisement.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ProjectPlacementRequirements {
    /// Toolchains the setup commands invoke, in the advertised vocabulary.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub setup_toolchains: Vec<String>,
    /// The build-prerequisite scripts (repository paths) the project's
    /// checkout declares. A machine's prerequisite verdict speaks for this
    /// project only when the script that produced it is one of these.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub prerequisite_scripts: Vec<String>,
    /// A digest of the setup commands these were derived from. A preparation
    /// refusal recorded under one setup applies only while the project still
    /// runs that setup. Empty when the request runs no setup at all.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub setup_identity: String,
}

impl ProjectPlacementRequirements {
    /// For `skip_serializing_if` on the boxed field: boxed so a request, which
    /// deep async placement code copies several times over, carries one
    /// pointer for this rather than three collections.
    #[allow(clippy::borrowed_box)]
    fn is_empty_boxed(requirements: &Box<Self>) -> bool {
        requirements.is_empty()
    }

    pub fn is_empty(&self) -> bool {
        self.setup_toolchains.is_empty()
            && self.prerequisite_scripts.is_empty()
            && self.setup_identity.is_empty()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CellRequest {
    pub request_id: String,
    pub attempt_id: String,
    pub project_id: String,
    pub repository: RepositoryLocator,
    pub base_commit: String,
    pub command: String,
    #[serde(default)]
    pub command_class: CellCommandClass,
    /// Why this work is running, used by the active operator placement profile.
    /// Older peers omit it and decode conservatively as an agent session.
    #[serde(default)]
    pub placement_work_class: PlacementWorkClass,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<CellOwnerRef>,
    #[serde(default)]
    pub cwd: String,
    #[serde(default)]
    pub env: Vec<(String, String)>,
    pub priority: CellPriority,
    /// The instant past which nobody is waiting for this request.
    ///
    /// The requester's own answer to "when does this result stop being wanted",
    /// not a queue budget and not an attempt bound. The executor holds a queued
    /// entry until this instant and then answers with the substrate evidence it
    /// collected; it does not evict merely because the machine stayed busy.
    ///
    /// Computed once per request and carried unchanged across any
    /// re-presentation. Refreshing it per attempt would reintroduce exactly the
    /// drift between what a caller will wait and what the executor honours that
    /// this field exists to remove.
    pub wait_horizon_unix_ms: u64,
    /// When the requester began waiting, which is not when this enqueue
    /// happened.
    ///
    /// Seniority is a property of the wait, not of the entry. The executor ages
    /// priority from this and ranks equal tiers by it, so a request that reaches
    /// the queue a second time keeps the seniority its wait already earned
    /// instead of starting again at the tail.
    ///
    /// Zero from a requester that does not state it, which is read as "now".
    #[serde(default)]
    pub waiting_since_unix_ms: u64,
    pub timeout_ms: u64,
    pub mutation_policy: MutationPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requesting_job_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub affinity_key: Option<String>,
    /// What the requester asked for, in the public placement vocabulary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executor: Option<ExecutorSelector>,
    /// Whether placement policy may choose among machines for this request.
    ///
    /// Deliberately separate from `executor` and `pinned_executor_id`: absence
    /// of a selector is not permission to move. See [`PlacementMobility`].
    #[serde(default)]
    pub placement_mobility: PlacementMobility,
    /// The platform families whose answer to this request the requester counts,
    /// as lowercase OS names (`macos`, `linux`, `windows`). Empty means the
    /// requester counts an answer from anywhere.
    ///
    /// Placement policy may only CHOOSE a machine on one of these platforms.
    /// This is not a capability question — a windows machine can run `eslint`
    /// and `clippy` perfectly well — it is a question about whose result the
    /// requester is willing to publish as the answer. A check suite whose gate
    /// has only ever meant "green on macOS" gets a windows-only finding
    /// (`#[cfg(windows)]` dead code, a backslash in a lint path) recorded as ITS
    /// verdict the moment idleness sends it there, and one placement decision
    /// becomes an operator reading "all checks are failing" about a tree that is
    /// green everywhere the project actually gates.
    ///
    /// Deliberately separate from `executor`: a selector states where the work
    /// must go, and this states which answers count. The two are set by
    /// different parties for different reasons, and collapsing them would make a
    /// project's gate policy indistinguishable from a caller's routing request
    /// in every diagnostic that mentions either.
    ///
    /// Nothing on the executor side reads this. It is a runner-side placement
    /// input, resolved before dispatch, and it travels on the request only
    /// because the request is where everything else the requester asked for
    /// lives.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub verdict_platforms: Vec<String>,
    /// Machines this request must NOT land on, because a check it carries is
    /// infrastructure-suppressed on that machine's current environment.
    ///
    /// Steering work away from a broken machine is not the same as refusing it:
    /// a suite whose toolchain is missing on one host may be perfectly healthy
    /// on the next, and the honest answer is a verdict from the healthy one
    /// rather than a diagnostic from the broken one. Placement resolves this as
    /// a hard clause with no fallback — an override that reran an item on a
    /// bounded environment would make suppression advisory, which is the one
    /// thing a kill switch may not be.
    ///
    /// Like [`Self::verdict_platforms`], nothing on the executor side reads
    /// this. It is a runner-side placement input, resolved before dispatch, and
    /// it travels on the request only because the request is where everything
    /// else the requester asked for lives.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub avoided_executor_ids: Vec<String>,
    /// What the project's own cell preparation needs from whichever machine
    /// hosts this work, whatever the work itself asked for.
    ///
    /// Stamped by the runner from the project's canonical execution policy
    /// before placement, and read only by placement: like
    /// [`Self::avoided_executor_ids`], nothing on the executor side reads it,
    /// and an older executor ignores the key.
    #[serde(
        default,
        skip_serializing_if = "ProjectPlacementRequirements::is_empty_boxed"
    )]
    pub project_requirements: Box<ProjectPlacementRequirements>,
    /// Where this batch must run because its working tree already lives there.
    ///
    /// Not a selector and never settable by a requester: a job's execution home
    /// is a leased cell on one machine, and the runner states that fact by the
    /// connection's internal identity rather than by a public name, which a
    /// rename or a dropped link could make unresolvable at exactly the moment
    /// the pin matters most.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pinned_executor_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command_resource_identity: Option<CommandResourceIdentity>,
    #[serde(default)]
    pub resource_reservation: ResourceReservation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub learned_estimate: Option<LearnedResourceEstimate>,
}

/// What one machine is told about one queue entry, so it can answer from live
/// facts.
///
/// Deliberately not the request. An offer confers nothing, so it carries only
/// what admission actually reads: how much the work reserves, whether it needs
/// provisioning and against which checkout, and how long the requester will
/// wait. The batch, the environment, and the object-plane grant ride
/// [`ExecutorMessage::Granted`], which is the message that binds.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct OfferSummary {
    pub request_id: String,
    pub attempt_id: String,
    pub project_id: String,
    /// Whether provisioning is needed at all, and against which checkout.
    pub repository: RepositoryLocator,
    /// Cache-distance input: how far this machine's warm trees stand from the
    /// commit the work wants.
    pub base_commit: String,
    pub reservation: ResourceReservation,
    pub admission_kind: CellAdmissionKind,
    /// Whether someone is blocked on this. Interactive work is admitted against
    /// a reserve that batch work may not touch.
    pub interactive: bool,
    pub priority: CellPriority,
    pub waiting_since_unix_ms: u64,
    pub wait_horizon_unix_ms: u64,
    /// The runner's own prediction of what this costs here, sent so the machine
    /// can confirm with the same function rather than invent a second one.
    /// Carried and recorded today; consulting it is a later change.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub predicted_cost_ms: Option<u64>,
}

/// Why a machine would not take work it was offered.
///
/// The point of every variant is that it routes differently. [`Self::Capacity`]
/// and [`Self::Pressure`] say "not here, not now" and the entry belongs on
/// another machine or on this one after its next posted state. [`Self::Disk`]
/// says the same and additionally means reclamation is worth starting.
/// [`Self::Constraint`] says "never here", which is a runner-side bug: it
/// offered work to a machine that could not have taken it under any conditions.
///
/// A refusal with no reason attached is what made a parked placement
/// indistinguishable from a wedged one, so the enum has no "other" that carries
/// meaning — [`Self::Unknown`] exists for a decline whose evidence the executor
/// genuinely could not classify, and is worth reading as a gap in this list.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum DeclineReason {
    /// The reservation does not fit what remains of this machine's budget.
    Capacity,
    /// A host-pressure floor would be crossed — memory above all.
    Pressure,
    /// The free-disk watermark refuses the growth this request declared. The
    /// typed [`DiskDecline`] that rides beside the reason names the floor and
    /// whether reclamation can still relieve it.
    Disk,
    /// Selector, platform, toolchain, or project mismatch. This machine could
    /// never take this work, whatever its load.
    Constraint,
    /// The refusal is real and its cause did not classify.
    Unknown,
    /// This project is already provisioning a cell here, and a project
    /// provisions one cell at a time. A transient serialisation rather than a
    /// shortage: it routes exactly like [`Self::Capacity`] (not here, not now),
    /// and the detail names the request that holds the project and for how
    /// long. Kept apart from `Capacity` because a hold that never clears is a
    /// leak, and under that label it read as a machine that was merely full.
    ProjectProvisioning,
}

/// Why a disk refusal will or will not clear on its own.
///
/// The question a caller holding a [`DeclineReason::Disk`] refusal actually
/// has is whether waiting can help. A sweep still running may free the bytes;
/// a pressure pass that already ran to completion without meeting its target
/// will not, and parking work behind it is a wait for nothing.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(
    tag = "state",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum DiskReliefOutlook {
    /// A storage sweep is running now and may free the bytes.
    InFlight,
    /// The last pressure pass reached its target. This refusal is new growth
    /// since then, and the next pass is expected to relieve it.
    Relieved,
    /// The last pressure pass ran every rung policy permits and fell short.
    /// `heldBytes` is what Cairn still holds that it may not reclaim now
    /// (quarantine trees inside their forensic window, cells fenced behind
    /// surviving processes, managed caches in use or under the pressure
    /// floor); the rest of the volume is outside Cairn's accounting.
    Unrelieved { held_bytes: u64 },
    /// The storage sweep failed and admission is latched until it succeeds.
    CleanupFailed,
    /// No pressure pass has run since this executor started.
    NotAttempted,
    /// The last pressure pass yielded to arriving work before it finished;
    /// the next pass continues it, so waiting may help.
    Deferred,
}

/// The typed half of a disk refusal: the reading, the floor it was compared
/// against, and whether reclamation can still relieve it.
///
/// Carried beside the reason rather than inside the detail string, so routing
/// and rendering read a fact instead of parsing a sentence.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DiskDecline {
    pub free_bytes: u64,
    pub floor_bytes: u64,
    pub outlook: DiskReliefOutlook,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ObjectChannelCredential {
    pub base_url: String,
    pub bearer_token: String,
    pub expires_at_unix_ms: u64,
}

/// Where the runner's executor listener can be reached, and which certificate
/// identities are valid there.
///
/// A resident executor learns this once during its SSH bootstrap and writes it
/// into its config file. Carrying it on the live link as well is what lets the
/// pair change without another bootstrap: the machine hears about a new endpoint
/// or a rotating certificate over a connection it already trusts, rather than
/// needing someone to log in and edit a file.
///
/// Both lists are ordered by preference and may hold more than one entry, which
/// is the whole point during a rotation: the old and new identities are valid at
/// once, so no machine is stranded by the moment the runner switches.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ExecutorListenerAdvertisement {
    /// Dialable endpoints, e.g. `wss://host:3850/api/executor`.
    pub endpoints: Vec<String>,
    /// Lowercase hex SHA-256 of each acceptable certificate's DER encoding, as
    /// [`certificate_fingerprint`] computes it.
    pub cert_sha256: Vec<String>,
}

/// The name one certificate identity goes by on both sides of the link.
///
/// Lowercase hex SHA-256 of the DER encoding. Defined once, here, because the
/// runner computes it to advertise and the executor computes it to decide
/// whether to keep talking: two implementations that disagreed by a byte would
/// produce a fleet that refuses its own runner, and nothing else in the system
/// would say why.
pub fn certificate_fingerprint(certificate_der: &[u8]) -> String {
    use sha2::{Digest, Sha256};

    let digest = Sha256::digest(certificate_der);
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

/// Where and how a resident executor reaches its runner.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OrchEndpoint {
    /// Dialable runner endpoints, tried in order. More than one is how a machine
    /// that can reach its runner by several routes survives losing any of them.
    pub endpoints: Vec<String>,
    /// Certificate fingerprints (lowercase hex SHA-256 of DER) this executor
    /// will accept from the runner. More than one during a rotation.
    #[serde(default)]
    pub cert_sha256: Vec<String>,
    /// The runner's device id, checked against what `Ready` reports. Empty means
    /// this machine was not told, and does not check.
    #[serde(default)]
    pub runner_device_id: String,
}

/// A resident executor's whole configuration: the `executor.json` the SSH
/// bootstrap ceremony writes and the daemon reads on every start.
///
/// One type for both sides of that file, owned here because neither side may
/// own it alone. The ceremony serializes this struct and the executor parses it
/// with [`ResidentExecutorConfig::parse`]; when the ceremony instead built its document
/// by hand, it wrote a key the executor's `deny_unknown_fields` refused, and
/// every machine it bootstrapped exited 78 on every start.
///
/// Unknown keys stay refused rather than ignored: this file names the runner
/// the machine hands its credential to, and a key the executor does not
/// understand is an instruction it cannot honour.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ResidentExecutorConfig {
    pub orch: OrchEndpoint,
    /// The one-time enrollment grant the ceremony issued, present only between
    /// a bootstrap and the first enrollment it opens.
    ///
    /// A service manager starts this daemon with no environment worth trusting,
    /// so the grant travels in the file rather than beside the process. The
    /// executor redeems it, saves the credential the runner issues, and then
    /// removes it from this file. A stored credential always wins over a grant,
    /// which makes one left behind by a failed scrub inert: it is spent, and it
    /// is never presented again.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enrollment_grant: Option<String>,
    /// Stable identity assigned at enrollment. Absent falls back to the
    /// home-derived id, which is what a hand-run executor gets.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executor_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub device_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    /// Projects this machine serves. Empty means every project.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub project_ids: Vec<String>,
    /// The executor's Cairn home. Defaults to the directory holding this file,
    /// because the ceremony writes it into that home — which matters on Windows,
    /// where an autostart entry carries no environment at all.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cairn_home: Option<std::path::PathBuf>,
}

impl ResidentExecutorConfig {
    /// The one parser for `executor.json`: decode, then validate.
    pub fn parse(contents: &str) -> Result<Self, String> {
        let config: Self = serde_json::from_str(contents).map_err(|error| error.to_string())?;
        config.validate()?;
        Ok(config)
    }

    /// Refuse a configuration that would send this machine's credential
    /// somewhere it cannot verify.
    pub fn validate(&self) -> Result<(), String> {
        if self.orch.endpoints.iter().all(|e| e.trim().is_empty()) {
            return Err("executor config names no runner endpoint".to_string());
        }
        for endpoint in &self.orch.endpoints {
            let endpoint = endpoint.trim();
            if !endpoint.starts_with("wss://") && !endpoint.starts_with("ws://") {
                return Err(format!(
                    "runner endpoint must be a WebSocket URL: {endpoint}"
                ));
            }
        }
        // A plaintext endpoint with no pin is the shape this whole transport
        // exists to refuse: the machine could not tell its runner from anything
        // else that answered, and would hand over its credential to whatever
        // did. Loopback is the exception, because there is no network to be on.
        if self.orch.cert_sha256.is_empty()
            && self
                .orch
                .endpoints
                .iter()
                .any(|endpoint| !is_loopback_endpoint(endpoint))
        {
            return Err(
                "executor config pins no runner certificate but names a non-loopback endpoint"
                    .to_string(),
            );
        }
        Ok(())
    }

    /// The Cairn home this configuration describes.
    pub fn cairn_home(&self, config_path: &Path) -> std::path::PathBuf {
        self.cairn_home.clone().unwrap_or_else(|| {
            config_path
                .parent()
                .map(Path::to_path_buf)
                .unwrap_or_else(|| std::path::PathBuf::from("."))
        })
    }

    /// Endpoints, trimmed and in preference order.
    pub fn endpoints(&self) -> Vec<String> {
        self.orch
            .endpoints
            .iter()
            .map(|endpoint| endpoint.trim().to_string())
            .filter(|endpoint| !endpoint.is_empty())
            .collect()
    }
}

fn is_loopback_endpoint(endpoint: &str) -> bool {
    let authority = endpoint
        .split_once("://")
        .map(|(_, rest)| rest)
        .unwrap_or(endpoint);
    let authority = authority.split('/').next().unwrap_or_default();
    let host = if let Some(rest) = authority.strip_prefix('[') {
        rest.split(']').next().unwrap_or_default()
    } else {
        authority.split(':').next().unwrap_or_default()
    };
    host.eq_ignore_ascii_case("localhost") || host == "127.0.0.1" || host == "::1"
}

/// Why a resident executor last stopped on an error, as it recorded itself in
/// [`EXECUTOR_LAST_EXIT_FILE_NAME`]. Cleared each time it reaches its runner, so
/// a record that exists describes a daemon that has failed since it last
/// attached.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ResidentExecutorExit {
    pub at_unix_ms: u64,
    pub exit_code: u8,
    pub error: String,
}

/// A live process holding a Cairn home's executor generation lock.
///
/// The kernel releases that lock the moment its holder dies, so this is always
/// a live process. Which one comes from the record the holder stamps into the
/// lock file, which can be missing or unreadable; the fields are optional for
/// that reason and never because the holder might be dead.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ExecutorGenerationHolder {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recorded_ago_ms: Option<u64>,
}

/// What `cairn-exec service status --json` reports about a machine's resident
/// executor: the facts the bootstrap ceremony consults instead of assuming.
///
/// It carries no claim about whether the executor is connected. Presence is
/// asserted by the machine over its own link and read on the runner; this is
/// the machine's side of why that link may not exist.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ResidentExecutorStatus {
    /// The unit file or registry value exists.
    pub installed: bool,
    /// The service manager has it loaded.
    pub loaded: bool,
    /// An enrollment credential is stored in the executor's Cairn home.
    pub credential_present: bool,
    /// The config still carries an enrollment grant nobody has redeemed.
    pub grant_pending: bool,
    /// Why the config the service would start from does not load, when it
    /// does not.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_exit: Option<ResidentExecutorExit>,
    /// A live process holding this home's executor generation, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generation_holder: Option<ExecutorGenerationHolder>,
    /// The service manager's account of the process, where it keeps one. This
    /// is the evidence for a daemon that died before it could write
    /// `last_exit`: a panic, or a failure ahead of its own error handling.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_runtime: Option<crate::service::ServiceRuntime>,
    /// The last lines the service's process wrote to its error stream.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_log: Option<String>,
}

pub const CLOUD_OBJECT_GRANT_VERSION: u16 = 1;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum CloudObjectOperation {
    Get,
    Put,
}

/// A transient exact-object bearer grant. Callers must never persist or log `url`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CloudObjectGrant {
    pub version: u16,
    pub content_hash: String,
    pub operation: CloudObjectOperation,
    pub url: String,
    pub method: String,
    pub expires_at: String,
    #[serde(default)]
    pub headers: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CloudObjectGrantRequest {
    pub coordinate: ObjectTransferCoordinate,
    pub content_hash: String,
    pub operation: CloudObjectOperation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub byte_count: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ObjectTransferCoordinate {
    pub repository: RepositoryIdentity,
    pub request_id: String,
    pub attempt_id: String,
    pub executor_id: String,
    pub connection_generation: u64,
}

impl ObjectTransferCoordinate {
    pub fn matches_execution(
        &self,
        request: &CellRequest,
        executor_id: &str,
        connection_generation: u64,
    ) -> bool {
        self.repository == request.repository.identity()
            && self.request_id == request.request_id
            && self.attempt_id == request.attempt_id
            && self.executor_id == executor_id
            && self.connection_generation == connection_generation
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeltaUploadReceipt {
    pub receipt_id: String,
    pub coordinate: ObjectTransferCoordinate,
    pub base_commit: String,
    pub delta_commit: String,
    pub content_hash: String,
    pub pack_checksum: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MutationDelta {
    pub base_commit: String,
    pub delta_commit: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upload_receipt: Option<DeltaUploadReceipt>,
}

/// Tracked repository content written by a pure-verdict command and discarded
/// before the command result is published.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TrackedModificationEvidence {
    pub paths: Vec<String>,
    pub files_changed: usize,
    pub lines_added: u64,
    pub lines_deleted: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CellExecutionMeta {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub executor_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub executor_device_id: String,
    #[serde(default)]
    pub executor_connection_generation: u64,
    #[serde(rename = "slotId")]
    pub cell_id: String,
    pub cell_epoch: u64,
    pub started_at_unix_ms: u64,
    pub finished_at_unix_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peak_rss_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peak_physical_footprint_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk_delta_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub measurement_quality: Option<ExecutionMeasurementQuality>,
    /// What the cell already held when this command started, as the executor
    /// observed it rather than as the runner predicted it.
    ///
    /// The learner keys a duration observation on this, so a warm run can never
    /// be recorded as evidence about a cold one. `None` from an executor that
    /// does not state it, which is read as "no claim" and keeps the observation
    /// out of every warmth-keyed profile.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub warmth: Option<ExecutionWarmth>,
    /// The machine load observed immediately before this command started.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub load_context: Option<ExecutionLoadContext>,
    /// The width this command was granted and how it was applied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allocation: Option<ExecutionAllocation>,
    /// CPU time consumed by the command's process group, carrying its own
    /// measurement quality. `None` where nothing read it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu: Option<CpuTime>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub environment_fingerprint: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verdict_platform: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verdict_arch: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub toolchain_fingerprint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verdict_environment_hash: Option<String>,
    /// Sandbox marker state and measured kernel capability at execution time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sandbox: Option<SandboxCapabilityEvidence>,
    /// Edge observations for the execution seam actually used.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub containment: Option<ContainmentObservations>,
}

/// The width a command was granted and how that grant was applied on the
/// executor that ran it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionAllocation {
    /// Declared by the requester, never learned or presented as a measurement.
    pub granted_units: u32,
    pub enforcement: AllocationEnforcement,
    /// CPU time over the command's monotonic wall time, in thousandths of a
    /// core. `None` when no CPU was read or the interval was zero.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivered_units_millis: Option<u32>,
}

/// How a grant was applied. No variant claims a kernel-enforced cap: without a
/// cap written and read back, a grant only advises the command's own
/// parallelism knobs, and the label says whether CPU was measured against it.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum AllocationEnforcement {
    /// No hard cap; CPU time was measured instead.
    Measured { reason: NoCapReason },
    /// Neither a hard cap nor a CPU reading.
    Advisory { reason: NoCapReason },
}

/// Why a grant was not held by a kernel cap.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum NoCapReason {
    /// The platform has no hard CPU cap an unprivileged executor can apply.
    PlatformHasNoCap,
    /// The platform has a hard cap (cgroup v2 `cpu.max`, a job-object CPU
    /// rate) that this executor does not apply.
    CapNotImplemented,
}

impl ExecutionAllocation {
    /// A grant applied without a hard cap: `Measured` when CPU was read and
    /// `Advisory` when it was not, so the label never implies a reading.
    pub fn uncapped(
        granted_units: u32,
        reason: NoCapReason,
        cpu: Option<CpuTime>,
        wall_ms: u64,
    ) -> Self {
        Self {
            granted_units,
            enforcement: match cpu {
                Some(_) => AllocationEnforcement::Measured { reason },
                None => AllocationEnforcement::Advisory { reason },
            },
            delivered_units_millis: cpu.and_then(|cpu| cpu.delivered_units_millis(wall_ms)),
        }
    }

    /// The same application, observed over one part of the command, under that
    /// part's own grant where it stated one.
    pub fn reobserved(
        &self,
        granted_units: Option<u32>,
        cpu: Option<CpuTime>,
        wall_ms: u64,
    ) -> Self {
        let (AllocationEnforcement::Measured { reason }
        | AllocationEnforcement::Advisory { reason }) = self.enforcement;
        Self::uncapped(
            granted_units.unwrap_or(self.granted_units),
            reason,
            cpu,
            wall_ms,
        )
    }
}

/// CPU provenance travels with a reading; neither a default nor a quality-only
/// constructor can claim that measurement happened.
///
/// ```compile_fail
/// use cairn_common::executor_protocol::CpuTime;
/// let cpu = CpuTime::default();
/// ```
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CpuTime {
    cpu_ms: u64,
    #[serde(deserialize_with = "deserialize_cpu_quality")]
    quality: MeasurementQuality,
}

fn deserialize_cpu_quality<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<MeasurementQuality, D::Error> {
    match MeasurementQuality::deserialize(deserializer)? {
        quality @ (MeasurementQuality::Authoritative | MeasurementQuality::Sampled) => Ok(quality),
        _ => Err(serde::de::Error::custom(
            "CPU time requires authoritative or sampled accounting",
        )),
    }
}

impl CpuTime {
    /// Kernel accounting for a whole group, including exited children.
    pub fn authoritative(cpu_ms: u64) -> Self {
        Self {
            cpu_ms,
            quality: MeasurementQuality::Authoritative,
        }
    }

    /// A live-process census misses processes born and dead between samples,
    /// and work after the final sighting of a process. It undercounts by an
    /// unmeasured amount; it is not kernel accounting of exited children.
    pub fn sampled(cpu_ms: u64) -> Self {
        Self {
            cpu_ms,
            quality: MeasurementQuality::Sampled,
        }
    }

    pub fn cpu_ms(self) -> u64 {
        self.cpu_ms
    }

    pub fn quality(self) -> MeasurementQuality {
        self.quality
    }

    /// Total work across the parts of one command. CPU is additive, unlike a
    /// peak-memory high-water mark, and a total is only as authoritative as its
    /// least authoritative part.
    pub fn total(readings: impl IntoIterator<Item = CpuTime>) -> Option<Self> {
        readings.into_iter().reduce(|total, reading| Self {
            cpu_ms: total.cpu_ms.saturating_add(reading.cpu_ms),
            quality: if total.quality == MeasurementQuality::Authoritative
                && reading.quality == MeasurementQuality::Authoritative
            {
                MeasurementQuality::Authoritative
            } else {
                MeasurementQuality::Sampled
            },
        })
    }

    /// Thousandths of a core, using the same monotonic interval as the reading.
    /// A zero interval has no defined ratio and is never reported as zero work.
    pub fn delivered_units_millis(self, wall_ms: u64) -> Option<u32> {
        let value = (u128::from(self.cpu_ms) * 1_000).checked_div(u128::from(wall_ms))?;
        Some(value.min(u128::from(u32::MAX)) as u32)
    }
}

impl CellExecutionMeta {
    /// This command's metadata narrowed to one batch item: the item's own
    /// timing, memory, disk, environment, and CPU, with the command's grant
    /// re-observed over the item's interval. Every consumer of per-item
    /// outcomes derives item metadata here, so no axis is narrowed in one place
    /// and forgotten in another.
    pub fn for_batch_item(&self, item: &ProcessBatchItemOutcome) -> Self {
        let mut meta = self.clone();
        meta.started_at_unix_ms = item.started_at_unix_ms;
        meta.finished_at_unix_ms = item.finished_at_unix_ms;
        meta.duration_ms = Some(item.duration_ms);
        meta.peak_rss_bytes = item.peak_rss_bytes;
        meta.disk_delta_bytes = item.disk_delta_bytes;
        meta.environment_fingerprint = item.environment_fingerprint.clone();
        meta.cpu = item.cpu;
        meta.allocation = self.allocation.as_ref().map(|allocation| {
            allocation.reobserved(item.granted_units, item.cpu, item.duration_ms)
        });
        meta
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionMeasurementQuality {
    pub duration: MeasurementQuality,
    pub memory: MeasurementQuality,
    pub disk: MeasurementQuality,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_platform: Option<String>,
    pub disk_boundary: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum MeasurementQuality {
    Authoritative,
    Sampled,
    Approximate,
    Unavailable,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ObjectInfrastructureStage {
    FetchInterrupted,
    IntegrityFailure,
    IncompleteClosure,
    InstallFailure,
    UploadFailure,
    ExpiredReceipt,
    StaleReceipt,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum AdmissionRejectionReason {
    QueueFull,
    RequestTooLarge,
    StorageCleanupFailed,
    Draining,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum StorageFailureStage {
    PreAdmissionPressure,
    #[serde(rename = "provisioningMaterialization")]
    ProvisioningCheckout,
    StatePersistence,
    CommandSeal,
    DeltaUpload,
    Recovery,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum StorageFailureKind {
    NoSpace,
    QuotaExceeded,
    CleanupFailed,
    AccountingUnavailable,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum HostPressureCondition {
    MemoryAvailable {
        available_bytes: u64,
        floor_bytes: u64,
        /// How much of `floor_bytes` is held for resident work whose demand has
        /// never been measured, rather than asked for by the arriving request.
        ///
        /// Without this the floor is a number with no derivation: an operator
        /// reading "floor 112 GiB" against a request that asked for 16 cannot
        /// tell a policy from a bug. Defaulted so an executor that predates the
        /// hold still deserializes.
        #[serde(default)]
        unmeasured_hold_bytes: u64,
    },
    DiskFree {
        free_bytes: u64,
        floor_bytes: u64,
    },
    ResidentOccupancy {
        process_count: usize,
        reservation: ResourceReservation,
    },
    CpuUtilization {
        utilization: f64,
        entry_utilization: f64,
        clear_utilization: f64,
        state_since_unix_ms: u64,
        measured_at_unix_ms: u64,
    },
    /// A reading admission decides on has stopped moving on a machine that was
    /// producing it. Named by the same [`MachineMeasurement`] the runner's
    /// placement seam reports blindness with, so the two ends of the fleet
    /// describe the same failure in the same words.
    TelemetryBlind {
        measurement: MachineMeasurement,
        measured_at_unix_ms: u64,
        age_ms: u64,
        blind_after_ms: u64,
    },
}

// This protocol is JSON-only, and JSON cannot represent non-finite numbers.
// Consequently the numeric CPU variant cannot contain NaN, the sole value that
// would violate f64's reflexive equality requirement.
impl Eq for HostPressureCondition {}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct HostPressureEvidence {
    pub conditions: Vec<HostPressureCondition>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ExecutorSubstrateState {
    SupervisorSpawning,
    SupervisorRespawning,
    /// The child declared that this runner/executor binary pairing cannot
    /// attach. Supervision remains dormant until the executable is replaced.
    PermanentRefusal,
    ProtocolAttaching,
    InitialStorageSweep,
    StorageAccounting,
    DispatchPreparing,
    SlotAdoption,
    CapacityBusy,
    ExecutionRunning,
    ConnectedStalled,
    Draining,
    /// Another live process holds this home's executor generation, so the
    /// supervisor is deliberately not starting a successor. Not a recovery in
    /// progress: it ends when that process exits, and the diagnostic names it.
    GenerationHeld,
}

/// What the runner knows about supervising its colocated executor across
/// attempts.
///
/// Kept beside the substrate declaration rather than inside it, because a
/// declaration is replaced on every attempt and cleared on readiness, while
/// history is the whole point of this record: how long the successor has been
/// failing, and how the last handoff went even after it succeeded.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ColocatedSupervision {
    /// When the current run of failed starts began. `None` once a start
    /// reached readiness.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failing_since_unix_ms: Option<u64>,
    /// Starts that failed since `failing_since_unix_ms`.
    #[serde(default)]
    pub failed_attempts: u32,
    /// Why the most recent start failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_cause: Option<String>,
    /// Set while the supervisor is waiting on a process that holds the
    /// generation lock.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generation_hold: Option<ColocatedGenerationHold>,
    /// The most recent retirement of an outgoing colocated executor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_retirement: Option<ExecutorRetirementSummary>,
    /// Workload processes a retired executor launched that outlived the
    /// retirement's SIGKILL, still being re-verified.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub workload_survivors: Vec<WorkloadSurvivor>,
    /// Cells a retirement could not read, whose workload is still unknown.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unresolved_cells: Vec<UnresolvedCell>,
}

/// A process a retired executor recorded launching that is not yet known to be
/// gone.
///
/// Kept until the kernel says it is gone (or its pid names another process
/// now), or until the cell's current executor generation records it as its
/// own. Retrying is not death, so nothing else removes it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct WorkloadSurvivor {
    pub pid: u32,
    pub start_token: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub leads_group: Option<u32>,
    pub kind: crate::executor_generation::WorkloadKind,
    /// The cell authority directory it was recorded in.
    pub cell_authority: String,
    /// That cell's epoch when the retirement read it.
    pub cell_epoch: u64,
    /// Whether a live process still held the cell's lock at the last look.
    #[serde(default)]
    pub cell_lock_held: bool,
    pub first_seen_unix_ms: u64,
    pub last_verified_unix_ms: u64,
    pub next_verification_unix_ms: u64,
    /// Verified SIGKILLs delivered to it so far.
    #[serde(default)]
    pub kill_attempts: u32,
    /// Why it was not signalled at the last look, when it was not.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unsignalled_because: Option<String>,
    /// Members of the group it leads, seen while it was verified alive: what
    /// lets that group be signalled once the leader itself is gone.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub group_anchors: Vec<crate::process_identity::ProcessAnchor>,
    /// The executor whose retirement found it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retired_executor_pid: Option<u32>,
    /// Set when it was recorded without knowing whose cells it ran in (the
    /// generation could not be probed), and why. It is not signalled until the
    /// generation is found free.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ownership_unconfirmed: Option<String>,
    /// Whether a successor's start has already been held on it. Each survivor
    /// holds one start, a bounded number of looks long, and never another.
    #[serde(default)]
    pub successor_gated: bool,
}

impl WorkloadSurvivor {
    /// The recorded identity this survivor names.
    pub fn recorded(&self) -> crate::executor_generation::RecordedWorkloadProcess {
        crate::executor_generation::RecordedWorkloadProcess {
            pid: self.pid,
            start_token: self.start_token.clone(),
            leads_group: self.leads_group,
            kind: self.kind,
            cell_authority: std::path::PathBuf::from(&self.cell_authority),
            cell_epoch: self.cell_epoch,
            group_anchors: self.group_anchors.clone(),
        }
    }
}

/// A cell whose workload a retirement could not learn, because its state file
/// did not decode.
///
/// Unknown is not none, and nothing later written at that path is the record
/// that could not be read: a successor quarantines such a cell, moving it aside
/// or rebuilding its state from the cell's identity. So the entry is kept until
/// the host has restarted since it was recorded, which ends every process
/// that could have been running there, or until an operator removes it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct UnresolvedCell {
    pub cell_authority: String,
    /// Why its workload is still unknown, as of the last look.
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retired_executor_pid: Option<u32>,
    /// The kernel's boot identity when it was recorded, where the platform
    /// has one ([`crate::process_identity::boot_id`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub boot_id: Option<String>,
    pub first_seen_unix_ms: u64,
    pub last_verified_unix_ms: u64,
    pub next_verification_unix_ms: u64,
}

/// A live process the supervisor found holding the generation lock it needs.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ColocatedGenerationHold {
    /// The pid the holder stamped into the lock file, when it is legible.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub holder_pid: Option<u32>,
    /// When the holder recorded taking the lock.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub holder_acquired_at_unix_ms: Option<u64>,
    /// When this supervisor first found it held.
    pub observed_since_unix_ms: u64,
    pub reason: GenerationHoldReason,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum GenerationHoldReason {
    /// Nothing this runner recorded identifies the holder, so it is waited
    /// out and never signalled: a resident executor on the same home, or one
    /// left behind by a runner build that kept no record.
    Unidentified,
    /// The runner retired the holder and it did not go.
    RetirementStuck { phase: RetirementPhase },
}

/// The steps of retiring an outgoing executor, in order.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum RetirementPhase {
    /// Asking it to stop: a protocol `Shutdown` when it is attached, then
    /// SIGTERM.
    Request,
    /// Waiting for it to exit on its own.
    Graceful,
    /// SIGKILL to its process group, then waiting for the kernel to reap it.
    Force,
    /// Confirming the pid is gone, and that every process it recorded
    /// launching is gone and every cell lock is free.
    Verify,
}

impl RetirementPhase {
    pub fn label(self) -> &'static str {
        match self {
            Self::Request => "request",
            Self::Graceful => "graceful wait",
            Self::Force => "SIGKILL",
            Self::Verify => "verification",
        }
    }
}

/// Whose executor a retirement ended.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum RetirementOrigin {
    /// The child this runner spawned.
    OwnChild,
    /// A child a previous runner spawned and recorded, which outlived it.
    PriorRunner,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum RetirementOutcome {
    /// The process is gone and the generation lock is free.
    Retired,
    /// The process outlived the phase named.
    Stuck { phase: RetirementPhase },
}

/// One retirement of an outgoing colocated executor, as it is reported.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ExecutorRetirementSummary {
    pub pid: u32,
    pub origin: RetirementOrigin,
    /// `Some(Force)` when the graceful window ran out and SIGKILL was needed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub escalated_to: Option<RetirementPhase>,
    /// How long the process was given to exit on its own.
    pub graceful_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub force_ms: Option<u64>,
    pub total_ms: u64,
    pub outcome: RetirementOutcome,
    pub completed_at_unix_ms: u64,
    /// What the verify step found of the workload the executor recorded.
    #[serde(default)]
    pub workload: RetirementWorkload,
}

/// The workload half of a retirement's verification.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RetirementWorkload {
    /// Recorded workload processes still running that were SIGKILLed.
    #[serde(default)]
    pub reaped: u32,
    /// Recorded processes not known to be gone when the deadline passed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub surviving_pids: Vec<u32>,
    /// Cell locks a live process still held at the end.
    #[serde(default)]
    pub held_cell_locks: u32,
    /// Cells whose state could not be decoded, so whose workload is unknown.
    #[serde(default)]
    pub unreadable_cells: u32,
    /// Why the workload was left alone, when it was: a live executor other
    /// than the one retired holds the generation, so every cell in the home is
    /// that executor's. That is positive evidence, and a clean outcome.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub left_alone_because: Option<String>,
    /// Why whose cells these are could not be established, when it could not
    /// (the generation could not be probed). Nothing was signalled, and the
    /// outcome is not clean.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ownership_unconfirmed: Option<String>,
}

impl RetirementWorkload {
    /// Every recorded process is known gone, every cell lock is free, and
    /// nothing about the workload is unknown.
    pub fn is_clean(&self) -> bool {
        self.surviving_pids.is_empty()
            && self.held_cell_locks == 0
            && self.unreadable_cells == 0
            && self.ownership_unconfirmed.is_none()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ExecutorSubstrateEvidence {
    pub state: ExecutorSubstrateState,
    pub since_unix_ms: u64,
    pub last_progress_unix_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostic: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_depth: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_position: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "activeSlotCount")]
    pub active_cell_count: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oldest_running_started_at_unix_ms: Option<u64>,
}

impl ExecutorSubstrateEvidence {
    pub fn without_queue(
        state: ExecutorSubstrateState,
        since_unix_ms: u64,
        last_progress_unix_ms: u64,
    ) -> Self {
        Self {
            state,
            since_unix_ms,
            last_progress_unix_ms,
            diagnostic: None,
            queue_depth: None,
            queue_position: None,
            active_cell_count: None,
            oldest_running_started_at_unix_ms: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum CellUnavailableReason {
    Deadline {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        host_pressure: Option<HostPressureEvidence>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        substrate: Option<ExecutorSubstrateEvidence>,
    },
    Provisioning,
    Checkout,
    Spawn,
    /// The runner could not authenticate or reconstruct the originating run
    /// context required to execute this cell.
    RunnerContext,
    Preparation,
    /// A project setup command exited "command not found" (POSIX 127,
    /// `cmd.exe` 9009) because `program`, a host toolchain named bare, does not
    /// resolve on the executor's own PATH, and the setup does not compose its
    /// own PATH. Only then is it a fact about the machine rather than the
    /// branch: a missing branch-owned script, or a 127 propagated from inside
    /// a script, stays `Preparation`. The runner records this one against
    /// (machine, project, setup).
    PreparationCommandNotFound {
        command: String,
        program: String,
        exit_code: i32,
    },
    /// The slot could not be made fit for this batch, so the executor retired
    /// it. Distinct from `Preparation` because of what it licenses rather than
    /// what failed: nothing of the batch ran, and the slot that could not take
    /// it is out of the pool, so presenting the work again places it on a
    /// different slot — and, for a batch free to move, possibly a different
    /// machine. `Preparation` names a fault that the next attempt would meet
    /// again; this one names a fault the next attempt cannot meet.
    SlotUnhealthy,
    ExecutorUnavailable,
    NoMatchingExecutor,
    AdmissionRejected {
        reason: AdmissionRejectionReason,
    },
    ObjectInfrastructure(ObjectInfrastructureStage),
    /// Pinned to its job's execution home, and that machine can never hold
    /// this work's measured memory demand while another usable machine can.
    /// Refused at once rather than queued, because the home would only run it
    /// into its own memory limit. `alternative_executor_id` is where placement
    /// would put it with the pin lifted, so the caller can run it there from
    /// the branch head and leave the environment where it is.
    ///
    /// Emitted by the runner's placement alone; no executor produces or reads
    /// it.
    HomeCannotHold {
        home: String,
        ceiling: MemoryCeiling,
        alternative_executor_id: String,
        alternative_executor_name: String,
    },
    /// The runner stopped waiting for an attempt it had granted, because the
    /// machine holding it gave no account of it for `quiet_ms`: its link was
    /// down or silent, or its snapshots stopped listing the attempt.
    ///
    /// Never a duration budget. A claimed attempt the machine keeps listing as
    /// queued, held, or executing is waited on for as long as it takes, so
    /// this ends only work the runner has evidence it is no longer hearing
    /// about. Whether any of its commands ran is unknown.
    ///
    /// Emitted by the runner alone; no executor produces or reads it.
    ClaimedAttemptSilent {
        executor_name: String,
        quiet_ms: u64,
        /// The last account that counted as progress, or `None` when the
        /// machine never listed the attempt after it was granted.
        last_progress: Option<ClaimedAttemptAccount>,
        /// What the runner read when it stopped waiting.
        final_account: ClaimedAttemptAccount,
    },
}

/// What the machine holding a claimed attempt says about it, read by the runner
/// from that machine's link and latest snapshot.
///
/// The first three are progress: the machine is connected, reporting, and
/// still accounts for the attempt. The rest are its absence, distinguished
/// because they send a reader to different places.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ClaimedAttemptAccount {
    /// Listed among the machine's queued requests.
    Queued,
    /// Granted and held with no command running: acquiring or preparing its
    /// cell, materializing its checkout, or between two batch items.
    Held,
    /// A command of the attempt is running.
    Executing,
    /// The machine lists the attempt as completed, and its result never
    /// reached the runner.
    CompletedWithoutResult,
    /// The machine's link is live and its snapshot does not mention the
    /// attempt.
    NotListed,
    /// The machine's connection is attached but has reported nothing for
    /// longer than [`EXECUTOR_PROGRESS_FRESHNESS_MS`].
    LinkSilent,
    /// No connection to the machine is attached at the generation holding
    /// the claim.
    LinkDown,
}

impl ClaimedAttemptAccount {
    /// Whether this account is the machine still answering for the attempt.
    pub fn is_progress(self) -> bool {
        matches!(self, Self::Queued | Self::Held | Self::Executing)
    }

    /// The account as a clause: "the machine …".
    pub fn describe(self) -> &'static str {
        match self {
            Self::Queued => "listed it as queued",
            Self::Held => {
                "held it with no command running (preparing its cell or checkout, or between batch items)"
            }
            Self::Executing => "listed it as running a command",
            Self::CompletedWithoutResult => {
                "listed it as completed, but its result never reached the runner"
            }
            Self::NotListed => "was connected and no longer listed it",
            Self::LinkSilent => "was connected but had stopped reporting",
            Self::LinkDown => "was not connected",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "camelCase")]
// This wire enum intentionally keeps completed metadata inline for protocol
// compatibility; additive optional measurements make that variant larger.
#[allow(clippy::large_enum_variant)]
pub enum CellOutcome {
    Completed {
        request_id: String,
        attempt_id: String,
        exit_code: Option<i32>,
        output: String,
        timed_out: bool,
        metadata: CellExecutionMeta,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        mutation_delta: Option<Box<MutationDelta>>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        sandbox_denials: Vec<SandboxDenialEvidence>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tracked_modifications: Option<TrackedModificationEvidence>,
    },
    Unavailable {
        reason: CellUnavailableReason,
        diagnostic: String,
    },
    FailedAfterExecution {
        request_id: String,
        attempt_id: String,
        diagnostic: String,
    },
    StorageFailure {
        request_id: String,
        attempt_id: String,
        stage: StorageFailureStage,
        kind: StorageFailureKind,
        diagnostic: String,
        #[serde(default)]
        slot_retired: bool,
    },
    Cancelled {
        request_id: String,
        attempt_id: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum PersistentCellLifecycle {
    Materializing,
    Idle,
    Active,
    Deleting,
    Quarantined,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ActiveCellRequest {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub executor_id: String,
    pub request_id: String,
    pub attempt_id: String,
    pub command: String,
    #[serde(default)]
    pub command_class: CellCommandClass,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<CellOwnerRef>,
    pub priority: CellPriority,
    pub requesting_job_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub affinity_key: Option<String>,
    pub queued_at_unix_ms: u64,
    pub started_at_unix_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stage: Option<CellExecutionStage>,
    #[serde(default)]
    pub resource_reservation: ResourceReservation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub learned_estimate: Option<LearnedResourceEstimate>,
    #[serde(default = "default_subscriber_count")]
    pub subscriber_count: usize,
}

fn default_subscriber_count() -> usize {
    1
}

/// Who holds a cell.
///
/// A holder keeps one cell for the whole of its life so that everything it runs
/// shares one checkout root, one installed dependency tree, and one `$TMPDIR`.
/// A holder is not work: it runs nothing by itself, reserves no concurrency, and
/// never appears in a user-facing surface. What runs is occupancy.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum ResidencyHolder {
    Service { service_id: String },
    Job { job_id: String },
    DevInstance { instance_id: String },
    ProjectTerminals { project_id: String },
    Workflow { run_id: String },
}

impl ResidencyHolder {
    /// Whether provisioning this residency has someone waiting on it. An
    /// interactive acquire is admitted ahead of batch work, because a person or
    /// an agent is blocked until its checkout exists. A workflow runtime is
    /// scheduled work and waits its turn.
    pub fn is_interactive(&self) -> bool {
        match self {
            Self::Service { .. }
            | Self::Job { .. }
            | Self::DevInstance { .. }
            | Self::ProjectTerminals { .. } => true,
            Self::Workflow { .. } => false,
        }
    }

    /// A compact, stable rendering of the holder for storage columns, lock keys,
    /// and log lines. Round-trips through `parse_storage_key`.
    pub fn storage_key(&self) -> String {
        match self {
            Self::Service { service_id } => format!("service:{service_id}"),
            Self::Job { job_id } => format!("job:{job_id}"),
            Self::DevInstance { instance_id } => format!("devInstance:{instance_id}"),
            Self::ProjectTerminals { project_id } => format!("projectTerminals:{project_id}"),
            Self::Workflow { run_id } => format!("workflow:{run_id}"),
        }
    }

    pub fn parse_storage_key(value: &str) -> Option<Self> {
        let (class, id) = value.split_once(':')?;
        if id.is_empty() {
            return None;
        }
        Some(match class {
            "service" => Self::Service {
                service_id: id.to_string(),
            },
            "job" => Self::Job {
                job_id: id.to_string(),
            },
            "devInstance" => Self::DevInstance {
                instance_id: id.to_string(),
            },
            "projectTerminals" => Self::ProjectTerminals {
                project_id: id.to_string(),
            },
            "workflow" => Self::Workflow {
                run_id: id.to_string(),
            },
            _ => return None,
        })
    }
}

impl std::fmt::Display for ResidencyHolder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.storage_key())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct OwnerDeathPolicy {
    pub heartbeat_timeout_ms: u64,
    pub reclaim_grace_ms: u64,
}

/// What a cell costs while nothing runs in it: a real checkout on disk, and the
/// resident memory of the shells kept between batches.
///
/// There is deliberately no concurrency field. A residency performs no work, so
/// it can never be charged for existing — the phantom unit that turned every
/// live job into a standing subtraction from the machine's capacity cannot be
/// reintroduced by declaration, because there is nothing to declare it in.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ResidencyFootprint {
    pub memory_bytes: u64,
    pub disk_growth_bytes: u64,
}

impl ResidencyFootprint {
    /// The footprint as an admission reservation. Concurrency is zero by
    /// construction.
    pub fn reservation(&self) -> ResourceReservation {
        ResourceReservation {
            memory_bytes: self.memory_bytes,
            disk_growth_bytes: self.disk_growth_bytes,
            concurrency_units: 0,
            source: ResourceReservationSource::Declared,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ResidentProcessSpec {
    /// Executable path, or [`RESIDENT_PROCESS_DEFAULT_SHELL`] when the spawning
    /// executor must resolve its own user's shell.
    pub program: String,
    #[serde(default)]
    pub args: Vec<String>,
    /// The command this resident process is being launched to execute.
    ///
    /// A terminal may submit this as shell input instead of putting it in argv,
    /// but executor-side launch preparation still needs the command before spawn
    /// to discover its Cargo context. This is launch metadata, not another
    /// command to execute.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub launch_command: Option<String>,
    #[serde(default)]
    pub cwd: String,
    #[serde(default)]
    pub cwd_root: ResidentProcessCwdRoot,
    #[serde(default)]
    pub env: Vec<(String, String)>,
    #[serde(default)]
    pub sandbox_mode: ProcessSandboxMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sandbox_policy: Option<ResidentSandboxPolicy>,
    /// Files supplied by the runner that are not part of the repository checkout.
    /// The executor validates and materializes these beneath its lease-owned scratch
    /// directory and exposes that root through `CAIRN_RUNTIME_ASSETS`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub runtime_assets: Vec<ResidentRuntimeAsset>,
    /// Runner-provided JavaScript packages, addressed relative to the
    /// `node_modules` root they install into (e.g. `@cairn/sdk/package.json`).
    ///
    /// Distinct from `runtime_assets` in destination rather than in kind:
    /// assets land in lease-owned scratch and are reached through
    /// `CAIRN_RUNTIME_ASSETS`, while packages install in the Cairn-owned
    /// directory ABOVE the checkout, where a bare-specifier import resolves
    /// them from anywhere inside it. Same contract as
    /// `ProcessBatch::runtime_packages`, so a REPL and an inline run item
    /// resolve a Cairn package the same way.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub runtime_packages: Vec<ResidentRuntimeAsset>,
    #[serde(default)]
    pub io: ResidentProcessIoMode,
}

pub const MAX_RESIDENT_RUNTIME_ASSET_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_RESIDENT_RUNTIME_ASSETS_BYTES: usize = 32 * 1024 * 1024;
pub const MAX_RESIDENT_RUNTIME_ASSETS: usize = 256;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ResidentRuntimeAsset {
    pub path: String,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "mode", rename_all = "camelCase")]
pub enum ResidentProcessIoMode {
    #[default]
    Supervised,
    /// Transport stdin, stdout, and stderr over the fenced lifetime-process
    /// protocol. The executor keeps stdin open until process teardown.
    Pipe,
    Pty {
        size: ResidentPtySize,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ResidentPtySize {
    pub rows: u16,
    pub cols: u16,
    #[serde(default)]
    pub pixel_width: u16,
    #[serde(default)]
    pub pixel_height: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "event", rename_all = "camelCase")]
pub enum ResidentProcessEventKind {
    Output {
        sequence: u64,
        stream: ResidentProcessStream,
        data: Vec<u8>,
    },
    State {
        status: ResidentProcessStatus,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ResidentProcessEvent {
    pub holder: ResidencyHolder,
    pub incarnation_id: String,
    pub cell_epoch: u64,
    pub process_key: String,
    pub process_generation: u64,
    pub event: ResidentProcessEventKind,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(
    tag = "status",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum ResidentProcessStatus {
    Stopped,
    Starting,
    Running {
        #[serde(alias = "started_at_unix_ms")]
        started_at_unix_ms: u64,
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            alias = "process_group_id"
        )]
        process_group_id: Option<u32>,
        /// Exact physical launch identity. Older persisted cells omit this and
        /// are never eligible for restart-time signalling.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ownership: Option<ResidentProcessOwnership>,
    },
    Exited {
        #[serde(alias = "finished_at_unix_ms")]
        finished_at_unix_ms: u64,
        #[serde(default, skip_serializing_if = "Option::is_none", alias = "exit_code")]
        exit_code: Option<i32>,
        restartable: bool,
        #[serde(alias = "executor_lost")]
        executor_lost: bool,
    },
}

/// What a resident process is, for the surfaces that show it.
///
/// This is the typed discriminator a running list needs to label a row without
/// string-matching prose. It replaces the free-text name and purpose a lease
/// carried, which described the substrate rather than the work.
///
/// Every payload here is something a person can read: a slug someone chose, or
/// words a subsystem declares about itself. A storage id, a lease key, or a
/// filesystem path names nobody, and a panel handed one can only fall back to
/// calling the work anonymous (CAIRN-3435).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum ResidentProcessKind {
    /// A process placed by one of Cairn's own long-lived subsystems.
    ///
    /// Both fields default because a `cairn-build-slot-state.json` written
    /// before services declared an identity carries neither, and a service cell
    /// that fails to decode is skipped by adoption entirely — its running watch
    /// left alive but invisible and unaddressable, which is a worse outcome
    /// than a row that cannot name it. The old shape's `service` key held the
    /// lease id rather than words, so this deliberately no longer reads it:
    /// decoding a storage key into `name` would put `channel-imessage` in the
    /// identity column, which is the thing CAIRN-3435 exists to stop. An empty
    /// `name` therefore means "recorded before this contract", and the surface
    /// says so rather than painting the key.
    Service {
        /// What a person calls the subsystem that placed this process — the
        /// words its lease declares, not the id its residency is keyed by
        /// (that id is on the cell's `ResidencyHolder::Service`).
        #[serde(default)]
        name: String,
        /// What this process does within that service, as a word rather than
        /// its lease-internal process key.
        #[serde(default)]
        role: String,
    },
    Terminal {
        slug: String,
    },
    Repl {
        slug: String,
    },
    DevInstance {
        /// Exact tracked terminal process that initiated this runtime. The
        /// launcher handoff breaks OS ancestry and process-group identity.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        source_terminal_session_id: Option<String>,
    },
    WorkflowRuntime {
        /// The workflow's own name — what it is invoked as, not where its
        /// package happens to live on disk.
        workflow: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ResidentProcess {
    pub generation: u64,
    pub kind: ResidentProcessKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spec: Option<ResidentProcessSpec>,
    pub status: ResidentProcessStatus,
    /// What this process costs the machine while it is live, when the surface
    /// that started it can honestly declare that. Absent means uncharged: a
    /// terminal shell and a REPL declare nothing, because an idle prompt and a
    /// compiling `cargo test` are the same live PTY and only measurement can
    /// tell them apart. This is the seam that measurement attaches to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reservation: Option<ResourceReservation>,
}

impl ResidentProcess {
    /// Whether this process is still live, and therefore still contributes its
    /// declared reservation to what the machine is charged.
    pub fn is_live(&self) -> bool {
        matches!(
            self.status,
            ResidentProcessStatus::Starting | ResidentProcessStatus::Running { .. }
        )
    }
}

/// What is running in a cell.
///
/// A cell runs at most one command batch at a time, plus the named long-lived
/// processes someone asked for. Occupancy is what user-facing surfaces
/// enumerate; the residency that holds the cell is not part of it.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CellOccupancy {
    /// Present for a transient batch that took a cell of its own and for a batch
    /// bound to a residency alike — which is what makes an agent's run visible
    /// as work rather than invisible behind the substrate that hosts it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<ActiveCellRequest>,
    /// Durable physical ownership for the transient command batch. A legacy
    /// `command` without this record is diagnostic-only: it is not authority to
    /// signal anything found in the checkout.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command_ownership: Option<CommandBatchOwnership>,
    /// A prior epoch's ownership that has not been proven dead yet.
    ///
    /// Occupancy on purpose rather than a field beside it: a tree with a live
    /// writer in it is occupied in the only sense destructive operations care
    /// about, so every gate already written in terms of [`Self::is_empty`] —
    /// the target reclaim, retirement, cell selection — refuses it without
    /// being taught about survivors individually.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub draining: Option<DrainingCommandOwnership>,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub processes: std::collections::BTreeMap<String, ResidentProcess>,
}

impl CellOccupancy {
    pub fn is_empty(&self) -> bool {
        self.command.is_none()
            && self.command_ownership.is_none()
            && self.draining.is_none()
            && self.processes.is_empty()
    }

    pub fn live_processes(&self) -> impl Iterator<Item = (&String, &ResidentProcess)> {
        self.processes
            .iter()
            .filter(|(_, process)| process.is_live())
    }

    /// What the live resident processes in this cell declare against the host.
    /// Derived from the persisted status of each process, so a recorded exit
    /// stops contributing without any release call having to run.
    pub fn resident_reservation(&self) -> ResourceReservation {
        let mut total = ResourceReservation {
            memory_bytes: 0,
            disk_growth_bytes: 0,
            concurrency_units: 0,
            source: ResourceReservationSource::Declared,
        };
        for (_, process) in self.live_processes() {
            let fallback = ResourceReservation::default();
            let reservation = process.reservation.as_ref().unwrap_or(&fallback);
            total.memory_bytes = total.memory_bytes.saturating_add(reservation.memory_bytes);
            total.disk_growth_bytes = total
                .disk_growth_bytes
                .saturating_add(reservation.disk_growth_bytes);
            total.concurrency_units = total
                .concurrency_units
                .saturating_add(reservation.concurrency_units.max(1));
        }
        total
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ResidencyPhase {
    Active,
    AwaitingReclaim,
    Releasing,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum ResidencyEventKind {
    Acquired,
    Renewed,
    AwaitingReclaim,
    Reclaimed,
    ProcessStarting {
        #[serde(alias = "process_key")]
        process_key: String,
        generation: u64,
    },
    ProcessRunning {
        #[serde(alias = "process_key")]
        process_key: String,
        generation: u64,
    },
    ProcessExited {
        #[serde(alias = "process_key")]
        process_key: String,
        generation: u64,
        restartable: bool,
        #[serde(alias = "executor_lost")]
        executor_lost: bool,
    },
    #[serde(rename = "materializationRefreshed")]
    CheckoutRefreshed {
        #[serde(alias = "base_commit")]
        base_commit: String,
    },
    CheckoutHistoryDiscontinuity {
        prior_head: String,
        new_base_commit: String,
        cell_epoch: u64,
    },
    Releasing,
    Released,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ResidencyEvent {
    pub revision: u64,
    pub occurred_at_unix_ms: u64,
    pub event: ResidencyEventKind,
}

/// Who holds this cell, at which coordinate, under which heartbeat and reclaim
/// policy. A residency runs nothing by itself.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CellResidency {
    pub holder: ResidencyHolder,
    pub repository: RepositoryLocator,
    /// Attribution for infrastructure views. Descriptive only, never identity:
    /// it is read from a query whose result changes as runs are added.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_ref: Option<CellOwnerRef>,
    /// The coordinate this residency was launched against, when the holder is
    /// known by something other than its id — a dev instance's branch. Like
    /// `owner_ref`, this is how a surface says which residency this is, not what
    /// makes it that residency.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selector: Option<String>,
    #[serde(default)]
    pub incarnation_id: String,
    pub current_base_commit: String,
    pub phase: ResidencyPhase,
    pub last_heartbeat_unix_ms: u64,
    pub reclaim_deadline_unix_ms: u64,
    pub death_policy: OwnerDeathPolicy,
    pub footprint: ResidencyFootprint,
    pub state_revision: u64,
    #[serde(default)]
    pub events: Vec<ResidencyEvent>,
}

impl CellResidency {
    /// Whether an acquire names this residency.
    ///
    /// A residency is the holder it belongs to and the repository it holds.
    /// Nothing an acquirer happens to be carrying is identity: the coordinate
    /// drifts with every head advance, the footprint is sizing, the death policy
    /// is a timeout, and `owner_ref` is display attribution. Comparing those
    /// would make an ordinary re-acquire look like a second, conflicting holder
    /// — the opposite of the convergence a residency exists to provide. A
    /// same-holder acquire either lands in this residency or is refused; a
    /// sibling is never legal.
    pub fn identity_matches(
        &self,
        holder: &ResidencyHolder,
        repository: &RepositoryIdentity,
    ) -> bool {
        self.holder == *holder && self.repository.identity() == *repository
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum CellCheckoutKind {
    #[default]
    JujutsuWorkspace,
    DetachedGitWorktree,
    /// A checkout owned outside the build fabric. The executor may host a
    /// resident process in it, but must never reset, clean, or delete it.
    ExistingCheckout,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PersistentCellState {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub executor_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executor_display_name: Option<String>,
    pub project_id: String,
    #[serde(rename = "slotId")]
    pub cell_id: String,
    pub path: String,
    #[serde(default)]
    pub workspace_name: String,
    pub repository: String,
    #[serde(default)]
    #[serde(rename = "materializationKind")]
    pub checkout_kind: CellCheckoutKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git_common_dir: Option<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub authority_path: String,
    pub lifecycle: PersistentCellLifecycle,
    pub cell_epoch: u64,
    pub last_sealed_commit: Option<String>,
    /// The last distance observation, labelled with the target it measured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_distance: Option<crate::cache_distance::MeasuredDistance>,
    /// Why this slot is fenced; detailed process and original-file evidence lives
    /// in the authority's quarantine record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quarantine_reason: Option<String>,
    pub last_used_unix_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_affinity_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preparation_fingerprint: Option<String>,
    /// Work classes that have completed at least once in this checkout since it
    /// was created.
    ///
    /// The one fact that separates a cell which has compiled this project from
    /// one that merely exists. It accumulates over the cell's life because build
    /// products do: re-preparing onto a different base leaves the target
    /// directory in place, and only recreating the checkout clears it. Bounded
    /// by the number of [`CellCommandClass`] variants, so it never grows.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warm_command_classes: Vec<CellCommandClass>,
    /// Who holds this cell. Absent for a free cell and for a transient batch
    /// that took a cell of its own.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub residency: Option<CellResidency>,
    /// What is running in this cell.
    #[serde(default, skip_serializing_if = "CellOccupancy::is_empty")]
    pub occupancy: CellOccupancy,
    /// How much room this cell's tree, scratch, and build products occupy.
    ///
    /// Maintained incrementally where bytes actually change — materialization,
    /// delta sealing, scratch reclamation — and overwritten by the periodic
    /// storage sweep, which stays the source of truth. Persisted so it survives
    /// adoption across an executor restart. Zero means unmeasured, not empty.
    #[serde(default)]
    pub size_bytes: u64,
}

impl PersistentCellState {
    /// Whether this checkout has already completed the given work class, and so
    /// holds whatever that class writes into the tree.
    pub fn is_warm_for(&self, class: CellCommandClass) -> bool {
        self.warm_command_classes.contains(&class)
    }

    /// Record that a work class completed here. Idempotent, and bounded by the
    /// number of classes that exist.
    pub fn mark_warm_for(&mut self, class: CellCommandClass) {
        if !self.warm_command_classes.contains(&class) {
            self.warm_command_classes.push(class);
        }
    }

    /// A cell nobody holds and nothing runs in — the only cell that may be
    /// reused, retired, or swept.
    pub fn is_free(&self) -> bool {
        self.residency.is_none() && self.occupancy.is_empty()
    }

    pub fn is_held(&self) -> bool {
        !self.is_free()
    }
}

/// What an admission request is asking the fleet for: a cell to run a command
/// batch in, or a cell to hold as a residency.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum CellAdmissionKind {
    #[default]
    Command,
    Residency,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct QueuedCellRequest {
    #[serde(default)]
    pub admission_kind: CellAdmissionKind,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub executor_id: String,
    pub request_id: String,
    pub attempt_id: String,
    pub project_id: String,
    pub command: String,
    #[serde(default)]
    pub command_class: CellCommandClass,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<CellOwnerRef>,
    pub priority: CellPriority,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_priority: Option<CellPriority>,
    pub requesting_job_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub affinity_key: Option<String>,
    /// The profile identity this work was submitted under, echoed back so the
    /// runner can price what sits ahead of a request it is considering placing.
    /// Without it a queue forecast could only ever use class priors, which
    /// over-predicts the wait on exactly the machines that have run this work
    /// often enough to be fast at it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command_resource_identity: Option<CommandResourceIdentity>,
    pub queued_at_unix_ms: u64,
    #[serde(default)]
    pub resource_reservation: ResourceReservation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub learned_estimate: Option<LearnedResourceEstimate>,
    #[serde(default = "default_subscriber_count")]
    pub subscriber_count: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub substrate_hold: Option<ExecutorSubstrateEvidence>,
    /// The newest runner-side placement decision for this exact request attempt.
    /// Executor queue state explains that a request waits; this explains why the
    /// fleet could not move it somewhere else.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub placement: Option<QueuedPlacementEvidence>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct QueuedPlacementEvidence {
    pub decided_at_unix_ms: u64,
    pub mobility: PlacementMobility,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pinned_executor_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected: Option<QueuedPlacementTarget>,
    #[serde(default)]
    pub rejected: Vec<QueuedPlacementRejection>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct QueuedPlacementTarget {
    pub executor_name: String,
    pub executor_id: String,
    pub available_memory_bytes: Option<u64>,
    pub free_volume_bytes: Option<u64>,
    pub reservation: ResourceReservation,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct QueuedPlacementRejection {
    pub executor_name: String,
    pub executor_id: String,
    pub reason: PlacementRejectionReason,
}

/// What the live resident processes across the fleet cost. This counts work, not
/// holders: a cell held by a residency with nothing running in it contributes
/// nothing here, because it is running nothing.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ResidentOccupancyEvidence {
    pub process_count: usize,
    pub reservation: ResourceReservation,
}

impl Default for ResidentOccupancyEvidence {
    fn default() -> Self {
        Self {
            process_count: 0,
            reservation: ResourceReservation {
                concurrency_units: 0,
                ..ResourceReservation::default()
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CellOutputEvent {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub executor_id: String,
    #[serde(rename = "slotId")]
    pub cell_id: String,
    pub request_id: String,
    pub attempt_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub stream_id: String,
    pub chunk: String,
    pub emitted_at_unix_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ExecutingCellRequest {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub executor_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    #[serde(rename = "slotId")]
    pub cell_id: String,
    pub request_id: String,
    pub attempt_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<CellOwnerRef>,
    #[serde(default)]
    pub command_class: CellCommandClass,
    #[serde(default)]
    pub command: String,
    pub started_at_unix_ms: u64,
    pub process_ids: Vec<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<CellPriority>,
    #[serde(default = "default_subscriber_count")]
    pub subscriber_count: usize,
    #[serde(default)]
    pub resource_reservation: ResourceReservation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub learned_estimate: Option<LearnedResourceEstimate>,
    /// The profile identity this execution was submitted under. Lets the runner
    /// price the remaining time of work already running when it forecasts what a
    /// new request would wait behind.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command_resource_identity: Option<CommandResourceIdentity>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum CellCompletionVerdict {
    Succeeded,
    Failed,
    TimedOut,
    Cancelled,
    Unavailable,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CellCompletion {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub executor_id: String,
    pub request_id: String,
    pub attempt_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<CellOwnerRef>,
    pub command_class: CellCommandClass,
    pub command: String,
    pub priority: CellPriority,
    pub queued_at_unix_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at_unix_ms: Option<u64>,
    pub finished_at_unix_ms: u64,
    pub duration_ms: u64,
    pub verdict: CellCompletionVerdict,
    /// Tail of output already captured for a failed process batch. Bounded by
    /// the executor before it enters the fleet snapshot.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_output_tail: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transcript_event_uri: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource_reservation: Option<ResourceReservation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub learned_estimate: Option<LearnedResourceEstimate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actuals: Option<CellExecutionMeta>,
    #[serde(default)]
    pub cached: bool,
    #[serde(default = "default_subscriber_count")]
    pub subscriber_count: usize,
    pub served_at_unix_ms: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct FleetSnapshot {
    #[serde(rename = "slots")]
    pub cells: Vec<PersistentCellState>,
    /// Executor-local command waits only. Residency acquisition is either a
    /// claimed grant or a convergence-only reattachment and never queues here.
    pub queued_requests: Vec<QueuedCellRequest>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub executing_requests: Vec<ExecutingCellRequest>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub in_flight_residency_operations: HashMap<String, ExecutorSubstrateEvidence>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub recent_completions: Vec<CellCompletion>,
    /// Every attempt this executor process was granted and has not finished,
    /// whatever phase it is in: acquiring a cell, materializing a checkout,
    /// between batch items, or running a command. `executingRequests` names only
    /// attempts with a live command process, so this is the account the runner
    /// settles work held across a link gap against: an attempt absent from it
    /// and from `recentCompletions` did not survive the process that ran it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub granted_attempts: Vec<GrantedAttempt>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resident_occupancy: Option<ResidentOccupancyEvidence>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub substrate_state: Option<ExecutorSubstrateEvidence>,
    /// Each project this machine is provisioning a cell for, and the request
    /// whose admission holds it. Every other admission for that project waits
    /// behind the hold, so it is posted with its owner and age rather than left
    /// to be inferred from declines.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub provisioning_holds: Vec<ProvisioningHold>,
    /// Holds this machine released because no claim and no in-flight admission
    /// owned them. Each one is an admission exit that failed to release, so
    /// anything but zero is a defect to report.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub orphaned_provisioning_releases: u64,
}

fn is_zero_u64(value: &u64) -> bool {
    *value == 0
}

/// One attempt an executor process holds a grant for. See
/// [`FleetSnapshot::granted_attempts`].
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "camelCase")]
pub struct GrantedAttempt {
    pub request_id: String,
    pub attempt_id: String,
}

/// One project's provisioning serialisation on a machine, and who holds it.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ProvisioningHold {
    pub project_id: String,
    /// The request whose admission holds the project.
    pub owner_request_id: String,
    /// The runner's queue entry, when the hold was taken by a claim.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entry_id: Option<String>,
    pub since_unix_ms: u64,
}

/// Bumped to 5 for CAIRN-3355: `CompileCacheStats` carries what the daemon did
/// with the compiles it ran ITSELF — `compilesExecuted`, `compilations`,
/// `compileFailures` — not just what it was asked for. A consumer that cannot
/// see them renders a daemon failing every compile it accepts as an ordinary
/// cache line with no hits yet, which is the misreport those fields exist to
/// prevent.
///
/// Bumped to 4 for CAIRN-3332: the snapshot carries `compileCache`, the
/// lifecycle and statistics of the machine's Cairn-supervised compile-cache
/// daemon. It is optional, so an older consumer decodes the rest unchanged, but
/// the number moves because a build that cannot see the field renders a fleet
/// whose cache state is simply absent rather than named.
///
/// Bumped to 3 for CAIRN-3330: a disk-accounting walk no longer reports an entry
/// that vanished under it as a skip. `DiskAccounting.skipped` now carries only
/// paths that still exist and went unmeasured — so it alone decides
/// `DiskAccountingPartial` — while entries that disappeared mid-scan ride along
/// as the `vanishedEntries` count.
pub const SUBSTRATE_HEALTH_SCHEMA_VERSION: u32 = 8;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub enum SubstrateHealthStatus {
    Healthy,
    Degraded,
    Blocked,
    Unknown,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ExecutorHealthStatus {
    Online,
    Stale,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum DiskHealthStatus {
    /// Free space is above the pressure line.
    Ok,
    /// Free space is at or below the pressure line and above the admission
    /// watermark. The pressure tier is reclaiming; admission still accepts.
    /// A price, never a refusal.
    Tight,
    /// Free space is below the admission watermark.
    Pressured,
    Full,
    Unknown,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum StorageSweepStatus {
    #[default]
    NotStarted,
    InFlight,
    Completed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum SubstrateHealthReason {
    NoExecutors,
    StaleExecutor {
        executor_id: String,
    },
    HostPressure {
        executor_id: String,
    },
    DiskPressured {
        executor_id: String,
    },
    DiskFull {
        executor_id: String,
    },
    AdmissionSaturated {
        executor_id: String,
    },
    StorageCleanupFailed {
        executor_id: String,
    },
    /// The categorized walk completed but could not price every entry. It
    /// carries the bounded evidence, not just a count, so a recurring skip is
    /// diagnosable from the snapshot alone.
    DiskAccountingPartial {
        executor_id: String,
        skipped_entries: usize,
        skipped: Vec<SkippedEntry>,
    },
    /// A named machine reading this executor cannot currently take.
    MeasurementUnavailable {
        executor_id: String,
        measurement: MachineMeasurement,
        reason: MeasurementGap,
    },
    /// The link is healthy but the facts riding it have aged out. Distinct from
    /// [`SubstrateHealthReason::StaleExecutor`], which is the link itself.
    StaleTelemetry {
        executor_id: String,
        age_ms: u64,
    },
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BoundedDurationSummary {
    pub sample_count: u64,
    pub p50_ms: Option<u64>,
    pub p95_ms: Option<u64>,
    pub max_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct QueueClassHealth {
    pub priority: CellPriority,
    pub depth: usize,
    pub oldest_age_ms: Option<u64>,
    pub waits: BoundedDurationSummary,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AdmissionHealth {
    pub concurrency_capacity: Option<u32>,
    #[serde(default)]
    pub interactive_reserve: u32,
    pub memory_capacity_bytes: Option<u64>,
    pub disk_growth_capacity_bytes: Option<u64>,
    pub active_reservation: ResourceReservation,
    pub queued_reservation_bytes: u64,
    pub accepted_count: u64,
    pub rejected_count: u64,
    pub timed_out_count: u64,
}

/// Why a measurement is absent.
///
/// A consumer renders a named gap from one of these rather than inventing a
/// meaning for a null. The codes are stable because both the operator panel and
/// the placement seam branch on them: "this platform cannot answer" and "this
/// platform tried and failed" are different facts about the same machine.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub enum MeasurementGap {
    /// No implementation exists for this reading on this platform.
    UnsupportedPlatform,
    /// The platform API exists, was called, and failed.
    SamplingFailed,
    /// The reading requires a permission this executor does not hold.
    PermissionDenied,
    /// No attempt has completed yet. This is the state a freshly started
    /// executor is honestly in, and it is what a defaulted measurement means.
    NotSampled,
}

impl MeasurementGap {
    /// Why there is no value, in the words every surface uses. One home for the
    /// vocabulary, so a placement rejection and the operator panel cannot come
    /// to describe the same gap differently.
    pub fn describe(self) -> &'static str {
        match self {
            Self::UnsupportedPlatform => "this platform has no reading",
            Self::SamplingFailed => "sampling failed",
            Self::PermissionDenied => "permission denied",
            Self::NotSampled => "not sampled yet",
        }
    }
}

/// The outcome of one attempt to take a reading: a value, or a named reason
/// there is none.
///
/// Deliberately not `Option<T>`. A null cannot say whether the platform has no
/// implementation, refused the call, or has simply not been asked yet, and the
/// three lead to different actions.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum MeasurementReading<T> {
    Measured {
        value: T,
    },
    Unavailable {
        reason: MeasurementGap,
        /// Bounded diagnostic text where the reason alone is not enough to act
        /// on — an OS error string, say. Never a path on the executor's disk.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        detail: Option<String>,
    },
}

/// One machine reading, carrying when it was taken.
///
/// The timestamp belongs to the *attempt*, not to the heartbeat that ships it:
/// a fresh beat cannot freshen a cached fact, and a failed refresh publishes an
/// unavailable reading stamped for the failure rather than resending the last
/// good value under a new time. Consumers compute age from
/// `measured_at_unix_ms` and never from the beat.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub struct Measurement<T> {
    pub measured_at_unix_ms: u64,
    pub reading: MeasurementReading<T>,
}

impl<T> Default for Measurement<T> {
    fn default() -> Self {
        Self {
            measured_at_unix_ms: 0,
            reading: MeasurementReading::Unavailable {
                reason: MeasurementGap::NotSampled,
                detail: None,
            },
        }
    }
}

impl<T> Measurement<T> {
    pub fn measured(measured_at_unix_ms: u64, value: T) -> Self {
        Self {
            measured_at_unix_ms,
            reading: MeasurementReading::Measured { value },
        }
    }

    pub fn unavailable(measured_at_unix_ms: u64, reason: MeasurementGap) -> Self {
        Self {
            measured_at_unix_ms,
            reading: MeasurementReading::Unavailable {
                reason,
                detail: None,
            },
        }
    }

    pub fn unavailable_with(
        measured_at_unix_ms: u64,
        reason: MeasurementGap,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            measured_at_unix_ms,
            reading: MeasurementReading::Unavailable {
                reason,
                detail: Some(detail.into()),
            },
        }
    }

    pub fn value(&self) -> Option<&T> {
        match &self.reading {
            MeasurementReading::Measured { value } => Some(value),
            MeasurementReading::Unavailable { .. } => None,
        }
    }

    pub fn gap(&self) -> Option<MeasurementGap> {
        match &self.reading {
            MeasurementReading::Measured { .. } => None,
            MeasurementReading::Unavailable { reason, .. } => Some(*reason),
        }
    }

    /// How long ago this reading was taken, relative to a capture instant.
    /// Distinct from heartbeat age, which describes the link rather than the
    /// fact.
    pub fn age_ms(&self, now_unix_ms: u64) -> u64 {
        now_unix_ms.saturating_sub(self.measured_at_unix_ms)
    }
}

/// How much of the machine's processor time was spent working, over the window
/// between two samples.
///
/// This is the quantity every platform actually keeps: cumulative user, system,
/// and idle tick counters, differenced across a sampling window. macOS reads it
/// from the mach host, Linux from `/proc/stat`, Windows from `GetSystemTimes`,
/// and all three mean exactly the same thing — which is why the reading carries
/// no provenance discriminant. It is the number Activity Monitor and Task
/// Manager show, and the shares below sum to one with idle.
///
/// Deliberately not a run-queue load average. A load average is a lagging
/// exponential mean of runnable *and* uninterruptible threads, it has no upper
/// bound, and it is not comparable across machines without dividing by cores and
/// then explaining what the result means. It made a build host doing its job
/// read as 234% of something.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CpuPressure {
    /// Non-idle share of processor time in `[0, 1]`. Equals `user + system`.
    pub utilization: f64,
    /// Share spent in user code. Nice time folds in here, as the platform tools
    /// present it.
    pub user: f64,
    /// Share spent in the kernel. A high share here is contention or I/O rather
    /// than work getting done, which is why it is worth separating.
    pub system: f64,
    pub logical_cores: usize,
}

/// Physical memory on the machine, which is the memory question placement asks.
/// The executor daemon's own resident size is in [`ProcessTelemetry`] and is not
/// a substitute for this.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub struct MachineMemory {
    pub total_bytes: u64,
    pub available_bytes: u64,
}

impl MachineMemory {
    pub fn used_bytes(&self) -> u64 {
        self.total_bytes.saturating_sub(self.available_bytes)
    }
}

/// Capacity of the volume the executor home lives on.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub struct MachineVolume {
    pub total_bytes: u64,
    pub free_bytes: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub enum SkippedEntryOperation {
    ReadDirectory,
    ReadEntry,
    ReadMetadata,
    /// Sizing a whole tree failed somewhere inside it; the path is the tree.
    /// Only the storage sweep records this, in [`DiskHealth::cleanup_skipped`].
    Measure,
    /// Deleting the path failed with a condition the sweep survives. Only the
    /// storage sweep records this, in [`DiskHealth::cleanup_skipped`].
    Remove,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub enum SkippedEntryReason {
    /// The executor may not read the entry. Correct to skip once, wrong to keep
    /// skipping: a persistent one is a configuration problem, not a race.
    PermissionDenied,
    /// The scan was interrupted or the entry was busy.
    Contended,
    /// The process ran out of descriptors or another bounded resource.
    ResourceExhausted,
    /// An error outside the classes above, which the storage sweep survives
    /// only because sizing and reclaiming a retired tree must never latch
    /// admission. The executor's log carries the full error.
    Other,
}

/// One directory entry a categorized walk could not price, and why.
///
/// Every reason here is a path that is still on disk, holding bytes the walk did
/// not count. An entry that stopped existing mid-scan is not one of these: its
/// bytes are gone, so it is aggregated into [`DiskAccounting::vanished_entries`]
/// rather than named as a skip.
///
/// The path is what makes a partial measurement diagnosable after the fact: two
/// paths skipped is a number, and `build-slots/slot-3` skipped for
/// `permissionDenied` is a bug report.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub struct SkippedEntry {
    /// Executor-home-relative. Absolute paths never cross this boundary.
    pub path: String,
    pub operation: SkippedEntryOperation,
    pub reason: SkippedEntryReason,
}

/// What the categorized disk walk found, which is allowed to be partial.
///
/// Partial is a distinct state from failed and from measured-zero. A walk that
/// skipped an unreadable cell still prices everything else, and that is worth
/// publishing — as long as it stays visibly partial.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DiskAccounting {
    pub used_bytes: u64,
    pub categories: DiskCategoryAccounting,
    /// Entries this scan could not price that are still on disk. Empty means
    /// every byte the walk found was counted.
    #[serde(default)]
    pub skipped: Vec<SkippedEntry>,
    /// Skips beyond the retained bound, so a pathological run reports its true
    /// count without shipping an unbounded list.
    #[serde(default)]
    pub skipped_truncated: usize,
    /// Entries that stopped existing between being listed and being priced.
    ///
    /// A quiet aggregate, deliberately without paths: this is a build host doing
    /// exactly what it is designed to do — scratch reclaimed at run teardown,
    /// git and jj temporaries churning under a build — and the entries hold no
    /// disk to account for. It is published because a walk that suddenly races
    /// thousands of entries is worth seeing, not because any one of them is.
    #[serde(default)]
    pub vanished_entries: usize,
    /// The largest top-level entries of the executor home whose bytes fall in
    /// `temporaryOtherBytes`, largest first and bounded. "Other" is the bucket
    /// that grows without an owner, and a total alone cannot say whose it is.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub other_top: Vec<DiskPathUsage>,
}

/// Bytes one executor-home-relative path holds.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DiskPathUsage {
    pub path: String,
    pub bytes: u64,
}

impl DiskAccounting {
    pub fn skipped_count(&self) -> usize {
        self.skipped.len().saturating_add(self.skipped_truncated)
    }

    /// Whether the total is short by an unknown amount.
    ///
    /// Keyed on skips alone. Vanished entries are excluded from the total
    /// because they occupy nothing, which leaves it exactly as accurate as a
    /// walk that raced nothing — demoting the verdict for them would report a
    /// problem where the system behaved as designed.
    pub fn is_partial(&self) -> bool {
        self.skipped_count() > 0
    }
}

/// Executor-daemon diagnostics.
///
/// Deliberately not capacity. This answers "how big is the helper process",
/// which is a useful thing to know when the helper misbehaves and a misleading
/// thing to schedule against. Machine pressure is [`MachineTelemetry`]'s other
/// fields.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ProcessTelemetry {
    /// Resident set size on Unix, working set on Windows.
    pub resident_bytes: Measurement<u64>,
    /// macOS physical footprint, which counts compressed and swapped pages the
    /// resident set does not. Unavailable elsewhere by construction.
    pub physical_footprint_bytes: Measurement<u64>,
    /// Virtual address space the process has mapped.
    ///
    /// The reading that separates the two ways a daemon gets large, which look
    /// identical from resident size alone. A process holding gigabytes of live
    /// data has a resident set near its virtual size; a process whose allocator
    /// has scattered a few megabytes across tens of thousands of regions it
    /// will not return has a virtual size many times its footprint and an
    /// attributed total near zero. Only the second is fixed by looking at the
    /// allocation *rate*, and without this field the two are indistinguishable
    /// until someone runs `vmmap` on a process that is about to be restarted.
    #[serde(default)]
    pub virtual_bytes: Measurement<u64>,
    /// The allocator's own split of the heap into live and retained-but-free.
    ///
    /// The reading that decides where a growing daemon's fix belongs, and the
    /// only one here that can decide it. Live bytes near resident size mean
    /// something is holding them: the owner table below is under-instrumented,
    /// and the next step is naming the owner. `free_retained_bytes` near
    /// resident size means the program already freed them and the allocator has
    /// not returned them to the kernel: no owner exists to name however many
    /// are added, because from the program's side those bytes are not owned at
    /// all, and the fix belongs at whatever site churns allocations of that
    /// size.
    ///
    /// Read as three figures rather than two. Live bytes are `in_use_bytes`
    /// *plus* `mmapped_bytes`, because glibc splits them by size class — taking
    /// `in_use_bytes` alone for "everything live" reports a leak made of large
    /// blocks as an empty heap.
    ///
    /// Without it the two are indistinguishable, and an attributed total near
    /// zero beside a resident set of tens of gigabytes reads as a failure of
    /// attribution when it may be attribution reporting correctly on memory
    /// that has no owner.
    #[serde(default)]
    pub allocator: Measurement<AllocatorState>,
    /// Which of the daemon's own in-memory owners is holding that resident set.
    ///
    /// A resident size alone says a process is large; it never says what is
    /// large about it, and a restart erases the evidence. This is the other
    /// half of that question.
    #[serde(default)]
    pub retained: ExecutorRetainedState,
}

/// What the C allocator says it is holding, straight from `mallinfo2`.
///
/// Read rather than estimated. Every byte-count in [`ExecutorRetainedState`] is
/// derived from the capacities of live collections, which by construction can
/// never see memory the program has already freed — so an allocator that is not
/// returning freed memory is invisible to attribution no matter how many owners
/// it grows. This is the reading that sees it.
///
/// glibc-specific and named as unavailable elsewhere, which is honest rather
/// than unfortunate: the quantity is a property of a particular allocator's
/// bookkeeping, and substituting a different allocator's numbers under these
/// names would make a platform comparison meaningless.
///
/// Deliberately `mallinfo2` and never `mallinfo`. The older call reports each of
/// these as a C `int`, which wraps silently past two gigabytes — so on the very
/// process this exists to describe, it would report a plausible small number
/// rather than an obviously missing one.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub struct AllocatorState {
    /// Bytes the program is currently holding in blocks served from the
    /// allocator's arenas (`uordblks`).
    ///
    /// Not a total of everything live, and the difference matters: a block big
    /// enough for the allocator to serve from a mapping of its own is counted
    /// in [`Self::mmapped_bytes`] instead. A process holding gigabytes in a few
    /// large blocks reads near zero here.
    pub in_use_bytes: u64,
    /// Bytes the program has freed that the allocator kept rather than
    /// returning to the kernel (`fordblks`). Resident, unattributable, and the
    /// signature of allocation churn rather than of a leak.
    pub free_retained_bytes: u64,
    /// Total size of the allocator's heap arenas (`arena`).
    pub arena_bytes: u64,
    /// Bytes the program is holding in blocks the allocator satisfied with
    /// their own mappings (`hblkhd`), which it returns to the kernel on free
    /// rather than retaining. Live memory, like [`Self::in_use_bytes`] and
    /// unlike [`Self::free_retained_bytes`] — the two together are what the
    /// program actually holds.
    pub mmapped_bytes: u64,
    /// How many such mappings are live (`hblks`).
    pub mmap_chunk_count: u64,
}

/// One owner of retained executor-daemon state: how many things it currently
/// holds, roughly how many bytes those things own, and the high-water marks
/// each has reached since the process started.
///
/// `estimated_bytes` is an estimate and says so. It is built from owned string
/// and vector capacities and from the serialized size of queued payloads, never
/// from the allocator, so it will not reconcile to resident set size and is not
/// meant to. What it is for is attribution — which owner is carrying the growth
/// — which is exactly the question a 144 GB process could not answer.
///
/// The peaks are what make a settled snapshot readable after the fact. A queue
/// that is empty now and once held two hundred megabytes is a different machine
/// from one that never queued anything, and only the peak distinguishes them.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RetainedOwner {
    pub entries: u64,
    pub estimated_bytes: u64,
    pub peak_entries: u64,
    pub peak_estimated_bytes: u64,
}

impl RetainedOwner {
    /// Holding nothing now and never having held anything. An owner in this
    /// state is noise in a report, so the renderer may omit it.
    pub fn is_untouched(&self) -> bool {
        self.entries == 0
            && self.estimated_bytes == 0
            && self.peak_entries == 0
            && self.peak_estimated_bytes == 0
    }
}

/// The stable name of a retained-state owner, so a report names owners rather
/// than positions and a reader can grep for the one that grew.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub enum RetainedOwnerKind {
    QueuedRequests,
    ExecutingRequests,
    CompletedRequests,
    Cancellations,
    CommandProcesses,
    ResidentProcesses,
    CommandOwnership,
    Projects,
    Cells,
    WaitSamples,
    ResidencyOperationLocks,
    ResidencyAcquisitionLocks,
    ResidencyReclaimContention,
    CallbackWaiters,
    OutgoingCallbackQueue,
    ActiveProcessAttempts,
    CapturedProcessOutput,
    QueuedOutputChunks,
    ManagedManifests,
    ProcessCensus,
    PendingSnapshot,
    OutgoingControlQueue,
}

impl RetainedOwnerKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::QueuedRequests => "queuedRequests",
            Self::ExecutingRequests => "executingRequests",
            Self::CompletedRequests => "completedRequests",
            Self::Cancellations => "cancellations",
            Self::CommandProcesses => "commandProcesses",
            Self::ResidentProcesses => "residentProcesses",
            Self::CommandOwnership => "commandOwnership",
            Self::Projects => "projects",
            Self::Cells => "cells",
            Self::WaitSamples => "waitSamples",
            Self::ResidencyOperationLocks => "residencyOperationLocks",
            Self::ResidencyAcquisitionLocks => "residencyAcquisitionLocks",
            Self::ResidencyReclaimContention => "residencyReclaimContention",
            Self::CallbackWaiters => "callbackWaiters",
            Self::OutgoingCallbackQueue => "outgoingCallbackQueue",
            Self::ActiveProcessAttempts => "activeProcessAttempts",
            Self::CapturedProcessOutput => "capturedProcessOutput",
            Self::QueuedOutputChunks => "queuedOutputChunks",
            Self::ManagedManifests => "managedManifests",
            Self::ProcessCensus => "processCensus",
            Self::PendingSnapshot => "pendingSnapshot",
            Self::OutgoingControlQueue => "outgoingControlQueue",
        }
    }

    /// What an operator reads. Deliberately says what the owner *is* rather
    /// than which field holds it, because the field is what moves between
    /// releases and the concept is what a recurrence will be described in.
    pub fn label(self) -> &'static str {
        match self {
            Self::QueuedRequests => "queued requests",
            Self::ExecutingRequests => "executing requests",
            Self::CompletedRequests => "recent completions (bounded history)",
            Self::Cancellations => "cancellation tokens",
            Self::CommandProcesses => "command-process supervision",
            Self::ResidentProcesses => "resident-process supervision",
            Self::CommandOwnership => "durable command ownership",
            Self::Projects => "configured projects",
            Self::Cells => "cells",
            Self::WaitSamples => "queue wait samples (bounded history)",
            Self::ResidencyOperationLocks => "residency operation locks",
            Self::ResidencyAcquisitionLocks => "residency acquisition locks",
            Self::ResidencyReclaimContention => "residency reclaim contention",
            Self::CallbackWaiters => "callback waiters",
            Self::OutgoingCallbackQueue => "queued callback payloads",
            Self::ActiveProcessAttempts => "active process attempts",
            Self::CapturedProcessOutput => "captured process output",
            Self::QueuedOutputChunks => "queued output chunks",
            Self::ManagedManifests => "managed-object manifests",
            Self::ProcessCensus => "process census (current sample)",
            Self::PendingSnapshot => "pending snapshot (latest only)",
            Self::OutgoingControlQueue => "queued control messages (bytes are a floor)",
        }
    }
}

/// What the executor daemon is holding in memory, by owner.
///
/// Diagnostic in the strongest sense: nothing here is a placement input, and
/// nothing here is allowed to become one. Its whole job is that the next time
/// this process grows without bound, a single read of `cairn://executors/{name}`
/// names the owner instead of a restart erasing the evidence.
///
/// Two collection disciplines meet here, and the difference is deliberate.
/// Flow-through owners — queues, waiters, captured output — carry live gauges
/// updated at every enqueue and dequeue, because their danger is a spike
/// between two samples. Structural owners — cells, projects, manifests, lock
/// maps — are counted by walking them when the snapshot is taken, because they
/// move slowly and a walk is exact where a hand-maintained counter drifts.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ExecutorRetainedState {
    /// When this snapshot was taken. Its own timestamp rather than the
    /// heartbeat's, on the same principle as every other measurement here: a
    /// beat must not be able to freshen a fact it did not collect.
    pub measured_at_unix_ms: u64,
    pub queued_requests: RetainedOwner,
    pub executing_requests: RetainedOwner,
    pub completed_requests: RetainedOwner,
    pub cancellations: RetainedOwner,
    pub command_processes: RetainedOwner,
    pub resident_processes: RetainedOwner,
    pub command_ownership: RetainedOwner,
    pub projects: RetainedOwner,
    pub cells: RetainedOwner,
    pub wait_samples: RetainedOwner,
    pub residency_operation_locks: RetainedOwner,
    pub residency_acquisition_locks: RetainedOwner,
    pub residency_reclaim_contention: RetainedOwner,
    pub callback_waiters: RetainedOwner,
    pub outgoing_callback_queue: RetainedOwner,
    pub active_process_attempts: RetainedOwner,
    pub captured_process_output: RetainedOwner,
    pub queued_output_chunks: RetainedOwner,
    pub managed_manifests: RetainedOwner,
    /// The census is resampled and dropped every tick, so this reports the
    /// current sample rather than retained history. A census that grows here is
    /// a machine with more processes on it, not a leak.
    pub process_census: RetainedOwner,
    /// The machine snapshot waiting for the link writer. At most one by
    /// construction, since a newer snapshot replaces an older one; its bytes
    /// are the size of the last one serialized. A reading above one entry is
    /// a regression of the bound itself. Defaulted so an executor that predates
    /// the slot still deserializes.
    #[serde(default)]
    pub pending_snapshot: RetainedOwner,
    /// Control frames (results, claims, residency answers, heartbeats) waiting
    /// for the link writer. Unbounded on purpose, since a result is never
    /// dropped, so this is the reading that says a stalled link is backing
    /// them up. Bytes count each message's inline size only. Defaulted like
    /// the field above.
    #[serde(default)]
    pub outgoing_control_queue: RetainedOwner,
}

impl ExecutorRetainedState {
    /// Every owner in a stable order, so the renderer and any future consumer
    /// cannot drift into disagreeing about what was measured.
    pub fn owners(&self) -> [(RetainedOwnerKind, RetainedOwner); 22] {
        [
            (RetainedOwnerKind::QueuedRequests, self.queued_requests),
            (
                RetainedOwnerKind::ExecutingRequests,
                self.executing_requests,
            ),
            (
                RetainedOwnerKind::CompletedRequests,
                self.completed_requests,
            ),
            (RetainedOwnerKind::Cancellations, self.cancellations),
            (RetainedOwnerKind::CommandProcesses, self.command_processes),
            (
                RetainedOwnerKind::ResidentProcesses,
                self.resident_processes,
            ),
            (RetainedOwnerKind::CommandOwnership, self.command_ownership),
            (RetainedOwnerKind::Projects, self.projects),
            (RetainedOwnerKind::Cells, self.cells),
            (RetainedOwnerKind::WaitSamples, self.wait_samples),
            (
                RetainedOwnerKind::ResidencyOperationLocks,
                self.residency_operation_locks,
            ),
            (
                RetainedOwnerKind::ResidencyAcquisitionLocks,
                self.residency_acquisition_locks,
            ),
            (
                RetainedOwnerKind::ResidencyReclaimContention,
                self.residency_reclaim_contention,
            ),
            (RetainedOwnerKind::CallbackWaiters, self.callback_waiters),
            (
                RetainedOwnerKind::OutgoingCallbackQueue,
                self.outgoing_callback_queue,
            ),
            (
                RetainedOwnerKind::ActiveProcessAttempts,
                self.active_process_attempts,
            ),
            (
                RetainedOwnerKind::CapturedProcessOutput,
                self.captured_process_output,
            ),
            (
                RetainedOwnerKind::QueuedOutputChunks,
                self.queued_output_chunks,
            ),
            (RetainedOwnerKind::ManagedManifests, self.managed_manifests),
            (RetainedOwnerKind::ProcessCensus, self.process_census),
            (RetainedOwnerKind::PendingSnapshot, self.pending_snapshot),
            (
                RetainedOwnerKind::OutgoingControlQueue,
                self.outgoing_control_queue,
            ),
        ]
    }

    /// The sum of every owner's estimate. A floor on what the daemon is
    /// holding, never a total: state this does not name is state this cannot
    /// count, and the gap between this and resident size is itself a reading.
    pub fn total_estimated_bytes(&self) -> u64 {
        self.owners().into_iter().fold(0_u64, |total, (_, owner)| {
            total.saturating_add(owner.estimated_bytes)
        })
    }

    /// Whether anything has been collected at all. A defaulted snapshot — an
    /// executor too old to publish one — must render as "not reported" rather
    /// than as a daemon holding nothing.
    pub fn is_unreported(&self) -> bool {
        self.measured_at_unix_ms == 0 && self.owners().into_iter().all(|(_, o)| o.is_untouched())
    }
}

/// The placement-facing vocabulary every enrolled machine speaks.
///
/// Each reading is independently timestamped and independently able to be a
/// named gap, because they are collected on different clocks: memory and CPU are
/// cheap enough to resample with liveness, while the categorized disk walk is
/// recursive and refreshes on its own bounded interval. Occupancy is not here —
/// it is not sampled, and `admission`/`inventory` on the same snapshot already
/// carry it exactly.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MachineTelemetry {
    pub cpu: Measurement<CpuPressure>,
    pub memory: Measurement<MachineMemory>,
    pub volume: Measurement<MachineVolume>,
    pub disk_accounting: Measurement<DiskAccounting>,
    pub process: ProcessTelemetry,
}

/// The stable name of a machine reading, used wherever a gap has to be reported
/// by name rather than by position.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub enum MachineMeasurement {
    Cpu,
    Memory,
    Volume,
    DiskAccounting,
    ProcessResident,
    ProcessPhysicalFootprint,
    ProcessVirtual,
    ProcessAllocator,
}

impl MachineMeasurement {
    /// Whether placement needs this reading to decide that a machine can take
    /// work.
    ///
    /// Only three readings answer that question: how hard the cores are being
    /// pushed, how much physical memory is left, and how much room is left on
    /// the volume. Everything else on [`MachineTelemetry`] is diagnosis — the
    /// categorized walk is a governance breakdown, and the process readings are
    /// the daemon's own size, which [`ProcessTelemetry`] says outright is not
    /// capacity.
    ///
    /// The distinction is load-bearing rather than tidy. Physical footprint is
    /// unavailable by construction everywhere except macOS, so letting it into
    /// the aggregate verdict would mean a perfectly healthy Windows or Linux
    /// executor could never report healthy: its one permanent, intentional,
    /// diagnostic-only gap would hold the whole fleet at unknown forever.
    pub fn is_placement_input(self) -> bool {
        match self {
            Self::Cpu | Self::Memory | Self::Volume => true,
            Self::DiskAccounting
            | Self::ProcessResident
            | Self::ProcessPhysicalFootprint
            | Self::ProcessVirtual
            | Self::ProcessAllocator => false,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Cpu => "cpu",
            Self::Memory => "memory",
            Self::Volume => "volume",
            Self::DiskAccounting => "diskAccounting",
            Self::ProcessResident => "processResident",
            Self::ProcessPhysicalFootprint => "processPhysicalFootprint",
            Self::ProcessVirtual => "processVirtual",
            Self::ProcessAllocator => "processAllocator",
        }
    }
}

impl MachineTelemetry {
    /// Every reading that is currently a named gap, in a stable order.
    ///
    /// One place derives this so the panel and the placement seam cannot drift
    /// into disagreeing about which readings a machine is missing.
    pub fn gaps(&self) -> Vec<(MachineMeasurement, MeasurementGap)> {
        [
            (MachineMeasurement::Cpu, self.cpu.gap()),
            (MachineMeasurement::Memory, self.memory.gap()),
            (MachineMeasurement::Volume, self.volume.gap()),
            (
                MachineMeasurement::DiskAccounting,
                self.disk_accounting.gap(),
            ),
            (
                MachineMeasurement::ProcessResident,
                self.process.resident_bytes.gap(),
            ),
            (
                MachineMeasurement::ProcessPhysicalFootprint,
                self.process.physical_footprint_bytes.gap(),
            ),
            (
                MachineMeasurement::ProcessVirtual,
                self.process.virtual_bytes.gap(),
            ),
            (
                MachineMeasurement::ProcessAllocator,
                self.process.allocator.gap(),
            ),
        ]
        .into_iter()
        .filter_map(|(measurement, gap)| gap.map(|gap| (measurement, gap)))
        .collect()
    }

    /// The gaps that describe whether this machine can take work, as distinct
    /// from the ones that merely describe the daemon or the storage breakdown.
    ///
    /// This is what a fleet-wide verdict is allowed to be built from. See
    /// [`MachineMeasurement::is_placement_input`] for why the difference
    /// matters.
    pub fn placement_gaps(&self) -> Vec<(MachineMeasurement, MeasurementGap)> {
        self.gaps()
            .into_iter()
            .filter(|(measurement, _)| measurement.is_placement_input())
            .collect()
    }

    /// The oldest age among readings that actually carry a value, or `None` when
    /// nothing has been measured. This is telemetry age, never link age.
    pub fn oldest_measured_age_ms(&self, now_unix_ms: u64) -> Option<u64> {
        [
            self.cpu.value().map(|_| self.cpu.age_ms(now_unix_ms)),
            self.memory.value().map(|_| self.memory.age_ms(now_unix_ms)),
            self.volume.value().map(|_| self.volume.age_ms(now_unix_ms)),
            self.disk_accounting
                .value()
                .map(|_| self.disk_accounting.age_ms(now_unix_ms)),
        ]
        .into_iter()
        .flatten()
        .max()
    }
}

/// Admission-pressure evidence and Tokio runtime diagnostics for the executor
/// daemon.
///
/// What this is *not* is machine capacity: the numbers placement reads live on
/// [`MachineTelemetry`], timestamped, where a missing one is a named gap rather
/// than a null.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct HostHealth {
    pub pressure: Option<HostPressureEvidence>,
    /// The executor's sustained-pressure verdict. This is separate from the
    /// latest machine reading because admission acts on hysteresis, not a
    /// single sample.
    #[serde(default)]
    pub cpu_admission: CpuAdmissionHealth,
    pub logical_cores: Option<usize>,
    pub tokio_worker_count: Option<usize>,
    pub tokio_alive_tasks: Option<usize>,
    pub tokio_global_queue_depth: Option<usize>,
    /// How stalled the host's storage is. A machine whose disk is timing out
    /// writes is slower for a build than its idle cores say, and nothing else
    /// this report carries shows it. Defaulted, and so an unsampled gap rather
    /// than a zero, from an executor too old to report it.
    #[serde(default)]
    pub io_pressure: Measurement<IoPressure>,
}

/// The host's storage stall, as Linux's pressure-stall information and block
/// device counters report it.
///
/// Kernel I/O timeout and error counts are deliberately absent: their only
/// source is the kernel log, and whether an unprivileged process may read it
/// is a property of each host's configuration rather than something an
/// executor can promise.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub struct IoPressure {
    /// Thousandths of wall time in which every non-idle task was stalled on
    /// I/O at once (PSI `full`), averaged over ten seconds.
    pub full_avg10_millis: u32,
    /// The same, averaged over sixty seconds.
    pub full_avg60_millis: u32,
    /// The whole device whose writes were slowest over the last sample window,
    /// or `None` before a window exists or when no device wrote or held I/O.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slowest_device: Option<DeviceWriteLatency>,
}

/// One block device's writes across a sample window.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub struct DeviceWriteLatency {
    pub device: String,
    /// Mean milliseconds each write completed in the window took. `None` for a
    /// device that completed no I/O at all while busy for nearly the whole
    /// window: stalled rather than slow.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mean_write_ms: Option<u64>,
    pub writes: u64,
    /// I/Os in progress on the device when the window closed.
    pub in_flight: u64,
    pub window_ms: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DiskCategoryAccounting {
    pub managed_objects_bytes: u64,
    #[serde(rename = "liveSlotsBytes")]
    pub live_cells_bytes: u64,
    pub warm_caches_bytes: u64,
    #[serde(default)]
    pub external_managed_caches_bytes: u64,
    pub quarantines_bytes: u64,
    pub temporary_other_bytes: u64,
}

/// Whether a sweep in the executor's current process has actually retried an
/// alarm, or whether it was restored from durable state and not yet re-observed.
///
/// The distinction is what stops a restart from laundering an alarm into either
/// a fresh failure or a silent recovery. A restored alarm is outstanding — it
/// degrades health from the moment the process comes up — but it is honestly
/// marked as not yet retried until a sweep has tried the path again.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum StorageAlarmVerification {
    Observed,
    RestoredUnverified,
}

/// One reclamation the executor's janitor has permanently given up on.
///
/// "Permanently" is the janitor's own bounded-retry verdict rather than a guess:
/// the tree survived every retry the sweep is willing to spend on it. It is
/// carried as governance data rather than left in the log because the log is not
/// a surface anyone watches — two of these sat unread for sixteen days while the
/// executor resource rendered `Storage verdict: Ok` (CAIRN-4217).
///
/// Cairn never remediates one of these on its own. Deleting, chmod-ing, or
/// chown-ing a path a human is holding as evidence is destructive and
/// operator-owned, so every field here exists to let a person decide, and the
/// decision stays theirs.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StorageReclamationAlarm {
    pub path: String,
    /// When bounded retries were first exhausted, which is the alarm's age.
    pub first_permanent_unix_ms: u64,
    pub attempts: u32,
    pub bytes: u64,
    pub last_error: String,
    /// A bounded description of what is still inside the tree.
    pub survivors: String,
    pub verification: StorageAlarmVerification,
}

/// What the executor's janitor can say about permanently obstructed reclamation.
///
/// `None` on [`DiskHealth::reclamation`] is the absence of a claim: an older
/// peer, or one that has not completed a sweep since starting. An `outstanding`
/// of zero is a positive claim that nothing is outstanding. Collapsing the two
/// would turn every silent peer into a healthy one, which is precisely the
/// failure this type exists to prevent.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StorageReclamationHealth {
    pub outstanding: usize,
    /// Oldest alarm first, bounded: the wire never carries an unbounded array.
    /// `outstanding` stays truthful when this is truncated.
    pub alarms: Vec<StorageReclamationAlarm>,
}

impl StorageReclamationHealth {
    pub fn is_outstanding(&self) -> bool {
        self.outstanding > 0
    }
}

/// Why the categorized storage accounting is as old as it is.
///
/// The categorized walk is a safety net priced from its own cost rather than a
/// fixed tick, so its age is a policy outcome rather than a fault. Publishing
/// the cadence is what keeps a deliberately long interval from reading as a
/// stalled subsystem.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StorageAccountingCadence {
    pub last_pass_duration_ms: Option<u64>,
    /// Total category movement the last pass found against the totals it
    /// inherited. Zero is what lets the interval grow.
    pub last_drift_bytes: Option<u64>,
    pub interval_ms: Option<u64>,
    pub next_due_unix_ms: Option<u64>,
    pub consecutive_failures: u32,
}

/// Storage governance: the budget, the derived pressure verdict, and the
/// janitor's state.
///
/// The bytes themselves are measurements and live on
/// [`MachineTelemetry::volume`] and [`MachineTelemetry::disk_accounting`], where
/// each carries its own collection time. `status` is derived from the volume
/// reading, so `Unknown` here means that reading is a gap.
///
/// `status` and [`Self::reclamation`] are deliberately separate verdicts. The
/// first is physical — how much room is left on the volume — and the second is
/// custodial: whether the executor still owns bytes it has admitted it cannot
/// reclaim. A volume with plenty of room and an unreclaimable tree is `Ok` and
/// degraded at once, and flattening that into one enum is what let the custodial
/// half go unreported.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DiskHealth {
    pub budget_bytes: Option<u64>,
    pub status: DiskHealthStatus,
    pub sweep_status: StorageSweepStatus,
    pub sweep_generation: u64,
    pub cleanup_blocked: bool,
    pub cleanup_last_error: Option<String>,
    pub cleanup_failing_path: Option<String>,
    /// How many entries the last completed sweep could not list, measure, or
    /// remove and that are still on disk. Entries that vanished under the
    /// sweep are churn and are not counted.
    pub cleanup_skipped_entries: Option<usize>,
    /// Which entries those were, bounded. A sweep that skipped something is
    /// partial, and a count alone cannot say whether it skipped a busy temp
    /// file or a 44 GB quarantine it will never reclaim.
    #[serde(default)]
    pub cleanup_skipped: Vec<SkippedEntry>,
    /// Skips beyond the retained bound.
    #[serde(default)]
    pub cleanup_skipped_truncated: usize,
    /// Permanently obstructed reclamation, or `None` when this peer does not
    /// report it at all. Absence is not a clean bill of health.
    #[serde(default)]
    pub reclamation: Option<StorageReclamationHealth>,
    /// The schedule the categorized walk is running on, or `None` when this peer
    /// does not report it.
    #[serde(default)]
    pub accounting_cadence: Option<StorageAccountingCadence>,
    /// The admission floor `status` was computed against: `pressured` below it.
    /// Zero from a peer that does not report its thresholds.
    #[serde(default)]
    pub watermark_bytes: u64,
    /// The critical line: at or below it the pressure tier lifts its cold-age
    /// restriction, because admission is close.
    #[serde(default)]
    pub critical_free_bytes: u64,
    /// The pressure line: at or below it `status` reads `tight` and the
    /// pressure tier reclaims toward it.
    #[serde(default)]
    pub pressure_free_bytes: u64,
    /// The last pressure pass, or `None` when none has run since this executor
    /// started.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pressure: Option<StoragePressureReport>,
    /// Quarantine build output the pressure tier removed, newest last and
    /// bounded. A forensic hold whose `target/` was given up says so here as
    /// well as in a marker inside the tree.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub quarantine_targets_stripped: Vec<QuarantineTargetStrip>,
    /// The lineage containers the occupancy probe counts as the executor's
    /// own, or `None` from a peer that does not report them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub occupancy_lineage: Option<OccupancyLineageReport>,
}

/// Which kernel containers the executor believes are its own lineage. A
/// process whose holdings the kernel withholds is left out of the occupancy
/// probe only when its container is outside every one of these.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct OccupancyLineageReport {
    /// The container this executor process runs in, rendered (`coalition
    /// 19563`, `cgroup /user.slice/…/cairn-exec.service`). `None` when it
    /// could not be read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current: Option<String>,
    /// Every container counted as the executor's since boot, `current`
    /// included: earlier launches' coalitions stay in scope.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub containers: Vec<String>,
    /// Why the lineage is unknown, when it is. No withheld process is then
    /// left out, so any one makes a tree's occupancy unknown.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unknown: Option<String>,
}

/// Which band of free space a pressure pass ran in.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum StoragePressureBand {
    /// At or below the pressure line: only cold idle cells are reclaimed.
    Pressure,
    /// At or below the critical line: any idle cell is reclaimable, coldest
    /// first, because admission is close.
    Critical,
}

/// Whether a pressure pass found what it asked for.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum StoragePressureOutcome {
    Relieved,
    /// Every rung policy permits ran and the deficit remains. The next pass
    /// waits for the housekeeping interval, or for free space to fall below
    /// `rearmBelowFreeBytes`, rather than stripping warm cells every minute
    /// for pressure that is not Cairn's.
    Unrelieved,
    /// A rung stopped early because work arrived. Nothing is concluded about
    /// what remains reclaimable, and the next pass runs on the ordinary
    /// pressure cadence.
    Yielded,
}

/// One rung of the pressure ladder, cheapest recovery first.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum StorageReclaimRung {
    /// Discards and quarantines past their forensic window.
    RetiredTrees,
    /// Cairn-managed caches outside the executor home.
    ManagedCaches,
    /// Build output inside quarantines still in their forensic window.
    QuarantineTargets,
    /// Build output inside idle live cells.
    IdleTargets,
    /// Whole idle cells.
    RetiredCells,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StorageRungReclaim {
    pub rung: StorageReclaimRung,
    pub bytes: u64,
    pub count: usize,
}

/// A tree the pressure tier would have reclaimed from and did not, because
/// the operating system said something was using it, or could not say.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct OccupiedTreeSkip {
    /// Executor-home-relative.
    pub path: String,
    /// The process holding it, or the process whose holdings could not be
    /// read when `unknown`. `None` only when no single process is to blame.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    /// Occupancy could not be determined, which skips the tree exactly as a
    /// live holder does.
    #[serde(default)]
    pub unknown: bool,
    /// The rule that fired. `None` from a peer that predates it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rule: Option<OccupancyRule>,
    /// The kernel's short name for the process `pid` names.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process: Option<String>,
    /// What that process runs, when the kernel said and the rule is a
    /// withheld one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executable: Option<String>,
    /// For a held rule, what is held: tree-relative when inside the tree.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub held_path: Option<String>,
    /// For a withheld rule, the process's lineage container when it was read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container: Option<String>,
}

/// Why the occupancy probe skipped a tree. The first three are holds a live
/// process has; the rest are the ways occupancy can be unknown.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum OccupancyRule {
    /// Its working directory is inside the tree.
    Cwd,
    /// The binary it runs lives inside the tree.
    Executable,
    /// It holds a file inside the tree open.
    OpenFile,
    /// The kernel withholds its holdings and it runs in the executor's own
    /// lineage container.
    WithheldInLineage,
    /// The kernel withholds its holdings and its lineage container could not
    /// be read.
    WithheldContainerUnreadable,
    /// Reading it failed for a reason other than a permission refusal.
    ReadFailed,
    /// The kernel withholds its holdings and the executor's own lineage is
    /// unknown.
    LineageUnknown,
    /// The process table could not be read.
    ProcessTableUnreadable,
    /// This platform cannot say which processes hold files.
    PlatformUnsupported,
}

/// A process the occupancy probe did not consult: the kernel withheld its
/// holdings, and its lineage container is not the executor's.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct WithheldProcess {
    pub pid: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executable: Option<String>,
    /// Its lineage container, rendered.
    pub container: String,
}

/// What the last pressure pass asked for, what each rung gave, and whether it
/// was enough.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StoragePressureReport {
    pub at_unix_ms: u64,
    pub band: StoragePressureBand,
    /// The deficit the pass reclaimed toward: the pressure line plus slack,
    /// less free space when it started.
    pub target_bytes: u64,
    pub free_bytes_before: u64,
    pub free_bytes_after: u64,
    pub reclaimed_by_rung: Vec<StorageRungReclaim>,
    pub outcome: StoragePressureOutcome,
    /// After an unrelieved pass, the free-space level that re-arms the
    /// pressure cadence early.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rearm_below_free_bytes: Option<u64>,
    /// Bytes Cairn holds that policy forbade reclaiming in this pass: the sum
    /// of [`Self::withheld`] from a peer that reports it.
    #[serde(default)]
    pub held_bytes: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub occupied_skips: Vec<OccupiedTreeSkip>,
    /// What the pass left on the volume that Cairn holds, by the reason it
    /// could not or would not take it. Empty from a peer that predates it.
    ///
    /// Absence of a reason reads as "nothing left to do", so a pass that falls
    /// short names every class of bytes it did not reach: that is what tells
    /// the operator whether the remaining relief is Cairn's or theirs.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub withheld: Vec<StorageWithheld>,
}

/// One class of Cairn-held bytes a pressure pass left in place.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StorageWithheld {
    pub reason: StorageWithheldReason,
    pub bytes: u64,
    /// Trees, cells, or cache partitions.
    pub count: usize,
    /// The class is present but nothing has sized it yet, so `bytes` is a
    /// floor, not a measurement. Present rather than omitted, because an
    /// omitted class reads as nothing held.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub unmeasured: bool,
}

/// Why a pressure pass left Cairn-held bytes in place.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum StorageWithheldReason {
    /// Quarantine trees inside their forensic window, after the pressure tier
    /// took what build output it could from them. Released when the window
    /// expires.
    ForensicWindow,
    /// Build cells fenced in place because processes from an ended epoch still
    /// run in them. The executor signals only processes it can prove it
    /// launched, so these are released when those processes exit.
    FencedCells,
    /// Managed caches written inside the active window, or held by a build, a
    /// reader, or a publication. Released when that use ends.
    CachesInUse,
    /// Managed caches the pass did not take: its ask was met first, or the
    /// pressure band's floor kept them. The critical band reaches below the
    /// floor.
    CachesKept,
    /// Cairn-managed caches whose writers do not take the executor's leases
    /// (CI targets, the immutable asset cache, the sccache daemon's store,
    /// which enforces its own ceiling). Accounted, never deleted by the
    /// executor; sized as of the last accounting walk, and `count` is the
    /// roots present. Released only by their owners' own retention.
    CachesOwnerRetained,
}

/// One quarantine whose build output the pressure tier removed.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct QuarantineTargetStrip {
    /// Executor-home-relative quarantine root.
    pub path: String,
    pub bytes: u64,
    pub at_unix_ms: u64,
    /// Processes the occupancy probe left out under the lineage rule when it
    /// answered vacant for this tree, bounded: the evidence of what the strip
    /// did not consult.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub withheld_outside_lineage: Vec<WithheldProcess>,
    /// Withheld processes beyond the bound.
    #[serde(default)]
    pub withheld_outside_lineage_truncated: usize,
}

impl Default for DiskHealth {
    fn default() -> Self {
        Self {
            budget_bytes: None,
            status: DiskHealthStatus::Unknown,
            sweep_status: StorageSweepStatus::NotStarted,
            sweep_generation: 0,
            cleanup_blocked: false,
            cleanup_last_error: None,
            cleanup_failing_path: None,
            cleanup_skipped_entries: None,
            cleanup_skipped: Vec::new(),
            cleanup_skipped_truncated: 0,
            reclamation: None,
            accounting_cadence: None,
            watermark_bytes: 0,
            critical_free_bytes: 0,
            pressure_free_bytes: 0,
            pressure: None,
            quarantine_targets_stripped: Vec::new(),
            occupancy_lineage: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ExecutorRuntimePolicy {
    pub memory_budget_bytes: Option<u64>,
    pub disk_growth_budget_bytes: Option<u64>,
    pub free_disk_watermark_bytes: u64,
    pub concurrency_units: u32,
    pub maximum_queue_depth: usize,
    /// Idle cells retained per project once the volume is measurably pressured.
    #[serde(default = "default_idle_retention_floor_per_project")]
    pub idle_retention_floor_per_project: usize,
    /// Idle cells retained per project while the volume is not pressured.
    #[serde(default = "default_idle_retention_ceiling_per_project")]
    pub idle_retention_ceiling_per_project: usize,
    /// Machine-wide idle-cell budget under measured disk pressure.
    #[serde(default = "default_idle_retention_floor_total")]
    pub idle_retention_floor_total: usize,
    /// Machine-wide idle-cell budget while disk is not pressured.
    #[serde(default = "default_idle_retention_ceiling_total")]
    pub idle_retention_ceiling_total: usize,
    /// The pressure line: free bytes at or below which the storage sweep
    /// reclaims toward it and the volume verdict reads `tight`.
    #[serde(default = "default_idle_retention_pressure_free_bytes")]
    pub idle_retention_pressure_free_bytes: u64,
    /// The critical line, between the watermark and the pressure line: at or
    /// below it the pressure tier may reclaim any idle cell, not only cold ones.
    #[serde(default = "default_storage_critical_free_bytes")]
    pub storage_critical_free_bytes: u64,
    /// How long a quarantined build cell is held for forensics before the
    /// storage sweep reclaims it.
    #[serde(default = "default_quarantine_forensic_window_ms")]
    pub quarantine_forensic_window_ms: u64,
    /// Bytes of build output one build cell may hold before a slot-target
    /// reclaim empties it outright rather than trimming it.
    #[serde(default = "default_slot_target_max_bytes")]
    pub slot_target_max_bytes: u64,
    #[serde(default)]
    pub cpu_admission: CpuAdmissionPolicy,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CpuAdmissionPolicy {
    pub entry_utilization: f64,
    pub clear_utilization: f64,
    pub entry_window_ms: u64,
    pub clear_window_ms: u64,
}

impl Default for CpuAdmissionPolicy {
    fn default() -> Self {
        Self {
            entry_utilization: 0.90,
            clear_utilization: 0.75,
            entry_window_ms: 15_000,
            clear_window_ms: 10_000,
        }
    }
}

impl CpuAdmissionPolicy {
    pub fn validate(&self) -> Result<(), String> {
        if !self.entry_utilization.is_finite() || !(0.0..=1.0).contains(&self.entry_utilization) {
            return Err(
                "CPU admission entry utilization must be a finite fraction in [0, 1]".into(),
            );
        }
        if !self.clear_utilization.is_finite() || !(0.0..=1.0).contains(&self.clear_utilization) {
            return Err(
                "CPU admission clear utilization must be a finite fraction in [0, 1]".into(),
            );
        }
        if self.clear_utilization >= self.entry_utilization {
            return Err("CPU admission clear utilization must be below entry utilization".into());
        }
        if self.entry_window_ms == 0 || self.clear_window_ms == 0 {
            return Err("CPU admission entry and clear windows must be greater than zero".into());
        }
        Ok(())
    }
}

/// Idle cells an executor keeps for one project no matter how little disk is
/// free. A warm cell is a populated build cache, and discarding the last one
/// costs that project's next command a full cold build — the cost this floor
/// exists to refuse. Byte-driven pressure retirement stops here; the cheaper
/// rungs (retired trees, managed caches, quarantine build output, idle build
/// targets) are where bytes beyond this point come from.
pub const IDLE_RETENTION_FLOOR_PER_PROJECT: usize = 1;

/// Idle cells an executor keeps for one project while disk is not pressured.
///
/// This was 1 until CAIRN-3188, inherited from the single-worktree era. Under
/// concurrent-children load a cell falls idle for moments between batches and
/// check waves, so a limit of 1 retired nearly every cell the instant it became
/// reusable: 2026-07-26 measured 238 warm-cell retirements in a day against 27
/// the day before, and build times went from roughly ten minutes to thirty.
/// Warm caches are the most valuable thing on a build volume, so retention is
/// generous by default and only measured disk pressure takes it back — in
/// bytes, not by collapsing this count. The ceiling is a runaway guard: a
/// project that cycles cells faster than the storage sweep reclaims them would
/// otherwise grow inventory without bound. It is enforced whatever the free
/// space, and it never decides what pressure reclaims (CAIRN-4860).
pub const IDLE_RETENTION_CEILING_PER_PROJECT: usize = 16;

/// Machine-wide idle-cell bounds. Per-project retention protects one project's
/// locality, but cannot bound a machine serving an unbounded number of projects.
/// The global ceiling is the count guard against runaway inventory. The floor
/// is what byte-driven pressure retirement never goes below: the two warmest
/// free cells on the machine survive any deficit, irrespective of project.
pub const IDLE_RETENTION_FLOOR_TOTAL: usize = 2;
pub const IDLE_RETENTION_CEILING_TOTAL: usize = 16;

/// The pressure line: free bytes on the executor volume at or below which the
/// storage sweep reclaims toward this line plus slack, and the volume verdict
/// reads `tight`.
///
/// Reclamation under pressure is byte-driven. Each pass asks for a deficit and
/// takes it rung by rung, cheapest recovery first, stopping the moment the
/// deficit is met: retired trees and managed caches, then the build output of
/// quarantines still in their window, then the build targets of cold idle cells,
/// and only then whole cold idle cells, never below the retention floors. A
/// pass that runs every permitted rung and still falls short — the pressure is
/// not Cairn's — backs off to the housekeeping interval rather than stripping
/// whichever cell just went idle every minute.
///
/// Deliberately far above `free_disk_watermark_bytes` (2 GiB by default), which
/// is where admission starts refusing work: warm inventory should be handed
/// back while there is still room to run, not once the volume has already
/// become an admission problem. Raising it reclaims sooner; lowering it keeps
/// caches longer at the risk of meeting the critical line and the watermark
/// first.
pub const IDLE_RETENTION_PRESSURE_FREE_BYTES: u64 = 64 * 1024 * 1024 * 1024;

/// The critical line: free bytes at or below which the pressure tier lifts its
/// cold-age restriction and may reclaim any idle cell, coldest first.
///
/// Above it, a cell used in the last hour keeps its build: tight is a price,
/// and a warm cell is worth more than headroom nobody has asked for yet. At it,
/// admission is one large build away, and the cost of a refusal outweighs one
/// cold rebuild. Sits between the watermark and the pressure line, and the
/// runtime policy refuses any other order.
pub const STORAGE_CRITICAL_FREE_BYTES: u64 = 16 * 1024 * 1024 * 1024;

/// The admission floor: free bytes below which the executor refuses work, and
/// the volume verdict reads `pressured`. One default for every reader, so the
/// executor's initial value and the runtime policy's cannot drift apart.
pub const FREE_DISK_WATERMARK_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// Bytes of build output one build cell may hold before a slot-target reclaim
/// stops trimming it and empties it.
///
/// A runaway guard, not an operating point. This was 40 GiB and was enforced on
/// every hold release, which put it under the roughly 56 GiB a full Cairn test
/// build leaves behind: every completed build was over budget by construction,
/// so releasing a slot deleted the build that had just finished and the next
/// lane recompiled it. One night measured 484 GB reclaimed in five hours while
/// the volume still had 414 GiB free (CAIRN-4349). Reclamation is a disk
/// pressure tier now, and this number only bounds a cell that has grown past
/// any plausible build, so it has to sit above a full test build of the largest
/// project the machine serves. Below that it is not a budget at all — it is a
/// delete-everything policy with a number attached.
pub const SLOT_TARGET_MAX_BYTES: u64 = 96 * 1024 * 1024 * 1024;

/// How long a quarantined build cell is held before the storage sweep reclaims
/// it.
///
/// Quarantine is a forensic hold, not an exemption from the disk budget. A cell
/// is moved aside when it fails validation or is retired, and the tree is worth
/// keeping only while someone might still read it to explain the incident that
/// produced it. Days, because a failure noticed on a Friday should still be
/// diagnosable on a Monday; not weeks, because the trees are whole build caches
/// and cost tens of gibibytes each.
///
/// The window protects the evidence: the tree, its state files, and its logs.
/// Nothing — aggregate size, disk pressure, an admission that would otherwise
/// be refused — shortens it for those, because a hold that yields when the
/// volume gets tight is not a hold, and the incidents worth keeping evidence
/// for are exactly the ones that fill a disk.
///
/// Build output inside a held quarantine is not evidence. It is derived state
/// and is rematerialized, never preserved (CAIRN-2859), and a full Cairn
/// `target/` is most of a quarantine's bytes. So under the pressure line, and
/// before any live cell's build is touched, the storage sweep removes the build
/// targets inside in-window quarantines — only after the operating system
/// reports no process with a working directory, executable, or open file inside
/// the tree, and never when that cannot be determined. The removal is recorded
/// in a marker inside the quarantine and on the executor's disk health, so the
/// tree still says what happened to it (CAIRN-4860).
pub const QUARANTINE_FORENSIC_WINDOW_MS: u64 = 7 * 24 * 60 * 60 * 1_000;

const fn default_quarantine_forensic_window_ms() -> u64 {
    QUARANTINE_FORENSIC_WINDOW_MS
}

const fn default_idle_retention_floor_per_project() -> usize {
    IDLE_RETENTION_FLOOR_PER_PROJECT
}

const fn default_idle_retention_ceiling_per_project() -> usize {
    IDLE_RETENTION_CEILING_PER_PROJECT
}

const fn default_idle_retention_floor_total() -> usize {
    IDLE_RETENTION_FLOOR_TOTAL
}

const fn default_idle_retention_ceiling_total() -> usize {
    IDLE_RETENTION_CEILING_TOTAL
}

const fn default_idle_retention_pressure_free_bytes() -> u64 {
    IDLE_RETENTION_PRESSURE_FREE_BYTES
}

const fn default_slot_target_max_bytes() -> u64 {
    SLOT_TARGET_MAX_BYTES
}

const fn default_storage_critical_free_bytes() -> u64 {
    STORAGE_CRITICAL_FREE_BYTES
}

impl Default for ExecutorRuntimePolicy {
    fn default() -> Self {
        Self {
            memory_budget_bytes: None,
            disk_growth_budget_bytes: None,
            free_disk_watermark_bytes: FREE_DISK_WATERMARK_BYTES,
            concurrency_units: u32::MAX,
            maximum_queue_depth: 512,
            idle_retention_floor_per_project: default_idle_retention_floor_per_project(),
            idle_retention_ceiling_per_project: default_idle_retention_ceiling_per_project(),
            idle_retention_floor_total: default_idle_retention_floor_total(),
            idle_retention_ceiling_total: default_idle_retention_ceiling_total(),
            idle_retention_pressure_free_bytes: default_idle_retention_pressure_free_bytes(),
            storage_critical_free_bytes: default_storage_critical_free_bytes(),
            quarantine_forensic_window_ms: default_quarantine_forensic_window_ms(),
            slot_target_max_bytes: default_slot_target_max_bytes(),
            cpu_admission: CpuAdmissionPolicy::default(),
        }
    }
}

impl ExecutorRuntimePolicy {
    pub fn validate(&self) -> Result<(), String> {
        self.cpu_admission.validate()?;
        if self.concurrency_units == 0 {
            return Err("executor concurrency units must be greater than zero".into());
        }
        if self.maximum_queue_depth == 0 {
            return Err("executor maximum queue depth must be greater than zero".into());
        }
        if self.idle_retention_floor_per_project == 0 {
            return Err(
                "executor idle-retention floor per project must be greater than zero".into(),
            );
        }
        if self.idle_retention_ceiling_per_project < self.idle_retention_floor_per_project {
            return Err(
                "executor idle-retention ceiling per project must be at least the floor".into(),
            );
        }
        if self.idle_retention_floor_total == 0 {
            return Err(
                "executor machine-wide idle-retention floor must be greater than zero".into(),
            );
        }
        if self.idle_retention_ceiling_total < self.idle_retention_floor_total {
            return Err(
                "executor machine-wide idle-retention ceiling must be at least the floor".into(),
            );
        }
        if self.idle_retention_pressure_free_bytes == 0 {
            return Err(
                "executor idle-retention disk-pressure threshold must be greater than zero".into(),
            );
        }
        if self.free_disk_watermark_bytes == 0 {
            return Err("executor free-disk watermark must be greater than zero".into());
        }
        // One policy for one question, ordered: admission refuses below the
        // watermark, the pressure tier lifts its cold-age restriction at the
        // critical line, and reclamation starts at the pressure line.
        if self.storage_critical_free_bytes <= self.free_disk_watermark_bytes {
            return Err(
                "executor storage critical line must be above the free-disk watermark".into(),
            );
        }
        if self.storage_critical_free_bytes >= self.idle_retention_pressure_free_bytes {
            return Err("executor storage critical line must be below the pressure line".into());
        }
        if self.quarantine_forensic_window_ms == 0 {
            return Err("executor quarantine forensic window must be greater than zero".into());
        }
        if self.slot_target_max_bytes == 0 {
            return Err("executor slot-target ceiling must be greater than zero".into());
        }
        if self.memory_budget_bytes == Some(0) {
            return Err("executor memory budget must be greater than zero when configured".into());
        }
        if self.disk_growth_budget_bytes == Some(0) {
            return Err(
                "executor disk-growth budget must be greater than zero when configured".into(),
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BuildSkew {
    pub runner_build_id: String,
    pub executor_build_id: String,
}

/// Everything a machine continuously posts about itself, so the runner can
/// route from state the machine asserted rather than from a forecast the runner
/// computed about it.
///
/// Free memory and free disk live in exactly one place here —
/// [`MachineTelemetry::memory`] and [`MachineTelemetry::volume`], each a
/// timestamped [`Measurement`]. The `availableBytes`/`freeBytes` inside a
/// [`HostPressureCondition`] are not a second home: they are the comparison that
/// produced a pressure verdict, and a floor without the reading it was compared
/// against is unreadable. [`DiskHealth`] carries budgets and sweep state and no
/// free-byte count at all.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ExecutorSubstrateReport {
    pub admission: AdmissionHealth,
    /// What this machine has already committed itself to, as the machine counts
    /// it.
    ///
    /// Distinct from [`AdmissionHealth::active_reservation`], which counts only
    /// work that has started. This also counts claims held but not yet granted,
    /// which is the window in which a second offer would otherwise be sized
    /// against capacity that is already spoken for.
    #[serde(default)]
    pub allocation: ExecutorAllocation,
    /// This machine's own answer to "when will something free up", in
    /// milliseconds from now.
    ///
    /// It replaces a forecast the runner used to compute by iterating the
    /// machine's reported executing set and pricing each entry against learned
    /// estimates. The machine knows when each of its own requests actually
    /// started; the runner only knew when it dispatched them. `None` when
    /// nothing is running, or when nothing running carries an estimate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub predicted_relief_ms: Option<u64>,
    /// Every slot this machine holds, warm or idle — the inventory cache
    /// distance is ranked over.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub slots: Vec<SlotState>,
    pub queues: Vec<QueueClassHealth>,
    pub host: HostHealth,
    pub disk: DiskHealth,
    /// The placement-facing machine readings, each with its own collection time.
    #[serde(default)]
    pub machine: MachineTelemetry,
    #[serde(default)]
    pub inventory: CellInventoryHealth,
    #[serde(default)]
    pub applied_policy: ExecutorRuntimePolicy,
    #[serde(default)]
    pub drain_mode: bool,
    /// Physical process ownership reconciled against durable resident records.
    #[serde(default)]
    pub resident_processes: ResidentProcessHealth,
    /// Physical ownership of transient command batches. Kept separate from
    /// resident generations and restartability by design.
    #[serde(default)]
    pub command_processes: CommandProcessHealth,
    /// Retained cells this executor released instead of adopting, most recent
    /// last. Bounded, and cleared by nothing but a restart.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evicted_cells: Vec<EvictedCellReport>,
}

/// What a machine has committed, counted by the machine.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ExecutorAllocation {
    pub committed_concurrency_units: u32,
    pub committed_memory_bytes: u64,
    /// Whether this machine actually bounds the width of an elastic task.
    ///
    /// False today, and it must stay false until that enforcement exists and has
    /// been measured. An allocation is only real where its enforcement is
    /// measured; reporting `true` ahead of the mechanism would be exactly the
    /// unearned claim that routing from posted state exists to stop making.
    pub enforced_width: bool,
}

/// One execution environment this machine holds, projected from its own durable
/// cell state.
///
/// A projection rather than the whole [`PersistentCellState`]: placement ranks
/// cache distance, which needs the project, the commit the tree stands on, how
/// recently it was touched, how much room it costs, and whether it can be
/// reused at all.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SlotState {
    #[serde(rename = "slotId")]
    pub cell_id: String,
    pub project_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub head_commit: Option<String>,
    /// The last observation and its target; placement must measure its own target.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_distance: Option<crate::cache_distance::MeasuredDistance>,
    pub last_used_unix_ms: u64,
    /// Maintained incrementally by storage accounting as bytes actually change,
    /// and reconciled by the periodic sweep. Never a directory walk on the
    /// heartbeat path: that is what stalled liveness refresh once already.
    /// Zero from a cell nothing has measured yet, which is the honest answer
    /// rather than a guess.
    #[serde(default)]
    pub size_bytes: u64,
    pub lifecycle: PersistentCellLifecycle,
    /// The cell's footprint (checkout plus scratch) as the last categorized walk
    /// measured it, held or not. Logical bytes, and as old as
    /// `measuredAtUnixMs`. `None` when no complete walk has priced this cell.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub measured_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub measured_at_unix_ms: Option<u64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ResidentProcessHealth {
    pub tracked_live_count: usize,
    pub reaped_stale_count: usize,
    pub unverified_stale_count: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oldest_stale_age_ms: Option<u64>,
}

/// A retained execution environment adoption gave up rather than took.
///
/// An executor coming back finds the cells its predecessor held on disk and
/// re-adopts them. A cell whose recorded state cannot be made adoptable on this
/// machine — a checkout standing on objects the store no longer holds, a
/// residency nothing can reconcile — is released instead, and its holder
/// re-acquires an environment through ordinary placement.
///
/// It is reported rather than only logged because eviction discards whatever
/// the checkout held that was never committed. An owner whose environment was
/// rebuilt underneath it is owed the reason.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct EvictedCellReport {
    pub project_id: String,
    #[serde(rename = "slotId")]
    pub cell_id: String,
    /// The residency holder that must re-acquire, as its storage key
    /// (`job:…`, `service:…`). Empty when the cell's state named none.
    #[serde(default)]
    pub holder: String,
    /// The validation failure, verbatim, so the report and the executor's log
    /// line cannot tell different stories.
    pub reason: String,
    pub evicted_at_unix_ms: u64,
    /// True when the cell was refused but its tree or authority could not be
    /// retired, so the same cell is still on disk and the next restart will meet
    /// it again. The refusal is durable; the release is not. Defaults false,
    /// which is what a report that does not mention it means.
    #[serde(default)]
    pub retirement_failed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ExecutorHealthSnapshot {
    pub identity: ExecutorIdentity,
    /// Canonical public address used by fleet resources and inspection lookup.
    pub public_name: String,
    /// True for the executor the runner supervises inside its own process tree.
    /// Everything else attached to this fleet is an enrolled executor, so work
    /// placed there is attributed to it rather than read as ambient local work.
    #[serde(default)]
    pub colocated: bool,
    /// The health of the *link*, derived from heartbeat age alone. An executor
    /// beating on time is `Online` even when the facts it carries have aged;
    /// that is `telemetry_stale`, and conflating the two reports a healthy
    /// machine as a dead one.
    pub status: ExecutorHealthStatus,
    pub heartbeat_age_ms: u64,
    /// How long ago the advertised facts were measured, as distinct from how
    /// long ago the beat carrying them arrived. `None` when the executor makes
    /// no claim.
    #[serde(default)]
    pub liveness_age_ms: Option<u64>,
    /// True when the advertised facts have aged past the liveness window,
    /// whatever the link is doing. Separate from `status` on purpose.
    #[serde(default)]
    pub telemetry_stale: bool,
    pub advertisement: ExecutorAdvertisement,
    pub admission: AdmissionHealth,
    pub queues: Vec<QueueClassHealth>,
    pub host: HostHealth,
    pub disk: DiskHealth,
    /// The placement-facing machine readings, each with its own collection time.
    #[serde(default)]
    pub machine: MachineTelemetry,
    #[serde(default)]
    pub inventory: CellInventoryHealth,
    #[serde(default)]
    pub connection_generation: u64,
    #[serde(default)]
    pub applied_policy: ExecutorRuntimePolicy,
    /// Whether this machine is refusing new admissions. True when the runner
    /// holds a drain for it — durable fleet state that outlives this connection
    /// — or when the executor itself reports one. Either way it is one fact,
    /// and the surfaces render it as one.
    #[serde(default)]
    pub drain_mode: bool,
    #[serde(default)]
    pub resident_processes: ResidentProcessHealth,
    #[serde(default)]
    pub command_processes: CommandProcessHealth,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evicted_cells: Vec<EvictedCellReport>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub build_skew: Option<BuildSkew>,
}

/// One executor as an agent inspects it: its public address, everything the
/// runner has cached about the machine, and the work resident on it.
///
/// Assembled from a single locked read of the fleet, so the link state, the
/// telemetry, and the occupancy all describe the same connection generation.
/// Combining a health report taken now with an occupancy record from a
/// reconnect ago would describe a machine that never existed.
///
/// `health` carries the executor's internal identity because it is the runner's
/// own cached record; the agent-facing rendering addresses the machine by
/// [`Self::name`] alone.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ExecutorInspection {
    /// The public address of this machine, and the only name a placement
    /// request needs.
    pub name: String,
    /// The network path the current link takes, as the runner's accepted
    /// socket saw it. `None` for a link the runner's executor listener did not
    /// accept, which is the colocated executor's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub link_path: Option<ExecutorLinkPath>,
    /// Placement decisions this machine took part in, newest first — the ones it
    /// won and the ones it was passed over for. An operator asking why a machine
    /// is idle reads the rejection reason here rather than inferring it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub recent_placements: Vec<PlacementDecision>,
    /// True for the executor the runner supervises in its own process tree.
    pub colocated: bool,
    /// The cached substrate report, link age, and advertisement.
    pub health: ExecutorHealthSnapshot,
    /// The build identifier the running executor reported, when it reported one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executor_build_id: Option<String>,
    /// The resident executor's most recent self-update outcome. The report is
    /// durable on the machine and refreshed by both Hello and Heartbeat.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_update: Option<UpdateReport>,
    /// This executor's own cells, queued work, and running work.
    pub occupancy: FleetSnapshot,
    /// The machine's own projection of its slots, which carries what the last
    /// categorized walk measured for each cell.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub slots: Vec<SlotState>,
    /// The one instant every age on this record is derived from.
    pub captured_at_unix_ms: u64,
    /// Recent remote-link transitions. Empty for the colocated executor.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub connection_timeline: Vec<RemoteConnectionTransition>,
    /// For each project the runner knows, whether placement would let this
    /// machine prepare (and build Rust for) its cells, decided by the same
    /// clauses placement applies.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub project_eligibility: Vec<ProjectEligibility>,
}

/// Whether one machine can prepare one project's cells, as placement decides.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ProjectEligibility {
    pub project_id: String,
    /// Every reason placement refuses this machine for the project's work
    /// (Rust work included); empty when the machine is eligible.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub refusals: Vec<PlacementRejectionReason>,
}

/// How far along the one-time SSH ceremony that turns a bare host into a
/// resident executor has got.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum BootstrapPhase {
    /// Asking the host what it is.
    Probing,
    /// Streaming and verifying the executor binary.
    Installing,
    /// Proving, from the machine's own vantage, which advertised endpoints
    /// answer with the pinned certificate.
    VerifyingReach,
    /// Writing the config and the enrollment credential.
    Configuring,
    /// Asking the machine to register its own service.
    Registering,
    /// Everything is installed; waiting for the machine to dial in for the
    /// first time.
    AwaitingFirstDial,
}

/// Where an enrolled machine stands with this runner.
///
/// Spoke-asserted, and the vocabulary follows from that. The runner no longer
/// reaches into a machine to make it present, so there is no such thing as an
/// attach that failed: after the one-time ceremony, presence is something the
/// machine asserts by dialling in and heartbeating, and its absence is
/// information rather than a runner-side error.
///
/// The distinctions that remain are the ones a person acts on differently.
/// [`Self::BootstrapFailed`] is a ceremony that did not complete, which only a
/// person can resolve. [`Self::BootstrapHeld`] is a ceremony that has not
/// started because something on the RUNNER is not ready yet; it is a fact about
/// the runner charged to nobody, and it clears on its own. [`Self::NeverConnected`] is a ceremony that DID complete
/// on a machine that has not dialled yet — installed, not broken, and reported
/// as "waiting for first dial" rather than as a failure. [`Self::Absent`] is an
/// enrolled machine that has connected before and is not connected now, which is
/// the ordinary condition of a laptop that left the network or a machine that is
/// powered off. Collapsing these would produce a fleet view that reads as broken
/// every time a machine sleeps, and a view nobody believes is how three machines
/// stayed gone for hours without anyone noticing (CAIRN-3356).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum RemoteLinkState {
    /// The bootstrap ceremony is running.
    Bootstrapping { phase: BootstrapPhase },
    /// The ceremony did not complete. A person has to resolve this.
    BootstrapFailed { cause: String },
    /// The ceremony is waiting on the runner, not on the machine.
    ///
    /// A fact about the runner: nothing about the machine needs fixing, no
    /// attempt has been spent on it, and the ceremony runs by itself when the
    /// precondition clears. Never connected, and never a failure.
    BootstrapHeld { precondition: RunnerPrecondition },
    /// Bootstrapped, and has not dialled in since. Not a failure.
    NeverConnected { since_bootstrap_unix_ms: u64 },
    /// The machine is here, on this generation, since this moment.
    Connected { generation: u64, since_unix_ms: u64 },
    /// Enrolled, has connected before, and is not connected now.
    Absent {
        last_beat_unix_ms: Option<u64>,
        last_reason: String,
    },
}

/// What a runner must have before it can bootstrap any machine.
///
/// Each of these is the runner's to satisfy, which is why a ceremony that meets
/// one is held rather than failed: charging a runner fact to a machine is how
/// six machines once failed in the same second for one missing setting.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    tag = "kind"
)]
pub enum RunnerPrecondition {
    /// The executor listener is not serving yet: not configured, still binding,
    /// or its certificate could not be prepared. `reason` is the runner's own
    /// account.
    ListenerNotServing { reason: String },
    /// The listener is serving but there is no endpoint worth advertising: no
    /// operator override, and discovery found no name or address a remote
    /// machine could dial.
    NoDialableEndpoint,
}

impl RunnerPrecondition {
    /// The precondition in the words a surface renders.
    pub fn describe(&self) -> String {
        match self {
            Self::ListenerNotServing { reason } if reason.is_empty() => {
                "the runner's executor listener is not serving yet".to_string()
            }
            Self::ListenerNotServing { reason } => {
                format!("the runner's executor listener is not serving yet: {reason}")
            }
            Self::NoDialableEndpoint => {
                "the runner found no endpoint a remote machine could dial; \
                 set buildSlots.executorListener.advertisedEndpoints"
                    .to_string()
            }
        }
    }
}

/// What one advertised endpoint did when a machine tried it during bootstrap.
///
/// Printed by `cairn-exec reach` as a JSON array and read by the runner, so the
/// spelling is shared here rather than agreed by convention.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ReachOutcome {
    /// A TCP connection opened and the peer completed a TLS handshake with a
    /// pinned certificate.
    Reached,
    /// No connection, or no completed handshake, within the bound.
    Unreachable,
    /// Something answered with a certificate that is not pinned. Never a route:
    /// the credential must not go there.
    PinRefused,
}

/// One endpoint's verdict from `cairn-exec reach`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ReachVerdict {
    pub endpoint: String,
    pub outcome: ReachOutcome,
    /// The error or the fingerprint presented, for a person reading why.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub detail: String,
}

impl RemoteLinkState {
    /// Whether this machine can be given work right now.
    pub fn is_connected(&self) -> bool {
        matches!(self, Self::Connected { .. })
    }
}

/// How long a bootstrap ceremony waits for the machine it just installed to dial
/// in for the first time.
///
/// Deliberately NOT [`EXECUTOR_STARTUP_READY_BUDGET_MS`]. That budget bounds a
/// successor process adopting cells from a predecessor on a machine that is
/// already warm, and it is the third link of an asserted chain in which each
/// budget clears the next. This one bounds a different job: a service
/// registration, a cold process start, a machine capacity probe, toolchain
/// detection, and a TLS handshake that may cross a relayed tailnet — the same
/// work the SSH attach path gave `LINK_READY_TIMEOUT`. Reusing the 12-second
/// handoff budget here would report machines as never-connected while they were
/// about to succeed.
///
/// Exceeding it is never a failure verdict. It is
/// [`RemoteLinkState::NeverConnected`]: installed, waiting for first dial.
pub const EXECUTOR_FIRST_DIAL_BUDGET_MS: u64 = 60_000;

/// Why an attach attempt failed at artifact distribution, when that is what
/// failed.
///
/// Carried as a fact rather than reconstructed from the message, because reading
/// meaning back out of an English sentence is what made a genuinely unpublished
/// target indistinguishable from corrupted bytes. The two call for opposite
/// things: an absent artifact is waiting for a publication and should poll
/// occasionally, while a mismatch between a manifest and its bytes is a broken
/// release that no amount of retrying repairs.
///
/// Absent on an ordinary attach failure, and absent from any peer that predates
/// this field, which reads as "the runner did not say" rather than as "nothing
/// was wrong".
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum RemoteArtifactFault {
    /// Nothing is published for this target at this protocol. Ordinary right
    /// after a protocol bump, and it clears on its own when CI publishes.
    Unpublished,
    /// The manifest names an artifact and the bytes served disagree with it.
    /// A person has to repair the release; retrying cannot.
    Integrity,
    /// The runner verified the bytes locally and the remote rejected them on
    /// arrival, which makes the transfer itself the suspect.
    RemoteIntegrity,
}

/// The longest a summary may run before it is elided.
///
/// A bound rather than a convention, because the headline is composed from
/// captured output and nothing upstream promises a captured line is short.
const ATTACH_SUMMARY_MAX_CHARS: usize = 200;

/// The runner's most recent attempt on a machine, and what stopped it.
///
/// Two fields rather than one string because the halves have different readers.
/// `summary` is the terminal cause standing alone, which is all a fleet overview
/// shows; `detail` is the runner's complete account, and for a link that died
/// mid-session that is a full bootstrap and command log.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RemoteAttachAttempt {
    pub attempted_at_unix_ms: u64,
    /// The terminal cause in one bounded line, already fit to show a person.
    pub summary: String,
    /// The runner's whole account of the attempt, present only when it said more
    /// than the summary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub held_for_ms: Option<u64>,
    /// Present only when artifact distribution is what failed. Readers switch on
    /// this rather than matching the summary's text.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_fault: Option<RemoteArtifactFault>,
}

impl RemoteAttachAttempt {
    /// An attempt that never brought a link up.
    pub fn bootstrap_failure(attempted_at_unix_ms: u64, account: impl Into<String>) -> Self {
        let (summary, detail) = split_account(account.into());
        Self {
            attempted_at_unix_ms,
            summary,
            detail,
            held_for_ms: None,
            artifact_fault: None,
        }
    }

    /// Name what went wrong in artifact distribution, when that is what did.
    pub fn with_artifact_fault(mut self, fault: Option<RemoteArtifactFault>) -> Self {
        self.artifact_fault = fault;
        self
    }

    /// A link that attached, held for `held_for_ms`, and then went away.
    pub fn link_lost(
        attempted_at_unix_ms: u64,
        account: impl Into<String>,
        held_for_ms: u64,
    ) -> Self {
        let (summary, detail) = split_account(account.into());
        Self {
            attempted_at_unix_ms,
            summary,
            detail,
            held_for_ms: Some(held_for_ms),
            artifact_fault: None,
        }
    }
}

/// Separate the line that leads from the account behind it.
fn split_account(account: String) -> (String, Option<String>) {
    let cut = [account.find("; "), account.find('\n')]
        .into_iter()
        .flatten()
        .min()
        .unwrap_or(account.len());
    let headline = account[..cut].trim_end();
    let elided = headline.chars().count() > ATTACH_SUMMARY_MAX_CHARS;
    let summary = if elided {
        let mut short: String = headline.chars().take(ATTACH_SUMMARY_MAX_CHARS).collect();
        while short.ends_with(char::is_whitespace) {
            short.pop();
        }
        short.push('…');
        short
    } else {
        headline.to_string()
    };
    let detail = (elided || cut < account.len()).then_some(account);
    (summary, detail)
}

/// A machine this runner is enrolled with that is NOT currently attached.
///
/// Enrollment and attachment are different facts, and this type exists because
/// only the second one was ever projected. A remote whose link is down held an
/// enrollment the whole time and produced no row anywhere, so the fleet read as
/// though the machine had never existed.
///
/// It rides *beside* the attached executors rather than inside them because that
/// collection is what inventory authority and placement are computed from: a
/// laptop that has simply left the LAN must not make the whole cell inventory
/// read as non-authoritative.
///
/// The facts here are the ones enrollment knows without the machine's help —
/// its name and platform — plus the runner's own account of what it last tried.
/// Nothing about load or capacity appears, because nothing measured it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct EnrolledRemote {
    /// The identity a placement pin names, which is not the display name below.
    /// A pin has to be answered about exactly one machine, and only this field
    /// can tell which. Defaulted because an older runner does not send it, and
    /// an empty identity matches no pin.
    #[serde(default)]
    pub executor_id: String,
    /// The public address of this machine, and the only name a placement
    /// request needs. It reads identically before and after the machine
    /// attaches.
    pub name: String,
    pub os: String,
    pub arch: String,
    pub link: RemoteLinkState,
    /// `None` only while [`RemoteLinkState::Pending`] holds.
    pub last_attempt: Option<RemoteAttachAttempt>,
    /// Whether the runner is holding this machine drained. Drain is runner-side
    /// state, so it is known for a machine that is not attached — and it is the
    /// state that decides what happens when this one comes back.
    #[serde(default)]
    pub drained: bool,
    /// When this machine's link was last up, or `None` if it has not attached
    /// since this runner started.
    pub last_seen_unix_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub connection_timeline: Vec<RemoteConnectionTransition>,
}

/// The complete, immutable account of one placement: what the caller stated,
/// which machines were considered, what was measured about each, and why one was
/// chosen or none could be.
///
/// One record rather than a second log. Everything an operator needs to answer
/// "why did this run there" is here, and a machine that ran work locally has to
/// say it won on measured evidence: "local fallback" is not an implicit state
/// this system is allowed to be in.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PlacementDecision {
    pub request_id: String,
    pub attempt_id: String,
    pub decided_at_unix_ms: u64,
    /// What the caller stated about whether policy may choose.
    pub mobility: PlacementMobility,
    /// What the caller asked for, when it asked for anything.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selector: Option<ExecutorSelector>,
    /// Where the work already lived, when it already lived somewhere.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pinned_executor_id: Option<String>,
    /// Where the same batch would have landed if its execution-home constraints
    /// were replaced by transferable, spill-eligible placement. This is a
    /// shadow decision only: dispatch still follows `outcome`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unpinned_winner: Option<PlacementCounterfactualWinner>,
    /// The operator policy consulted after hard placement gates and forecast
    /// comparability were established.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<PlacementPolicyEvidence>,
    pub outcome: PlacementOutcome,
    /// Every machine that was considered and passed over, with the reason.
    #[serde(default)]
    pub rejected: Vec<PlacementRejection>,
    /// How the decision actually bound: the offer, the claim that took it, and
    /// every decline that moved it on.
    ///
    /// [`Self::rejected`] says which machines placement never asked. This says
    /// what happened to the ones it did ask, which is the half that used to be
    /// unrecorded — a machine that took an order and then sat on it left no
    /// evidence but a log line about being parked. Empty on a refusal, which
    /// never reached a machine at all.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub binding: Vec<BindingEvent>,
}

/// One step in the chain from "placement chose a machine" to "a machine is
/// running it".
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(
    tag = "event",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum BindingEvent {
    Offered {
        executor_id: String,
        executor_name: String,
        generation: u64,
        at_unix_ms: u64,
    },
    Claimed {
        executor_id: String,
        generation: u64,
        at_unix_ms: u64,
    },
    Declined {
        executor_id: String,
        reason: DeclineReason,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        detail: Option<String>,
        /// The typed disk refusal, when the reason is `disk`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        disk: Option<DiskDecline>,
        at_unix_ms: u64,
    },
    /// A claim reached the runner after its entry had moved on.
    Lost {
        executor_id: String,
        at_unix_ms: u64,
    },
    /// The link that carried an offer ended before the machine answered it.
    GenerationEnded {
        executor_id: String,
        generation: u64,
        at_unix_ms: u64,
    },
}

impl BindingEvent {
    /// Which machine this step happened on.
    pub fn executor_id(&self) -> &str {
        match self {
            Self::Offered { executor_id, .. }
            | Self::Claimed { executor_id, .. }
            | Self::Declined { executor_id, .. }
            | Self::Lost { executor_id, .. }
            | Self::GenerationEnded { executor_id, .. } => executor_id,
        }
    }

    pub fn at_unix_ms(&self) -> u64 {
        match self {
            Self::Offered { at_unix_ms, .. }
            | Self::Claimed { at_unix_ms, .. }
            | Self::Declined { at_unix_ms, .. }
            | Self::Lost { at_unix_ms, .. }
            | Self::GenerationEnded { at_unix_ms, .. } => *at_unix_ms,
        }
    }
}

impl PlacementDecision {
    /// Whether this decision names the given executor at all — as the machine
    /// that won it, or as one that was passed over.
    pub fn mentions_executor(&self, executor_id: &str) -> bool {
        match &self.outcome {
            PlacementOutcome::Selected(selection) if selection.executor_id == executor_id => true,
            _ if self
                .unpinned_winner
                .as_ref()
                .is_some_and(|winner| winner.executor_id == executor_id) =>
            {
                true
            }
            _ => {
                self.rejected
                    .iter()
                    .any(|rejection| rejection.executor_id == executor_id)
                    // A machine that declined an offer must show that decline on
                    // its own page. It is not in `rejected` — placement did not
                    // pass it over, it was asked and said no — and leaving it out
                    // here would hide the most informative thing that ever
                    // happens to a machine in the placement record.
                    || self
                        .binding
                        .iter()
                        .any(|event| event.executor_id() == executor_id)
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(
    tag = "state",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum PlacementOutcome {
    Selected(Box<PlacementSelection>),
    /// No machine could take this work, and the rejections say why. A refusal is
    /// a decision with the same evidence attached as a success.
    Refused {
        diagnostic: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PlacementSelection {
    /// Measured by the runner against this placement's target, not copied from
    /// an executor's last measurement. Absent means no distance was established.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_distance: Option<crate::cache_distance::MeasuredDistance>,
    /// The public name, which is what a follow-up request would address.
    pub executor_name: String,
    pub executor_id: String,
    pub colocated: bool,
    pub reason: PlacementReason,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tie_break: Option<PlacementTieBreak>,
    /// The three placement inputs as they read for this machine, each with the
    /// instant it was measured. A gap stays a named gap here; it never becomes
    /// a zero.
    pub readings: PlacementReadings,
    /// What this work was charged on this machine, and how that number was
    /// arrived at.
    pub reservation: ResourceReservation,
    pub reservation_rationale: ReservationRationale,
    pub sync_cost: PlacementSyncCost,
    /// Bound only for remote managed-object execution, where it is the exact
    /// coordinate the objects travel under.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_transfer: Option<ObjectTransferCoordinate>,
    pub observation_reuse: ObservationReuse,
    /// What this machine was predicted to cost, and on what evidence. Absent
    /// only where nothing was ranked — a pin, or a caller that settled
    /// placement itself.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prediction: Option<PlacementPrediction>,
    /// Present when this work's measured memory demand is larger than this
    /// machine will ever reserve, and it was placed here anyway because no
    /// usable machine could hold it. The reservation above is then the clamp,
    /// not the demand, and this is what says so.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_ceiling: Option<MemoryCeiling>,
}

/// Why this machine, in words that distinguish a measured win from the absence
/// of a choice.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum PlacementReason {
    /// The work already lives here. Policy chose nothing.
    Pinned,
    /// Conservative untargeted work runs on the executor holding the runner's
    /// own checkout.
    ColocatedHome,
    /// The only machine that survived the caller's selector and the filters.
    OnlyCandidate,
    /// Predicted to reach a verdict soonest among the usable machines — queue
    /// wait plus run duration, at this machine's learned speed and warmth.
    ///
    /// This replaced a ranking on whole-percent CPU idleness, which measured how
    /// unloaded a machine was and called it fast. An idle eight-core host with
    /// an empty target directory beat a busy sixteen-core one that had just
    /// built the same tree, which is precisely backwards for a caller waiting on
    /// the answer.
    PredictedEarliestVerdict,
    /// An admission-ready executor with no work ahead had no usable duration
    /// history for this command at this warmth. Spill-eligible work was placed
    /// there to acquire the observations future earliest-verdict decisions need.
    BootstrapExploration,
    /// Nothing in the fleet had complete, fresh placement readings, so no
    /// measured comparison was possible and the work stayed on its home
    /// executor. Named explicitly because it is exactly the state that must not
    /// pass for a measured decision.
    MeasuredBlindFleet,
}

impl PlacementReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pinned => "pinned",
            Self::ColocatedHome => "colocatedHome",
            Self::OnlyCandidate => "onlyCandidate",
            Self::PredictedEarliestVerdict => "predictedEarliestVerdict",
            Self::BootstrapExploration => "bootstrapExploration",
            Self::MeasuredBlindFleet => "measuredBlindFleet",
        }
    }
}

/// The three readings placement is allowed to decide on, carried verbatim from
/// the machine's own telemetry so the record can be read without a second
/// lookup and without trusting that the machine still reads the same way.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PlacementReadings {
    pub cpu: Measurement<CpuPressure>,
    pub memory: Measurement<MachineMemory>,
    pub volume: Measurement<MachineVolume>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum PlacementSyncCost {
    /// Canonical object bytes this machine is missing for the requested commit.
    /// A placement approximation, not predicted wire bytes.
    Known {
        bytes: u64,
    },
    Unknown,
}

/// Whether the verdict this placement produces can seed a reusable baseline.
///
/// A spilled check trades baseline reusability for latency. Under CAIRN-3328's
/// rule a remote observation without a trusted full environment fingerprint is
/// diagnostic and non-reusable, so a suite that spills produces a valid gating
/// verdict for its own run and no reusable observation. That is the right trade,
/// and it is recorded rather than discovered: an operator wondering why baselines
/// miss reads the answer here.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ObservationReuse {
    /// Ran on the coordinator's own machine, in its own environment. The
    /// observation is reusable.
    Colocated,
    /// Ran on an enrolled remote whose full environment identity is not
    /// established. The verdict gates this run and seeds no baseline.
    UntrustedRemoteEnvironment,
}

impl ObservationReuse {
    pub fn is_reusable(self) -> bool {
        matches!(self, Self::Colocated)
    }

    pub fn describe(self) -> &'static str {
        match self {
            Self::Colocated => "observation reusable: colocated environment",
            Self::UntrustedRemoteEnvironment => {
                "observation non-reusable: untrusted remote environment identity"
            }
        }
    }
}

/// Observations of one command identity on one machine context below which a
/// learned memory/disk/duration estimate is not yet evidence.
///
/// One number, shared by the learner that refuses to displace its safety prior
/// and by every surface that renders an estimate, so a reading can never present
/// a single sample as a settled prediction while the scheduler is still treating
/// it as a guess.
pub const MIN_CONFIDENT_RESERVATION_SAMPLES: u64 = 5;

/// How the number this work was charged came to be that number.
///
/// A reservation without its rationale is indistinguishable from a guess. This
/// carries the profile that was consulted, the evidence it held, the prior it
/// was measured against, and — when nothing was learned — which of the several
/// different reasons for that applied.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ReservationRationale {
    /// Concurrency the CALLER stated for this work, when it stated any.
    ///
    /// Concurrency is never learned: an observation records how many cores a
    /// command used when nothing was in its way, which is not a claim about how
    /// many lanes it needs. Charging observed parallelism made every ordinary
    /// build claim the whole host (CAIRN-3345). This field keeps the declared
    /// half of a reservation legible beside the learned memory/disk half, so a
    /// whole-machine charge can never be misread as something a profile
    /// concluded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub declared_concurrency_units: Option<u32>,
    /// The profile identity consulted, absent when the work declares none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile_key: Option<String>,
    /// The executor context the profile was keyed by — class, OS, architecture,
    /// and toolchains. A profile learned on one platform never speaks for
    /// another.
    pub profile_context: String,
    pub sample_count: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upper_peak_rss_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upper_disk_growth_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upper_duration_ms: Option<u64>,
    /// The cold-start safety prior this was resolved against.
    pub prior: ResourceReservation,
    /// Percentage added above the learned high-water mark.
    pub headroom_percent: u32,
    /// Why no learned value was used, when none was.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback: Option<ReservationFallback>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ReservationFallback {
    /// The work declares no command resource identity, so nothing can be learned
    /// about it or looked up for it.
    NoCommandIdentity,
    /// This identity has never completed on a machine with this context.
    NoProfileRecorded,
    /// The profile store could not be read. Distinct from "nothing recorded":
    /// one is a cold start, the other is a fault.
    ProfileLookupFailed,
    /// Fewer samples than the confidence floor, so the prior still holds the
    /// floor and the learned value can only raise it.
    BelowConfidenceFloor,
    /// The caller stated its own demand and the resolver did not overrule it.
    CallerDeclared,
    /// The learned memory peak approached the machine's entire budget, which is
    /// evidence of host-wide pressure rather than this command's isolated demand.
    MemoryObservationQuarantined,
    /// This machine has not measured this command, but other machines have, and
    /// their observations stood in as the prior.
    ///
    /// Still a prior, never a measurement: it is labeled here, the reservation
    /// stays [`ResourceReservationSource::Unmeasured`], and nothing writes it
    /// into this machine's learned store. What it replaces is a command-class
    /// floor that could not distinguish a workspace build from a type-check,
    /// which on a large machine let a cold wave admit against essentially
    /// nothing (CAIRN-4365).
    FleetPrior,
}

impl ReservationFallback {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NoCommandIdentity => "noCommandIdentity",
            Self::NoProfileRecorded => "noProfileRecorded",
            Self::ProfileLookupFailed => "profileLookupFailed",
            Self::BelowConfidenceFloor => "belowConfidenceFloor",
            Self::CallerDeclared => "callerDeclared",
            Self::MemoryObservationQuarantined => "memoryObservationQuarantined",
            Self::FleetPrior => "fleetPrior",
        }
    }
}

/// Observations of one command identity, on one machine context, at one warmth
/// below which a learned duration is not yet a prediction.
///
/// Three, because three is the smallest window in which a median can reject a
/// single outlier — which is the whole failure this floor exists to prevent. One
/// forty-minute run on a machine that was swapping must not become that
/// machine's answer for the next hour. It is deliberately lower than
/// [`MIN_CONFIDENT_RESERVATION_SAMPLES`]: a reservation that is wrong overcommits
/// a host, while a ranking that is wrong sends work to the second-best machine,
/// and the second failure is cheap enough to learn from.
pub const MIN_CONFIDENT_DURATION_SAMPLES: u64 = 3;

/// How many recent observations a duration profile predicts from.
///
/// Bounded so that a machine which got slower — a new toolchain, a filling
/// disk, a heavier suite — is described by what it does now rather than by an
/// average over its whole history. Old samples age out by count, not by clock,
/// so a machine that is rarely used keeps the evidence it has.
pub const DURATION_SAMPLE_WINDOW: usize = 16;

/// Past this age the newest observation in a profile stops speaking for the
/// machine, and the labeled class prior takes over.
///
/// A week, because a duration profile describes a machine's current shape — its
/// toolchain, its caches, its disk — and none of those are stable across much
/// more than that. Silence is not evidence of continuity.
pub const DURATION_PROFILE_STALE_AFTER_MS: u64 = 7 * 24 * 60 * 60 * 1_000;

/// How long this work is predicted to take on one machine, and on what basis.
///
/// The prediction and its provenance travel together on purpose. A number
/// without its evidence is indistinguishable from a constant, and a fleet that
/// ranks machines on constants is the fiction this replaced.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DurationEstimate {
    /// Milliseconds of execution predicted for this command on this machine at
    /// this warmth.
    pub predicted_ms: u64,
    pub source: DurationEvidence,
    /// Observations behind the prediction. Zero whenever `source` is
    /// [`DurationEvidence::Unmeasured`].
    pub sample_count: u64,
    /// The profile identity consulted, absent when the work declares none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile_key: Option<String>,
    /// The executor context the profile was keyed by. A profile learned on one
    /// platform never speaks for another.
    pub profile_context: String,
    /// Which warmth's observations this prediction came from.
    pub warmth: ExecutionWarmth,
    /// When the newest observation behind this prediction was recorded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at_unix_ms: Option<u64>,
    /// Why no learned value was used, when none was.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback: Option<DurationFallback>,
}

impl DurationEstimate {
    pub fn is_learned(&self) -> bool {
        matches!(self.source, DurationEvidence::Learned)
    }

    /// One line an operator can read without a second lookup.
    pub fn describe(&self) -> String {
        match self.source {
            DurationEvidence::Learned => format!(
                "{}ms predicted from {} {} observation(s) of {}",
                self.predicted_ms,
                self.sample_count,
                self.warmth.as_str(),
                self.profile_key.as_deref().unwrap_or("this command")
            ),
            DurationEvidence::Unmeasured => format!(
                "{}ms from the labeled class prior ({})",
                self.predicted_ms,
                self.fallback
                    .map(DurationFallback::describe)
                    .unwrap_or("nothing learned")
            ),
        }
    }
}

/// Whether a duration prediction came from this machine's own history or from a
/// labeled prior standing in for history it does not have.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum DurationEvidence {
    Learned,
    /// A class prior. Never a claim about this machine's speed, and marked so
    /// that no surface can present it as one.
    Unmeasured,
}

/// Why a duration prediction fell back to the labeled class prior.
///
/// Deliberately its own vocabulary rather than [`ReservationFallback`]'s. A
/// reservation high-water mark does not rot with age, so it has no notion of a
/// profile that got too old to speak; a duration does, and that difference is
/// the whole reason a prediction can be stale while a reservation is not.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum DurationFallback {
    /// The work declares no command resource identity, so nothing can be
    /// learned about it or looked up for it.
    NoCommandIdentity,
    /// This identity has never completed on this machine at this warmth.
    NoProfileRecorded,
    /// The profile store could not be read. A fault, not a cold start.
    ProfileLookupFailed,
    /// Fewer observations than [`MIN_CONFIDENT_DURATION_SAMPLES`], so a median
    /// could not reject an outlier yet.
    BelowConfidenceFloor,
    /// The newest observation aged past [`DURATION_PROFILE_STALE_AFTER_MS`].
    ProfileTooOld,
    /// This placement had no profile store to consult at all. Distinct from
    /// having consulted one and found nothing: a residency placement carries no
    /// resource plan, and reporting that as "never observed" would blame the
    /// machine for a lookup that was never attempted.
    NoProfileStore,
}

impl DurationFallback {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NoCommandIdentity => "noCommandIdentity",
            Self::NoProfileRecorded => "noProfileRecorded",
            Self::ProfileLookupFailed => "profileLookupFailed",
            Self::BelowConfidenceFloor => "belowConfidenceFloor",
            Self::ProfileTooOld => "profileTooOld",
            Self::NoProfileStore => "noProfileStore",
        }
    }

    pub fn describe(self) -> &'static str {
        match self {
            Self::NoCommandIdentity => "the work declares no command identity",
            Self::NoProfileRecorded => "nothing has been observed here at this warmth",
            Self::ProfileLookupFailed => "the profile store could not be read",
            Self::BelowConfidenceFloor => "too few observations to reject an outlier",
            Self::ProfileTooOld => "every observation is older than the profile age limit",
            Self::NoProfileStore => "this placement consulted no profile store",
        }
    }
}

/// What the runner could establish about a machine's warmth for this request.
///
/// Warmth is derived from facts the executor reported about its own retained
/// cells and verified objects. When those facts are absent or aged, this says so
/// rather than reporting [`ExecutionWarmth::Cold`] — "we could not tell" and
/// "we checked and it is cold" rank differently and read differently.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(
    tag = "state",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum CacheWarmthEvidence {
    Observed {
        warmth: ExecutionWarmth,
        /// When the facts this classification was read from were collected.
        observed_at_unix_ms: u64,
    },
    Unknown {
        reason: WarmthUnknownReason,
    },
}

impl CacheWarmthEvidence {
    /// The warmth a prediction should be keyed on. An unknown warmth resolves to
    /// [`ExecutionWarmth::Cold`] for lookup — the conservative reading, which
    /// predicts the slower profile rather than the faster one — while the record
    /// keeps saying it was unknown.
    pub fn for_lookup(&self) -> ExecutionWarmth {
        match self {
            Self::Observed { warmth, .. } => *warmth,
            Self::Unknown { .. } => ExecutionWarmth::Cold,
        }
    }

    pub fn describe(&self) -> String {
        match self {
            Self::Observed { warmth, .. } => warmth.describe().to_string(),
            Self::Unknown { reason } => format!("warmth unknown: {}", reason.describe()),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum WarmthUnknownReason {
    /// The machine's advertised facts aged past the liveness bound, so what it
    /// last said about its cells is history.
    FactsStale,
    /// The executor does not claim authority over its own cell inventory, so
    /// the absence of a matching cell is not evidence that none exists.
    InventoryNotAuthoritative,
}

impl WarmthUnknownReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::FactsStale => "factsStale",
            Self::InventoryNotAuthoritative => "inventoryNotAuthoritative",
        }
    }

    pub fn describe(self) -> &'static str {
        match self {
            Self::FactsStale => "the machine's cell facts are past the liveness bound",
            Self::InventoryNotAuthoritative => "the machine does not claim inventory authority",
        }
    }
}

/// How long this request is predicted to wait before a machine starts it.
///
/// Advisory, and computed by the runner from facts the executor published. It
/// reserves nothing, enqueues nothing, and binds nobody: the executor's waiting
/// room remains the only authority over what actually runs when.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(
    tag = "state",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum QueueForecast {
    Forecast {
        predicted_ms: u64,
        /// Queued requests that would rank ahead of this one under the
        /// executor's own aging rule.
        requests_ahead: usize,
        /// Executions already running, whose remaining time this request waits
        /// on when the machine has no free lane.
        running_ahead: usize,
        /// False when any leg of the sum came from a labeled prior rather than
        /// from learned history.
        fully_measured: bool,
        observed_at_unix_ms: u64,
    },
    Unknown {
        reason: QueueUnknownReason,
    },
}

impl QueueForecast {
    /// Milliseconds this leg contributes to a total, or `None` when the leg is
    /// unknown. An unknown wait is never summed as zero.
    pub fn predicted_ms(&self) -> Option<u64> {
        match self {
            Self::Forecast { predicted_ms, .. } => Some(*predicted_ms),
            Self::Unknown { .. } => None,
        }
    }

    pub fn describe(&self) -> String {
        match self {
            Self::Forecast {
                predicted_ms,
                requests_ahead,
                running_ahead,
                fully_measured,
                ..
            } => format!(
                "{predicted_ms}ms behind {requests_ahead} queued and {running_ahead} running{}",
                if *fully_measured {
                    ""
                } else {
                    ", partly from class priors"
                }
            ),
            Self::Unknown { reason } => format!("queue wait unknown: {}", reason.describe()),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum QueueUnknownReason {
    /// Sustained measured host CPU pressure has no Cairn-owned completion event
    /// from which a relief time can be predicted.
    MeasuredCpuPressure,
    /// The machine's advertised facts aged past the liveness bound.
    FactsStale,
    /// The executor advertises no concurrency capacity, so what sits ahead of a
    /// request cannot be turned into a wait.
    NoAdmissionCapacity,
}

impl QueueUnknownReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::MeasuredCpuPressure => "measuredCpuPressure",
            Self::FactsStale => "factsStale",
            Self::NoAdmissionCapacity => "noAdmissionCapacity",
        }
    }

    pub fn describe(self) -> &'static str {
        match self {
            Self::MeasuredCpuPressure => {
                "sustained measured CPU pressure has no forecastable relief event"
            }
            Self::FactsStale => "the machine's queue facts are past the liveness bound",
            Self::NoAdmissionCapacity => "the machine advertises no concurrency capacity",
        }
    }
}

/// What this machine would have to fetch or build before the command could
/// start, and why that costs no predicted milliseconds yet.
///
/// This leg deliberately contributes nothing to a total. Turning missing object
/// bytes into milliseconds requires transfer history nothing has recorded, and
/// inventing a bytes-per-second constant would put back exactly the kind of
/// fiction predicted-verdict ranking exists to remove. The byte count rides here
/// as what it honestly is: a deterministic tiebreak between machines whose
/// predicted times are otherwise equal.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(
    tag = "state",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum PreparationForecast {
    /// This machine already holds every object the request names.
    ObjectsPresent,
    /// Objects must travel first, and no transfer history exists to price them.
    TransferPending { bytes: u64 },
    /// The runner could not estimate what this machine is missing.
    Unknown,
}

impl PreparationForecast {
    /// Bytes for the ranking tiebreak. A machine whose transfer cost is unknown
    /// sorts behind every machine whose cost is known, including large ones,
    /// because an unknown is not evidence of being small.
    pub fn tiebreak_bytes(self) -> u64 {
        match self {
            Self::ObjectsPresent => 0,
            Self::TransferPending { bytes } => bytes,
            Self::Unknown => u64::MAX,
        }
    }

    pub fn describe(self) -> String {
        match self {
            Self::ObjectsPresent => "every object is already here".into(),
            Self::TransferPending { bytes } => {
                format!("{bytes} bytes of objects to send, not yet priced in time")
            }
            Self::Unknown => "what this machine is missing could not be estimated".into(),
        }
    }
}

/// One machine's predicted time to a verdict, with every component's evidence.
///
/// This is what placement now ranks on, and it is attached to the winner and to
/// every machine that was passed over with something to compare. An operator
/// asking "why did this run there" reads the totals side by side off one record
/// rather than reconstructing them from a second log.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PlacementPrediction {
    pub executor_name: String,
    pub executor_id: String,
    /// Predicted wait before this machine would start the work.
    pub queue: QueueForecast,
    /// What this machine would have to fetch first. Carries no milliseconds by
    /// construction; see [`PreparationForecast`].
    pub preparation: PreparationForecast,
    /// Predicted execution time once started.
    pub run: DurationEstimate,
    /// Run duration before and after contention is priced.
    pub base_run_ms: u64,
    pub adjusted_run_ms: u64,
    pub contention: ContentionEstimate,
    /// Queue wait plus run duration — the two legs that carry honest time.
    pub predicted_verdict_ms: u64,
    /// What the runner established about this machine's warmth for this work.
    pub warmth: CacheWarmthEvidence,
}

impl PlacementPrediction {
    /// How much of this prediction is this machine's own history rather than a
    /// labeled prior. Lower is better evidence, and it breaks ties between
    /// machines whose predicted totals are equal — never ahead of the totals
    /// themselves, because a confidently predicted slow machine is still slow.
    pub fn evidence_rank(&self) -> u8 {
        let run = u8::from(!self.run.is_learned());
        let queue = match &self.queue {
            QueueForecast::Forecast {
                fully_measured: true,
                ..
            } => 0,
            QueueForecast::Forecast { .. } => 1,
            QueueForecast::Unknown { .. } => 2,
        };
        run * 3 + queue
    }

    /// The one line every readable surface renders for a candidate.
    pub fn describe(&self) -> String {
        format!(
            "{}: {}ms predicted ({}; {}; {}; {})",
            self.executor_name,
            self.predicted_verdict_ms,
            self.queue.describe(),
            self.run.describe(),
            self.warmth.describe(),
            self.preparation.describe()
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PlacementRejection {
    pub executor_name: String,
    pub executor_id: String,
    pub reason: PlacementRejectionReason,
    /// What this machine was predicted to cost, when it was comparable at all.
    /// A structural rejection — wrong project, closed link, pinned elsewhere —
    /// carries none, because no honest prediction exists for a machine that was
    /// never in the running.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prediction: Option<PlacementPrediction>,
}

/// A measured memory demand larger than one machine will ever reserve.
///
/// Every machine caps a reservation at a fixed share of its memory budget, so
/// that work measured on a bigger host cannot claim a small one whole and
/// starve admission there forever. The cap keeps work flowing, and it also
/// hides that it clamped: a reservation of exactly the cap reads as "fits".
/// This is the clamp stated as a fact, so placement can prefer a machine that
/// can hold the work and every surface can say when one could not.
///
/// Only a measurement sets it, never a class prior, and never a quarantined
/// peak: a peak near the whole budget measured the machine's saturation rather
/// than the command, and treating it as demand would refuse that machine the
/// work forever.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MemoryCeiling {
    /// The measured demand, before the cap.
    pub estimate_bytes: u64,
    /// The most this machine will ever reserve for one request.
    pub ceiling_bytes: u64,
}

impl MemoryCeiling {
    /// The two numbers in the units a person reads them in.
    pub fn describe(&self) -> String {
        format!(
            "its measured memory demand {} exceeds what this machine can ever hold ({})",
            describe_gib(self.estimate_bytes),
            describe_gib(self.ceiling_bytes)
        )
    }
}

/// Bytes as gibibytes with one decimal, for prose about memory demand.
pub fn describe_gib(bytes: u64) -> String {
    format!("{:.1} GiB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
}

/// Why a machine could not take this work. Every variant is actionable: it names
/// the thing that would have to change for this machine to be usable.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum PlacementRejectionReason {
    /// The link to this machine is gone.
    ConnectionClosed,
    /// The transport remains attached, but no message has crossed it within the
    /// executor progress liveness bound.
    ConnectionStale {
        silence_ms: u64,
        stale_after_ms: u64,
    },
    /// The caller asked for something this machine is not.
    SelectorMismatch { requested: String },
    /// The work already lives on another machine.
    PinMismatch { pinned_executor_id: String },
    /// This machine does not serve the requesting project.
    ProjectUnavailable { project_id: String },
    /// Conservative work stays on the machine holding the runner's checkout, and
    /// this is not that machine.
    NotColocated,
    /// Work that must be materialized from managed objects cannot go to a
    /// machine that does not already hold the checkout it names.
    RepositoryNotTransferable { locator: String },
    /// A placement input is a named gap. Placement will not ship a tree to a
    /// machine whose load it cannot see, and a gap is never read as zero load.
    TelemetryGap {
        measurement: MachineMeasurement,
        gap: MeasurementGap,
    },
    /// A placement input carries a value, but one measured too long ago to
    /// decide on.
    TelemetryStale {
        measurement: MachineMeasurement,
        age_ms: u64,
        stale_after_ms: u64,
    },
    /// The resolved demand does not fit what this machine has left.
    InsufficientMemory {
        required_bytes: u64,
        available_bytes: u64,
    },
    InsufficientVolume {
        required_bytes: u64,
        free_bytes: u64,
    },
    /// This work's measured memory demand is larger than this machine will
    /// ever reserve, however idle it is. Distinct from
    /// [`Self::InsufficientMemory`], which is about what is free now: no amount
    /// of waiting makes this machine big enough. See [`MemoryCeiling`].
    ExceedsMemoryCeiling {
        estimate_bytes: u64,
        ceiling_bytes: u64,
    },
    /// Usable, measured, and simply beaten by the machine that won.
    OutrankedBy { executor_name: String },
    /// Capable of running the work, but not a platform whose answer the
    /// requester counts as the verdict. Placement will not turn a lane red on a
    /// platform the project does not gate on.
    UntrustedVerdictPlatform { os: String, trusted: Vec<String> },
    /// An operator has drained this machine. The runner refuses to place on it
    /// here rather than relying on the executor to reject the admission,
    /// because a drain is the runner's own fact and holds whether or not the
    /// machine is in any condition to answer.
    Drained,
    /// A check this request carries has spent its retry budget on this machine's
    /// current environment. The machine may be perfectly healthy for other work;
    /// it is this check that must go elsewhere. Carries no payload because the
    /// rejection record it rides in already names the machine, which is the
    /// actionable half — the environment digest behind it is a hash.
    AvoidedInfrastructure,
    /// The machine was asked and its own admission said no, on the connection
    /// it still holds. Unlike every structural reason above, this is an answer
    /// about *now*: the machine is attached and capable, and the request waits
    /// for it to post new state rather than being refused. The reason and the
    /// detail are the machine's own words, carried verbatim.
    Declined {
        reason: DeclineReason,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        detail: Option<String>,
        declined_at_unix_ms: u64,
    },
    /// The project's setup commands, which every cell for the project runs
    /// before its first command, invoke a toolchain this machine does not
    /// advertise. A fact about the machine and the project, not the batch: a
    /// batch that selected nothing still cannot be prepared here.
    PreparationToolchainMissing {
        project_id: String,
        missing: Vec<String>,
        advertised: Vec<String>,
    },
    /// This machine already failed to prepare a cell for this project because
    /// a setup command was not found, on the environment it still advertises.
    /// Cleared when the machine re-attaches with changed evidence or an
    /// operator declares it `repaired`.
    PreparationRefused {
        project_id: String,
        command: String,
        program: String,
        recorded_at_unix_ms: u64,
    },
    /// Rust work for a project that declares the build-prerequisite script
    /// this machine's attach-time verdict came from, and that verdict reads
    /// MISSING. Only a re-attach that changes the verdict lifts it.
    BuildPrerequisitesMissing {
        project_id: String,
        script: String,
        detail: String,
    },
}

/// How long ago an instant was, coarse enough to read at a glance.
fn describe_age(at_unix_ms: u64) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or(at_unix_ms);
    let seconds = now.saturating_sub(at_unix_ms) / 1_000;
    match seconds {
        0..60 => format!("{seconds}s ago"),
        60..3_600 => format!("{}m ago", seconds / 60),
        3_600..86_400 => format!("{}h ago", seconds / 3_600),
        _ => format!("{}d ago", seconds / 86_400),
    }
}

impl PlacementRejectionReason {
    /// A single line an operator can act on, for the readable surfaces.
    pub fn describe(&self) -> String {
        match self {
            Self::ConnectionClosed => "connection closed".into(),
            Self::ConnectionStale {
                silence_ms,
                stale_after_ms,
            } => format!(
                "connection published no progress for {silence_ms}ms, past the {stale_after_ms}ms liveness bound"
            ),
            Self::SelectorMismatch { requested } => format!("does not match {requested}"),
            Self::PinMismatch { pinned_executor_id } => {
                format!("the work's tree lives on {pinned_executor_id}")
            }
            Self::ProjectUnavailable { project_id } => {
                format!("does not serve project {project_id}")
            }
            Self::NotColocated => "not the runner's colocated executor".into(),
            Self::RepositoryNotTransferable { locator } => {
                format!("{locator} cannot be materialized from managed objects")
            }
            Self::TelemetryGap { measurement, gap } => {
                format!(
                    "{} is unavailable: {}",
                    measurement.as_str(),
                    gap.describe()
                )
            }
            Self::TelemetryStale {
                measurement,
                age_ms,
                stale_after_ms,
            } => format!(
                "{} was measured {age_ms}ms ago, past the {stale_after_ms}ms bound",
                measurement.as_str()
            ),
            Self::InsufficientMemory {
                required_bytes,
                available_bytes,
            } => format!("needs {required_bytes} bytes of memory, has {available_bytes}"),
            Self::InsufficientVolume {
                required_bytes,
                free_bytes,
            } => format!("needs {required_bytes} bytes of volume, has {free_bytes}"),
            Self::ExceedsMemoryCeiling {
                estimate_bytes,
                ceiling_bytes,
            } => MemoryCeiling {
                estimate_bytes: *estimate_bytes,
                ceiling_bytes: *ceiling_bytes,
            }
            .describe(),
            Self::OutrankedBy { executor_name } => format!("outranked by {executor_name}"),
            Self::UntrustedVerdictPlatform { os, trusted } => format!(
                "runs {os}, and this verdict counts only from {}",
                trusted.join(", ")
            ),
            Self::Drained => "drained by an operator; refusing new work".into(),
            Self::AvoidedInfrastructure => {
                "a check in this request is infrastructure-suppressed on this machine's current environment"
                    .into()
            }
            Self::Declined {
                reason,
                detail,
                declined_at_unix_ms,
            } => describe_decline(*reason, detail.as_deref(), *declined_at_unix_ms),
            Self::PreparationToolchainMissing {
                project_id,
                missing,
                advertised,
            } => format!(
                "project setup for {project_id} requires {}; this machine advertises {}",
                missing.join(", "),
                if advertised.is_empty() {
                    "none".to_string()
                } else {
                    advertised.join(", ")
                }
            ),
            Self::PreparationRefused {
                project_id,
                command,
                program,
                recorded_at_unix_ms,
            } => format!(
                "could not prepare cells for {project_id}: `{command}` failed because `{program}` is not on this machine's PATH (recorded {}); clears on re-attach with changed toolchains, a change to the project's setup, or `repaired`",
                describe_age(*recorded_at_unix_ms)
            ),
            Self::BuildPrerequisitesMissing {
                project_id,
                script,
                detail,
            } => format!(
                "rust work for {project_id} needs the build prerequisites in {script}; this machine reads MISSING as of its last attach: {}",
                detail.trim()
            ),
        }
    }
}

/// One sentence for a machine's refusal of one request: its reason, its own
/// detail verbatim, and how long ago it said so. Every surface that names a
/// decline renders it through here, so the agent's refusal, the placement
/// record, and the fleet view say the same thing.
pub fn describe_decline(
    reason: DeclineReason,
    detail: Option<&str>,
    declined_at_unix_ms: u64,
) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or(declined_at_unix_ms);
    let age_s = now.saturating_sub(declined_at_unix_ms) / 1_000;
    match detail.filter(|detail| !detail.is_empty()) {
        Some(detail) => format!("declined this request ({reason:?}): {detail}, {age_s}s ago"),
        None => format!("declined this request ({reason:?}), {age_s}s ago"),
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum InventoryAuthorityState {
    Authoritative,
    #[default]
    Unknown,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CellInventoryHealth {
    pub authority: InventoryAuthorityState,
    /// Every cell in the pool, whatever its lifecycle or holder. Not a count of
    /// holds: that is `held_count`.
    #[serde(rename = "materializedCount")]
    pub checked_out_count: usize,
    /// Cells a residency or command holds right now (`!is_free()`), which no
    /// reclamation rung may touch.
    #[serde(default)]
    pub held_count: usize,
    pub idle_count: usize,
    /// Idle cells this executor will currently keep per project. Derived from
    /// the retention policy and the last free-space measurement, so it moves
    /// between the policy's floor and ceiling as the volume fills and drains.
    #[serde(default)]
    pub idle_retention_budget_per_project: usize,
    /// Hard machine-wide idle-cell budget currently enforced.
    #[serde(default)]
    pub idle_retention_budget_total: usize,
    /// True while measured free space holds retention down at its floor.
    #[serde(default)]
    pub idle_retention_pressured: bool,
    pub excess_idle_count: usize,
    pub transient_occupancy: usize,
    pub resident_occupancy: usize,
    pub active_transient_reservation: ResourceReservation,
    pub active_resident_reservation: ResourceReservation,
    pub retirement_in_progress: bool,
    pub sweep_status: StorageSweepStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "lastReclaimedSlotId")]
    pub last_reclaimed_cell_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_reclaimed_at_unix_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_reclaimed_bytes: Option<u64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CellLifecycleCensus {
    pub total: usize,
    pub provisioning: usize,
    pub idle: usize,
    pub queued: usize,
    pub running: usize,
    pub recovering: usize,
    pub retired: usize,
    pub quarantined: usize,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StoreLockHealth {
    pub store: String,
    pub waiter_count: usize,
    pub waits: BoundedDurationSummary,
    pub holds: BoundedDurationSummary,
}

/// Where the machine's supervised compile-cache daemon is in its lifecycle.
///
/// A closed vocabulary, because "unhealthy" is not one condition: a daemon
/// between bounded restart attempts, a daemon this runner can see but cannot
/// recover, and a daemon whose recovery has repeatedly failed call for
/// different reading and different action. The distinction is the whole point —
/// a cache that has stopped working was previously inferable only from builds
/// being slow (CAIRN-3332).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub enum CompileCacheState {
    /// Configured but switched off. Builds compile uncached by choice.
    Disabled,
    /// Enabled, but the cache program is not on this machine.
    NotInstalled,
    /// Live and answering its health round trip.
    Healthy,
    /// Down or wedged, with a bounded relaunch already scheduled.
    Restarting,
    /// Down or wedged and not recoverable by this runner right now — a daemon
    /// adopted from an earlier process, say, whose handle this one never held.
    Degraded,
    /// Launches have failed past the backoff ceiling. Named rather than
    /// inferred, and still retried on the supervisor's own cadence.
    RecoveryFailed,
}

/// One sample of a compile-cache daemon's own counters.
///
/// These are the daemon's lifetime totals, and they reset to zero when it
/// restarts — which is why every sample travels beside the generation it was
/// taken from. A consumer that cannot see the generation change cannot tell a
/// counter reset from a collapse in cache effectiveness.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub struct CompileCacheStats {
    pub compile_requests: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    /// Calls the cache saw but could not cache — the share of compilation it is
    /// structurally unable to help with, which is not a failure.
    pub non_cacheable: u64,
    /// Read, write, and general cache errors the installed version exposes,
    /// summed. A non-zero value is the cache failing at its job.
    pub cache_errors: u64,
    /// Compiles the daemon ran in its OWN process, because they missed the cache
    /// and were cacheable. This is the only work whose success depends on the
    /// daemon's own environment — its writable grant, its temp directory — and
    /// so the only work that can fail for reasons the requesting build did
    /// nothing to cause.
    #[serde(default)]
    pub compiles_executed: u64,
    /// Of those, the ones that SUCCEEDED and were stored.
    #[serde(default)]
    pub compilations: u64,
    /// Of those, the ones that FAILED.
    ///
    /// A genuine compiler error counts here exactly as a denied output write
    /// does — sccache records that the compile it ran returned non-zero, not
    /// why. So this number alone never accuses the daemon; read beside
    /// [`compilations`](Self::compilations) it becomes able to.
    #[serde(default)]
    pub compile_failures: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_size_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_cache_size_bytes: Option<u64>,
}

impl CompileCacheStats {
    /// Share of cacheable requests served from cache, or `None` when nothing
    /// cacheable has been asked of it yet.
    ///
    /// Deliberately `None` rather than zero on an empty denominator: a cache
    /// that has been asked nothing has not failed, and rendering that as a 0%
    /// hit rate is the exact misreport this type exists to prevent.
    pub fn hit_rate(&self) -> Option<f64> {
        let cacheable = self.cache_hits + self.cache_misses;
        (cacheable > 0).then(|| self.cache_hits as f64 / cacheable as f64)
    }
}

/// The machine's compile cache as the substrate panel reads it.
///
/// Lifecycle and statistics are separate on purpose. A daemon can be perfectly
/// healthy while its statistics are unavailable (nothing has sampled it yet) or
/// stale (the last sample predates the last restart), and a daemon that is down
/// has no statistics at all — which must render as a named gap, never as a zero
/// hit rate.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CompileCacheHealth {
    /// The configured service name, so an operator can find it in settings.
    pub service: String,
    pub state: CompileCacheState,
    /// Which incarnation of the daemon `stats` belongs to. Increments on every
    /// successful launch or adoption.
    pub generation: u64,
    /// Launches this runner has made since it started.
    pub restart_count: u64,
    /// Failed launches since the last healthy observation. Zero whenever the
    /// cache is healthy.
    pub consecutive_failures: u32,
    /// When the next bounded relaunch is due, while one is scheduled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_attempt_unix_ms: Option<u64>,
    /// When the lifecycle state last changed, so "restarting" can be shown with
    /// how long it has been restarting.
    pub state_changed_at_unix_ms: u64,
    pub stats: Measurement<CompileCacheStats>,
    /// Bounded operator text for a state that needs explaining. Never an
    /// environment value, a secret, or unbounded daemon output.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub condition: Option<String>,
}

impl CompileCacheHealth {
    /// Whether two samples differ in any way worth waking a UI for.
    ///
    /// Measurement time alone is not: the supervisor re-samples on every tick,
    /// and treating a fresh timestamp over identical counters as news would make
    /// an idle machine emit an invalidation forever.
    pub fn materially_differs(&self, other: &Self) -> bool {
        self.state != other.state
            || self.generation != other.generation
            || self.restart_count != other.restart_count
            || self.consecutive_failures != other.consecutive_failures
            || self.condition != other.condition
            || self.stats.reading != other.stats.reading
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SubstrateHealthSnapshot {
    pub schema_version: u32,
    pub captured_at_unix_ms: u64,
    pub status: SubstrateHealthStatus,
    pub reasons: Vec<SubstrateHealthReason>,
    /// The runner has not received an executor reading in this process lifetime
    /// and is still inside the bounded startup grace period. This is an absence
    /// of evidence, not a fleet failure.
    #[serde(default)]
    pub warming_up: bool,
    pub executors: Vec<ExecutorHealthSnapshot>,
    /// Machines enrolled with this runner that are not attached right now,
    /// sorted by name. Deliberately separate from `executors`: that collection
    /// is what inventory authority is computed from, and an unreachable machine
    /// must not make the fleet's inventory read as non-authoritative. Empty on
    /// a runner with no enrolled remotes, which is the common case.
    #[serde(default)]
    pub enrolled_remotes: Vec<EnrolledRemote>,
    /// This machine's supervised compile cache, absent when no build service is
    /// configured at all.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compile_cache: Option<CompileCacheHealth>,
    /// How many cells sit in each lifecycle state. This is a census of the
    /// fleet's inventory, not a statement about what is running in it.
    pub occupancy: CellLifecycleCensus,
    #[serde(default)]
    pub inventory: CellInventoryHealth,
    #[serde(rename = "buildSlots")]
    pub fleet: FleetSnapshot,
    pub store_locks: Vec<StoreLockHealth>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ResidencyAcquireRequest {
    pub holder: ResidencyHolder,
    pub repository: RepositoryLocator,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executor: Option<ExecutorSelector>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_ref: Option<CellOwnerRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selector: Option<String>,
    pub initial_base_commit: String,
    pub footprint: ResidencyFootprint,
    pub death_policy: OwnerDeathPolicy,
    pub priority: CellPriority,
    /// The instant past which nobody is waiting for this acquisition. See
    /// [`CellRequest::wait_horizon_unix_ms`]; the acquiring caller's horizon is
    /// threaded onto the queue entry this acquisition creates.
    pub wait_horizon_unix_ms: u64,
    /// When the acquiring caller began waiting. See
    /// [`CellRequest::waiting_since_unix_ms`].
    #[serde(default)]
    pub waiting_since_unix_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ResidencyFence {
    pub holder: ResidencyHolder,
    #[serde(default)]
    pub incarnation_id: String,
    pub cell_epoch: u64,
}

/// A bounded, read-only request against an already-live resident materialization.
/// This is deliberately not a lease operation: it cannot acquire, renew, refresh,
/// schedule, spawn, or otherwise extend the materialization lifetime.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MaterializationReadRequest {
    pub fence: ResidencyFence,
    pub cell_id: String,
    pub project_id: String,
    pub repository: RepositoryIdentity,
    pub base_commit: String,
    /// Exact executor preparation/materialization generation observed in the
    /// runner snapshot. The executor revalidates it immediately before reading.
    pub materialization_generation: Option<String>,
    /// Repository-relative path, or an absolute path already naming a file in
    /// this live cell's checkout or executor-owned scratch surface.
    pub path: String,
    pub deadline_unix_ms: u64,
    pub byte_cap: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "camelCase")]
pub enum MaterializationReadResult {
    Bytes {
        bytes: Vec<u8>,
    },
    Failed {
        kind: MaterializationReadFailureKind,
        diagnostic: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum MaterializationReadFailureKind {
    NoActiveMaterializationLease,
    StaleMaterialization,
    MaterializationUnavailable,
    DeadlineExceeded,
    Cancelled,
    InvalidPath,
    OutsideMaterialization,
    UnsupportedProjection,
    PathNotFound,
    ByteLimitExceeded,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "operation", rename_all = "camelCase")]
pub enum ResidencyOperation {
    Acquire {
        request: ResidencyAcquireRequest,
    },
    Reclaim {
        fence: ResidencyFence,
    },
    Renew {
        fence: ResidencyFence,
    },
    Release {
        fence: ResidencyFence,
    },
    StartProcess {
        fence: ResidencyFence,
        process_key: String,
        kind: ResidentProcessKind,
        process: ResidentProcessSpec,
        /// What this process costs while live, when the starting surface can
        /// declare it honestly. Terminals and REPLs pass none.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reservation: Option<ResourceReservation>,
    },
    StopProcess {
        fence: ResidencyFence,
        process_key: String,
    },
    WriteProcessInput {
        fence: ResidencyFence,
        process_key: String,
        process_generation: u64,
        data: Vec<u8>,
    },
    ResizePty {
        fence: ResidencyFence,
        process_key: String,
        process_generation: u64,
        size: ResidentPtySize,
    },
    #[serde(rename = "refreshMaterialization")]
    RefreshCheckout {
        fence: ResidencyFence,
        base_commit: String,
        /// Refuse before moving `HEAD` when tracked working-tree changes are
        /// present. Long-lived dev servers use this stricter contract because
        /// their checkout is a projection, not an authoring surface.
        #[serde(default)]
        require_clean: bool,
    },
    MaterializeConflict {
        fence: ResidencyFence,
        request: ConflictMaterializationRequest,
    },
    MaterializeAttachment {
        fence: ResidencyFence,
        request: AttachmentMaterializationRequest,
    },
}

/// Write one durable attachment's bytes into a held cell's executor-owned
/// scratch.
///
/// The runner supplies BYTES AND A NAME, never a location: the executor derives
/// the destination from its own scratch root, the ordinal, and the sanitized
/// name. That is what makes a hostile filename unable to place a file anywhere
/// but under the materialization root, and it is the same rule every other
/// runner-supplied payload on this wire already follows.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AttachmentMaterializationRequest {
    /// The attachment's ordinal within its scope. Part of the derived filename,
    /// so two attachments sharing a name do not collide in one cell.
    pub ordinal: i32,
    /// The record's already-sanitized file name. The executor sanitizes it
    /// again rather than trusting that it was: defense at the boundary that
    /// actually touches the filesystem.
    pub file_name: String,
    pub contents: Vec<u8>,
}

/// Project standard Git conflict markers into a held checkout without moving one
/// ref.
///
/// The store rolled the conflicting rebase back, so the branch is on its own
/// content and HEAD is `ours_commit`. This operation performs the three-way merge
/// the store refused to keep, and writes the result into the working tree only.
/// HEAD, the index, and every ref stay exactly where they were: the markers are
/// working-tree scaffolding for one resolution session, never history. The
/// literal-marker guard at both commit carriers is what makes that safe — a file
/// bearing markers cannot be committed by default.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ConflictMaterializationRequest {
    /// The commit the checkout must be sitting on. A checkout that has moved is
    /// refused rather than overwritten — whatever moved it knows something this
    /// request does not.
    pub expected_head: String,
    /// Merge base of ours and theirs. Absent when the store could not resolve it;
    /// the merge then treats every path as an add/add against an empty base.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_commit: Option<String>,
    /// The branch tip before the rolled-back rebase: the agent's own content.
    pub ours_commit: String,
    /// The advanced destination: the incoming content.
    pub theirs_commit: String,
    /// The paths jj recorded as conflicting. Only these are touched.
    pub paths: Vec<String>,
}

/// How one path ended up, once the merge actually ran. A path is only reported
/// here if its file was written.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ConflictMaterializationDisposition {
    /// Standard `<<<<<<<` / `=======` / `>>>>>>>` markers were written.
    Markers,
    /// Both sides changed the file but the three-way merge resolved without
    /// markers. The merged content was written; there is nothing to hand-resolve.
    Merged,
    /// Theirs deleted the file while ours kept it. Git writes no markers for a
    /// modify/delete; ours is left in place and the deletion is reported.
    DeletedByThem,
    /// Ours deleted the file while theirs changed it. Theirs is written back, so
    /// the incoming content is visible rather than silently absent.
    DeletedByUs,
}

impl ConflictMaterializationDisposition {
    /// The stable wire/storage name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Markers => "markers",
            Self::Merged => "merged",
            Self::DeletedByThem => "deleted_by_them",
            Self::DeletedByUs => "deleted_by_us",
        }
    }

    /// Whether a reader may tell an agent this file contains conflict markers.
    pub fn bears_markers(self) -> bool {
        matches!(self, Self::Markers)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MaterializedConflictPath {
    pub path: String,
    pub disposition: ConflictMaterializationDisposition,
}

/// What materialization actually did. This is the executor's confirmation, and
/// the only thing that entitles a wake or a resource to say markers exist.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ConflictMaterializationOutcome {
    /// The checkout HEAD, re-read after the write. Unchanged by construction;
    /// reported so a caller can verify rather than trust.
    pub head: String,
    pub paths: Vec<MaterializedConflictPath>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ResidencyFailureKind {
    InvalidDeclaration,
    ConflictingDeclaration,
    NotFound,
    /// The runner cannot currently route the operation to the executor that may
    /// still retain the lease. Unlike `NotFound`, this is not proof of lease death.
    Unavailable,
    StaleEpoch,
    InvalidState,
    Admission,
    Process,
    Cleanup,
    Persistence,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "camelCase")]
pub enum ResidencyResult {
    State {
        #[serde(rename = "slot")]
        cell: PersistentCellState,
    },
    Released {
        holder: ResidencyHolder,
        cell_epoch: u64,
    },
    /// A successful [`ResidencyOperation::MaterializeConflict`]. Distinct from
    /// `State` because the outcome is the point: a caller that cannot read which
    /// paths were written must not claim any were.
    ConflictMaterialized {
        #[serde(rename = "slot")]
        cell: PersistentCellState,
        outcome: ConflictMaterializationOutcome,
    },
    /// A successful [`ResidencyOperation::MaterializeAttachment`]. Distinct from
    /// `State` for the same reason `ConflictMaterialized` is: a caller that
    /// cannot read where the bytes landed must not claim they landed.
    AttachmentMaterialized {
        #[serde(rename = "slot")]
        cell: PersistentCellState,
        /// The absolute path in that cell, as the executor derived it.
        path: String,
    },
    Failed {
        kind: ResidencyFailureKind,
        diagnostic: String,
        #[serde(
            default,
            rename = "buildSlotOutcome",
            skip_serializing_if = "Option::is_none"
        )]
        cell_outcome: Option<Box<CellOutcome>>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ExecutorConfig {
    pub project_id: String,
    /// Human-readable project key used only for executor-owned presentation paths.
    /// Stable protocol and repository identity remains `project_id`.
    pub project_key: String,
    pub default_timeout_seconds: u64,
    #[serde(default)]
    pub setup_commands: Vec<String>,
    #[serde(default)]
    pub populate: cairn_cell::PopulateConfig,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub population_source_root: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ProcessSandboxMode {
    #[default]
    Unconfined,
    Confined,
    /// The externally owned checkout stays readable but is never writable,
    /// including after a fence grant. Temp and toolchain cache roots remain writable.
    ReadOnlyCheckout,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ProcessBatchFile {
    /// Environment variable whose value becomes the executor-local materialized path.
    pub env: String,
    pub contents: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ProcessBatch {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub files: Vec<ProcessBatchFile>,
    pub sequential: bool,
    pub stop_on_error: bool,
    pub sandbox_mode: ProcessSandboxMode,
    /// The containment evidence this batch was placed on.
    ///
    /// Carried into the claimed work rather than left behind at placement,
    /// because admission and launch are not the same instant. A profile, mount,
    /// helper, or policy can change in between, and evidence that was current
    /// when the runner chose this machine may not be current when the spawn
    /// happens. The executor re-reads its declared inputs immediately before
    /// applying the policy and refuses rather than starting the process, so the
    /// window between the two is closed by re-checking rather than by shortening
    /// it — a faster cadence reduces latency but never establishes currentness.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub containment: Option<WorkloadEnvelope>,
    /// Legacy wire field from estimate-derived check deadlines. Current runners
    /// leave it empty because duration estimates have no termination authority.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub timeout_bases: Vec<Option<DurationEstimate>>,
    pub items: Vec<ProcessBatchItem>,
    /// Runner-provided JavaScript packages this batch needs, addressed relative
    /// to the `node_modules` root they install into (e.g.
    /// `@cairn/sdk/package.json`).
    ///
    /// The executor installs these in the Cairn-owned directory ABOVE the
    /// checkout, never inside it, so a batch without `commit_msg` cannot lose
    /// them to checkout restoration and the project's own `node_modules` still
    /// wins resolution. See `cairn_common::runtime` for why placement rather
    /// than a resolver hook is the mechanism. Empty for batches that run no
    /// JavaScript, so unrelated shell, Python, and MATLAB batches carry no
    /// runtime bytes.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub runtime_packages: Vec<ResidentRuntimeAsset>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runner_context_id: Option<String>,
    /// Run this batch inside a cell an existing residency already holds, rather
    /// than a cell of its own. The residency keeps the cell for the whole batch
    /// and the batch becomes that cell's command occupancy, so a job's runs,
    /// REPLs, and terminals share one execution environment: one checkout root,
    /// one installed dependency tree, one `$TMPDIR`.
    ///
    /// A bound batch never resets scratch, never recovers the checkout to base,
    /// and never returns the cell to the pool. Its published delta is scoped to
    /// the paths it changed between bracket open and close, so work another
    /// process left in the shared environment before the batch is not published.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_residency: Option<ResidencyFence>,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ProcessBatchExecution {
    #[default]
    Direct,
    NativeShell,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ProcessBatchItem {
    pub header: String,
    pub stream_id: String,
    #[serde(default)]
    pub execution: ProcessBatchExecution,
    pub program: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub env: Vec<(String, String)>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stdin: Option<String>,
    /// Elapsed-time kill bound in milliseconds. Zero means unbounded and is used
    /// by checks, whose duration estimates inform placement but never termination.
    pub timeout_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command_resource_identity: Option<CommandResourceIdentity>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub verdict_environment_names: Vec<String>,
}

/// Typed result for one command in a build-cell process batch.
///
/// The runner-facing presentation fields remain stable while verifier callers can
/// consume command verdict and measurement fields without decoding an opaque body.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ProcessBatchItemOutcome {
    pub header: String,
    pub body: String,
    pub succeeded: bool,
    pub suspended: bool,
    #[serde(default)]
    pub images: Vec<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(default)]
    pub timed_out: bool,
    pub started_at_unix_ms: u64,
    pub finished_at_unix_ms: u64,
    pub duration_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peak_rss_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu: Option<CpuTime>,
    /// The width this item was told to use. Items running concurrently in one
    /// batch share the command's width, so an item's grant can be smaller.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub granted_units: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk_delta_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sandbox_denials: Vec<SandboxDenialEvidence>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tracked_modifications: Option<TrackedModificationEvidence>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub environment_fingerprint: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verdict_platform: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verdict_arch: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub toolchain_fingerprint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verdict_environment_hash: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "camelCase")]
pub enum McpRelayResult {
    Success { response: CallbackResponse },
    Rejected { diagnostic: String },
}

// The protocol keeps request payloads inline so serde preserves the established
// wire shape; boxing would be an in-memory optimization with a broad API cost.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ExecutorMessage {
    Hello {
        protocol_version: u32,
        advertisement: ExecutorAdvertisement,
        enrollment: ExecutorEnrollmentIdentity,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        executor_build_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        last_update: Option<UpdateReport>,
    },
    Ready {
        protocol_version: u32,
        identity: ExecutorIdentity,
        runner_device_id: String,
        generation: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        issued_credential: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        object_channel: Option<ObjectChannelCredential>,
        /// Where this runner's executor listener answers, for a resident
        /// executor that will have to dial it again. Absent when the runner is
        /// not serving one — a colocated executor never reconnects over a
        /// socket, and an orch with no remote fleet binds no listener at all.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        listener: Option<ExecutorListenerAdvertisement>,
    },
    ObjectChannelUpdated {
        credential: ObjectChannelCredential,
        executor_id: String,
        generation: u64,
    },
    EnrollmentCredentialUpdated {
        credential: String,
        expires_at_unix_ms: u64,
        runner_device_id: String,
        executor_id: String,
        generation: u64,
    },
    EnrollmentCredentialAccepted {
        credential: String,
        runner_device_id: String,
        executor_id: String,
        generation: u64,
    },
    EnrollmentRejected {
        reason: EnrollmentRejectionReason,
        diagnostic: String,
    },
    Heartbeat {
        advertisement: ExecutorAdvertisement,
        health: ExecutorSubstrateReport,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        last_update: Option<UpdateReport>,
    },
    /// Every executor-side queue entry this runner still has a live waiter for.
    ///
    /// Level-reported and complete: an id absent from the set is a statement that
    /// nobody is waiting for it, not an omission. The runner emits one on each
    /// [`ExecutorMessage::Heartbeat`] it receives and once after `Ready`, so the
    /// reap window is derivable from [`EXECUTOR_HEARTBEAT_INTERVAL_MS`] rather
    /// than from a second clock. An empty set is a legitimate report.
    WaitingRequests {
        request_ids: Vec<String>,
    },
    AdvertisementUpdated {
        advertisement: ExecutorAdvertisement,
    },
    ListenerUpdated {
        listener: ExecutorListenerAdvertisement,
    },
    ListenerAccepted {
        endpoints: Vec<String>,
        cert_sha256: Vec<String>,
    },
    ProtocolIncompatible {
        expected: u32,
        received: u32,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        manifest_url: Option<String>,
    },
    RuntimePolicyRequest {
        correlation_id: String,
        policy: ExecutorRuntimePolicy,
    },
    RuntimePolicyResponse {
        correlation_id: String,
        result: Result<ExecutorRuntimePolicy, String>,
    },
    DrainModeRequest {
        correlation_id: String,
        enabled: bool,
    },
    DrainModeResponse {
        correlation_id: String,
        result: Result<bool, String>,
    },
    /// Runner → the ONE machine placement chose. An invitation, not an order.
    ///
    /// It carries what the machine needs to answer from its own live facts and
    /// nothing more: no batch, no config, no checkout, and so no access to any
    /// of them. An offer that is never claimed costs the machine nothing and
    /// leaves nothing behind to revoke.
    Offer {
        entry_id: String,
        summary: OfferSummary,
    },
    /// Executor → runner. "I have the capacity now and I am taking it."
    ///
    /// Answered from admission, the one place with live facts, and answered
    /// immediately — a claim is never a promise to start later.
    Claim {
        entry_id: String,
    },
    /// Executor → runner. A truthful refusal, so the runner routes elsewhere
    /// now instead of parking work on a machine that cannot take it.
    Decline {
        entry_id: String,
        reason: DeclineReason,
        /// The one sentence a reader needs in order to act — which floor was
        /// crossed, by how much. Rendered by `cairn://executors?view=placement`,
        /// and never a substitute for `reason`, which is what routing reads.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        detail: Option<String>,
        /// For a `disk` refusal: the reading, the floor, and whether
        /// reclamation can still relieve it.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        disk: Option<DiskDecline>,
    },
    /// Runner → executor. The claim won; here is the work.
    ///
    /// This is the message that binds, and so the message that carries every
    /// grant: the request, the configuration, and the object-plane authorization
    /// that lets this machine fetch what the request names.
    Granted {
        entry_id: String,
        config: ExecutorConfig,
        request: CellRequest,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        batch: Option<ProcessBatch>,
    },
    /// Runner → executor. The grant for this claim is never coming: the claim
    /// arrived after the entry had moved on, or the runner abandoned the entry
    /// between the claim and its grant.
    ///
    /// Release the ticket. Nothing was started, nothing was granted, and there
    /// is nothing to cancel. `Cancel` is for work a grant reached; it does not
    /// release a ticket, because a residency grant's ticket is still standing
    /// when a Cancel for that acquisition may legitimately arrive.
    Lost {
        entry_id: String,
    },
    Result {
        request_id: String,
        attempt_id: String,
        outcome: CellOutcome,
    },
    #[serde(rename = "buildSlotOutput")]
    CellOutput {
        event: CellOutputEvent,
    },
    Cancel {
        request_id: String,
        attempt_id: String,
    },
    CancelJob {
        job_id: String,
    },
    ResidencyRequest {
        correlation_id: String,
        config: ResidencyRuntimeConfig,
        operation: ResidencyOperation,
        /// The queue entry this machine claimed and already admitted. Present
        /// only on a fresh acquisition grant; the machine consumes its ticket
        /// instead of admitting a second time. Absent on reattachment and every
        /// other operation. Absence never permits admission: an acquisition
        /// that cannot converge without capacity is refused by name.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        granted_entry_id: Option<String>,
    },
    ResidencyResponse {
        correlation_id: String,
        result: ResidencyResult,
    },
    ResidentProcessEvent {
        event: ResidentProcessEvent,
    },
    MaterializationReadRequest {
        correlation_id: String,
        request: MaterializationReadRequest,
    },
    MaterializationReadResponse {
        correlation_id: String,
        result: MaterializationReadResult,
    },
    SnapshotRequest {
        correlation_id: String,
    },
    SnapshotResponse {
        correlation_id: String,
        snapshot: FleetSnapshot,
        health: ExecutorSubstrateReport,
    },
    SnapshotUpdated {
        snapshot: FleetSnapshot,
        health: ExecutorSubstrateReport,
    },
    /// The runner is ending this link on purpose, and says why.
    ///
    /// The reason is the whole point of the frame. Without it a bounce the
    /// runner decided on reaches the executor as a bare socket reset, and both
    /// ends log the same `Connection reset without closing handshake` about
    /// each other — which is how one afternoon's link flapping was attributed
    /// three different ways by three different threads.
    ///
    /// Additive and defaulted, so it is not a wire compatibility boundary and
    /// does not bump [`EXECUTOR_PROTOCOL_VERSION`]: a peer that predates the
    /// field sends nothing and reads an empty reason, which is exactly the
    /// unexplained shutdown it already understood.
    Shutdown {
        #[serde(default, skip_serializing_if = "String::is_empty")]
        reason: String,
    },
    CallbackRequest {
        correlation_id: String,
        callback: RunnerCallback,
    },
    CallbackResponse {
        correlation_id: String,
        result: RunnerCallbackResult,
    },
    McpRelayRequest {
        correlation_id: String,
        runner_context_id: String,
        request: CallbackRequest,
    },
    McpRelayResponse {
        correlation_id: String,
        result: McpRelayResult,
    },
    InfrastructureDiagnostic {
        diagnostic: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "scope", content = "path", rename_all = "camelCase")]
pub enum SandboxDenial {
    Path(String),
    Command,
}

/// A sandbox denial observed while executing one concrete subprocess.
///
/// Pure-verdict callers keep this evidence without replacing the subprocess's
/// own exit-code verdict. Interactive callers continue to adjudicate `denial`
/// through the runner callback.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SandboxDenialEvidence {
    pub denial: SandboxDenial,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation: Option<String>,
    pub command: String,
    pub stream_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum RunnerCallback {
    SandboxDenied {
        runner_context_id: String,
        command: String,
        cwd: String,
        denial: SandboxDenial,
    },
    CacheCheckpoint {
        runner_context_id: String,
        command: String,
        cwd: String,
        exit_code: Option<i32>,
    },
    ProcessEvent {
        runner_context_id: String,
        stream_id: String,
        payload: String,
    },
    ProcessItemStarted {
        runner_context_id: String,
        stream_id: String,
    },
    ProcessItemCompleted {
        runner_context_id: String,
        stream_id: String,
        succeeded: bool,
        exit_code: Option<i32>,
        timed_out: bool,
        duration_ms: u64,
    },
}

impl RunnerCallback {
    /// A stable name for the callback's variant, for logs. Never serialized,
    /// and deliberately free of the payload: every variant carries the batch's
    /// `runner_context_id`, which is a capability and must not be logged.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::SandboxDenied { .. } => "sandbox_denied",
            Self::CacheCheckpoint { .. } => "cache_checkpoint",
            Self::ProcessEvent { .. } => "process_event",
            Self::ProcessItemStarted { .. } => "process_item_started",
            Self::ProcessItemCompleted { .. } => "process_item_completed",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "camelCase")]
pub enum RunnerCallbackResult {
    Allowed,
    Rejected { diagnostic: String },
    Suspended,
    Completed,
    Failed { diagnostic: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn five_state_lifecycle_wire_vocabulary_is_strict() {
        for (state, spelling) in [
            (PersistentCellLifecycle::Materializing, "materializing"),
            (PersistentCellLifecycle::Idle, "idle"),
            (PersistentCellLifecycle::Active, "active"),
            (PersistentCellLifecycle::Deleting, "deleting"),
            (PersistentCellLifecycle::Quarantined, "quarantined"),
        ] {
            let value = serde_json::json!(spelling);
            assert_eq!(serde_json::to_value(&state).unwrap(), value);
            assert_eq!(
                serde_json::from_value::<PersistentCellLifecycle>(value).unwrap(),
                state
            );
        }
        for legacy in [
            "provisioning",
            "queued",
            "running",
            "awaitingReclaim",
            "releasing",
            "recovering",
            "retired",
        ] {
            assert!(
                serde_json::from_value::<PersistentCellLifecycle>(serde_json::json!(legacy))
                    .is_err()
            );
        }
        assert_eq!(
            serde_json::to_value(ResidencyPhase::AwaitingReclaim).unwrap(),
            "awaitingReclaim"
        );
        assert_eq!(
            serde_json::to_value(ResidencyPhase::Releasing).unwrap(),
            "releasing"
        );
    }

    #[test]
    fn five_state_slot_distance_round_trips_with_its_target() {
        let mut slot: SlotState = serde_json::from_value(serde_json::json!({
            "slotId": "slot-1", "projectId": "project", "headCommit": "head",
            "lastUsedUnixMs": 7, "lifecycle": "idle"
        }))
        .unwrap();
        assert_eq!(slot.last_distance, None);
        slot.last_distance = Some(crate::cache_distance::MeasuredDistance {
            target_commit: "different-target".into(),
            distance: crate::cache_distance::CacheDistance {
                files: 1,
                ..Default::default()
            },
            measured_at_unix_ms: 9,
        });
        let value = serde_json::to_value(&slot).unwrap();
        assert_eq!(value["lastDistance"]["targetCommit"], "different-target");
        assert_eq!(value["lastDistance"]["distance"]["files"], 1);
        assert_eq!(value["lastDistance"]["measuredAtUnixMs"], 9);
        assert_eq!(serde_json::from_value::<SlotState>(value).unwrap(), slot);
    }

    fn sample_update_report() -> UpdateReport {
        UpdateReport {
            state: UpdateState::Succeeded,
            from_build: "old-build".into(),
            to_distribution_id: "release-v92".into(),
            to_build: "new-build".into(),
            cause: None,
            at_unix_ms: 42,
        }
    }

    fn sample_executor_artifact(bytes: &[u8]) -> ExecutorArtifact {
        ExecutorArtifact {
            target: "aarch64-apple-darwin".into(),
            url: "https://runner.invalid/api/executor/artifact/aarch64-apple-darwin".into(),
            sha256: hex::encode(Sha256::digest(bytes)),
            size: bytes.len() as u64,
            distribution_id: "release-v92".into(),
        }
    }

    #[test]
    fn executor_artifact_verification_names_declared_and_observed_size_and_digest() {
        let mut wrong_size = sample_executor_artifact(b"binary");
        wrong_size.size += 1;
        let size_error = verify_bytes(b"binary", &wrong_size).unwrap_err();
        assert!(size_error.contains("declares 7 bytes"), "{size_error}");
        assert!(size_error.contains("found 6 bytes"), "{size_error}");
        assert!(size_error.contains(&wrong_size.sha256), "{size_error}");

        let mut wrong_digest = sample_executor_artifact(b"binary");
        wrong_digest.sha256 = "0".repeat(64);
        let digest_error = verify_bytes(b"binary", &wrong_digest).unwrap_err();
        assert!(
            digest_error.contains(&wrong_digest.sha256),
            "{digest_error}"
        );
        assert!(
            digest_error.contains(&hex::encode(Sha256::digest(b"binary"))),
            "{digest_error}"
        );
    }

    #[test]
    fn update_fields_are_optional_for_pre_v92_messages() {
        let hello = ExecutorMessage::Hello {
            protocol_version: 91,
            advertisement: sample_advertisement(),
            enrollment: ExecutorEnrollmentIdentity::Colocated {
                supervision_nonce: String::new(),
            },
            executor_build_id: None,
            last_update: None,
        };
        let hello_value = serde_json::to_value(hello).unwrap();
        assert!(hello_value.get("last_update").is_none());
        let hello: ExecutorMessage = serde_json::from_value(hello_value).unwrap();
        assert!(matches!(
            hello,
            ExecutorMessage::Hello {
                last_update: None,
                ..
            }
        ));

        let incompatible: ExecutorMessage = serde_json::from_value(serde_json::json!({
            "type": "protocolIncompatible",
            "expected": 92,
            "received": 91
        }))
        .unwrap();
        assert!(matches!(
            incompatible,
            ExecutorMessage::ProtocolIncompatible {
                manifest_url: None,
                ..
            }
        ));
    }

    #[test]
    fn executor_inspection_carries_the_last_update_in_camel_case() {
        let report = sample_update_report();
        let value = serde_json::to_value(&report).unwrap();
        assert_eq!(
            value.get("toDistributionId"),
            Some(&serde_json::json!("release-v92"))
        );
        assert_eq!(value.get("toBuild"), Some(&serde_json::json!("new-build")));
        assert!(value.get("to_distribution_id").is_none());
        assert_eq!(
            serde_json::from_value::<UpdateReport>(value).unwrap(),
            report
        );
    }

    #[test]
    fn an_account_leads_with_its_headline_and_keeps_the_log_behind_it() {
        let attempt = RemoteAttachAttempt::link_lost(
            5_000,
            "the SSH session carrying the link exited after 433s (exit status: 255); bootstrap PATH\nprobe ok\nfatal: Not a valid commit name abc",
            433_000,
        );
        assert_eq!(
            attempt.summary,
            "the SSH session carrying the link exited after 433s (exit status: 255)"
        );
        let detail = attempt.detail.as_deref().expect("a log to drill into");
        assert!(
            detail.contains("fatal: Not a valid commit name abc"),
            "{detail}"
        );
        assert!(detail.starts_with("the SSH session carrying"), "{detail}");
        assert_eq!(attempt.held_for_ms, Some(433_000));
    }

    #[test]
    fn a_parenthesized_multi_line_stderr_still_yields_one_line() {
        let attempt = RemoteAttachAttempt::bootstrap_failure(
            5_000,
            "ssh authentication refused (Warning: Permanently added 'host'\nPermission denied (publickey).)",
        );
        assert!(!attempt.summary.contains('\n'), "{}", attempt.summary);
        assert!(attempt.detail.is_some());
    }

    #[test]
    fn a_single_line_account_has_no_detail() {
        let attempt =
            RemoteAttachAttempt::bootstrap_failure(5_000, "waiting for v48 artifact publish");
        assert_eq!(attempt.summary, "waiting for v48 artifact publish");
        assert_eq!(attempt.detail, None);
        assert_eq!(attempt.held_for_ms, None);
    }

    #[test]
    fn a_headline_too_long_to_lead_a_line_is_elided_and_kept_whole_behind_it() {
        let long = "x".repeat(ATTACH_SUMMARY_MAX_CHARS + 50);
        let attempt = RemoteAttachAttempt::bootstrap_failure(5_000, long.clone());
        assert_eq!(
            attempt.summary.chars().count(),
            ATTACH_SUMMARY_MAX_CHARS + 1
        );
        assert!(attempt.summary.ends_with('\u{2026}'), "{}", attempt.summary);
        assert_eq!(attempt.detail, Some(long));
    }

    /// The two silence thresholds are a sequence, not independent numbers, and the
    /// invariant belongs beside the constants rather than at any one consumer: a
    /// subscriber has to be able to observe `ConnectedStalled` well before the
    /// supervisor takes the link away. Collapsing them would turn every stall
    /// into an immediate reset with no observable intermediate state.
    #[test]
    fn link_stall_remediation_stays_above_progress_freshness() {
        const { assert!(EXECUTOR_LINK_STALL_REMEDIATION_MS > EXECUTOR_PROGRESS_FRESHNESS_MS) };
    }

    /// The runner's tolerance for a late answer and the executor's licence to
    /// pause a deadline are the same quantity seen from two sides, so they are
    /// one constant. This pins the reason: the margin has to be short next to
    /// the silence thresholds, because it is spent on an operation whose caller
    /// is still holding a response slot open, not on detecting a dead link.
    /// The executor handoff is three budgets living in two processes, and it
    /// only works read in order. Pinning the comparisons here states why each
    /// one exists rather than leaving the const assertions to speak alone.
    #[test]
    fn the_handoff_budgets_nest_from_the_exit_outward() {
        // An outgoing executor has to be gone -- and the kernel-held cell locks
        // gone with it -- before its successor stops waiting for them. Sized the
        // other way, every restart hands the successor a lock it will never see
        // released (CAIRN-3420).
        const { assert!(EXECUTOR_SHUTDOWN_BUDGET_MS < EXECUTOR_ADOPTION_HANDOFF_BUDGET_MS) };
        // And the successor has to finish that wait before the supervisor gives
        // up on it, or patience during an ordinary handoff reads as a failed
        // startup and is answered with a kill.
        const { assert!(EXECUTOR_ADOPTION_HANDOFF_BUDGET_MS < EXECUTOR_STARTUP_READY_BUDGET_MS) };
    }

    #[test]
    fn the_liveness_window_sits_between_a_beat_and_link_remediation() {
        // Long enough that one lost report cannot reap live work, short enough
        // that a link the runner has already abandoned is not still honoured.
        const { assert!(REQUESTER_LIVENESS_WINDOW_MS > EXECUTOR_HEARTBEAT_INTERVAL_MS) };
        const { assert!(REQUESTER_LIVENESS_WINDOW_MS < EXECUTOR_LINK_STALL_REMEDIATION_MS) };
    }

    #[test]
    fn executor_config_defaults_omitted_population_fields() {
        let config: ExecutorConfig = serde_json::from_value(serde_json::json!({
            "projectId": "p",
            "projectKey": "P",
            "defaultTimeoutSeconds": 30,
            "setupCommands": []
        }))
        .unwrap();
        assert!(config.populate.is_empty());
        assert!(config.population_source_root.is_none());
    }

    #[test]
    fn cpu_time_sums_across_parts_and_keeps_the_weakest_quality() {
        assert_eq!(CpuTime::total([]), None);
        let mixed = CpuTime::total([CpuTime::authoritative(400), CpuTime::sampled(600)]).unwrap();
        assert_eq!(mixed.cpu_ms(), 1_000);
        assert_eq!(mixed.quality(), MeasurementQuality::Sampled);
        let kernel = CpuTime::total([CpuTime::authoritative(1), CpuTime::authoritative(2)]);
        assert_eq!(kernel, Some(CpuTime::authoritative(3)));
        assert_eq!(
            CpuTime::sampled(3_000).delivered_units_millis(1_000),
            Some(3_000)
        );
        assert_eq!(CpuTime::sampled(3_000).delivered_units_millis(0), None);
        let unmeasured = serde_json::json!({"cpuMs": 5, "quality": "unavailable"});
        assert!(serde_json::from_value::<CpuTime>(unmeasured).is_err());
    }

    #[test]
    fn an_uncapped_label_claims_measurement_only_with_a_reading() {
        let reason = NoCapReason::PlatformHasNoCap;
        let measured =
            ExecutionAllocation::uncapped(2, reason, Some(CpuTime::sampled(5_000)), 1_000);
        assert_eq!(
            measured.enforcement,
            AllocationEnforcement::Measured { reason }
        );
        assert_eq!(measured.delivered_units_millis, Some(5_000));
        let unread = measured.reobserved(None, None, 1_000);
        assert_eq!(
            unread.enforcement,
            AllocationEnforcement::Advisory { reason }
        );
        assert_eq!(unread.granted_units, 2);
        assert_eq!(unread.delivered_units_millis, None);
    }

    #[test]
    fn batch_item_metadata_carries_the_items_own_cpu_and_width() {
        let mut command: CellExecutionMeta = serde_json::from_value(serde_json::json!({
            "slotId": "slot", "cellEpoch": 1, "startedAtUnixMs": 10,
            "finishedAtUnixMs": 5_010, "durationMs": 5_000, "peakRssBytes": 900,
        }))
        .unwrap();
        command.cpu = Some(CpuTime::sampled(9_000));
        command.allocation = Some(ExecutionAllocation::uncapped(
            2,
            NoCapReason::PlatformHasNoCap,
            command.cpu,
            5_000,
        ));
        let item: ProcessBatchItemOutcome = serde_json::from_value(serde_json::json!({
            "header": "h", "body": "", "succeeded": true, "suspended": false,
            "startedAtUnixMs": 20, "finishedAtUnixMs": 2_020, "durationMs": 2_000,
            "peakRssBytes": 300, "cpu": {"cpuMs": 5_000, "quality": "sampled"},
        }))
        .unwrap();
        let narrowed = command.for_batch_item(&item);
        assert_eq!(narrowed.duration_ms, Some(2_000));
        assert_eq!(narrowed.peak_rss_bytes, Some(300));
        assert_eq!(narrowed.cpu, Some(CpuTime::sampled(5_000)));
        let allocation = narrowed.allocation.unwrap();
        assert_eq!(allocation.granted_units, 2);
        assert_eq!(allocation.delivered_units_millis, Some(2_500));
        let mut shared = item.clone();
        shared.granted_units = Some(1);
        let shared = command.for_batch_item(&shared).allocation.unwrap();
        assert_eq!(shared.granted_units, 1);
    }

    #[test]
    fn every_message_variant_round_trips() {
        let request = sample_request();
        let outcome = sample_outcome();
        let snapshot = FleetSnapshot::default();
        let advertisement = sample_advertisement();
        let messages = vec![
            ExecutorMessage::Hello {
                protocol_version: EXECUTOR_PROTOCOL_VERSION,
                advertisement: advertisement.clone(),
                enrollment: ExecutorEnrollmentIdentity::Colocated {
                    supervision_nonce: "nonce".into(),
                },
                executor_build_id: Some("executor-build".into()),
                last_update: Some(sample_update_report()),
            },
            ExecutorMessage::Ready {
                protocol_version: EXECUTOR_PROTOCOL_VERSION,
                identity: advertisement.identity.clone(),
                runner_device_id: "runner".into(),
                generation: 2,
                issued_credential: None,
                object_channel: None,
                listener: Some(ExecutorListenerAdvertisement {
                    endpoints: vec!["wss://runner.example:3850/api/executor".into()],
                    cert_sha256: vec!["a".repeat(64)],
                }),
            },
            ExecutorMessage::ObjectChannelUpdated {
                credential: ObjectChannelCredential {
                    base_url: "https://runner.example/api/executor/objects".into(),
                    bearer_token: "rotated".into(),
                    expires_at_unix_ms: 10,
                },
                executor_id: "e".into(),
                generation: 2,
            },
            ExecutorMessage::EnrollmentCredentialUpdated {
                credential: "replacement".into(),
                expires_at_unix_ms: 10,
                runner_device_id: "runner".into(),
                executor_id: "e".into(),
                generation: 2,
            },
            ExecutorMessage::EnrollmentCredentialAccepted {
                credential: "replacement".into(),
                runner_device_id: "runner".into(),
                executor_id: "e".into(),
                generation: 2,
            },
            ExecutorMessage::EnrollmentRejected {
                reason: EnrollmentRejectionReason::Revoked,
                diagnostic: "revoked".into(),
            },
            ExecutorMessage::Heartbeat {
                advertisement: advertisement.clone(),
                health: ExecutorSubstrateReport::default(),
                last_update: Some(sample_update_report()),
            },
            ExecutorMessage::AdvertisementUpdated { advertisement },
            ExecutorMessage::ListenerUpdated {
                listener: ExecutorListenerAdvertisement {
                    endpoints: vec!["wss://runner.example:3850/api/executor".into()],
                    cert_sha256: vec!["b".repeat(64)],
                },
            },
            ExecutorMessage::ListenerAccepted {
                endpoints: vec!["wss://runner.example:3850/api/executor".into()],
                cert_sha256: vec!["b".repeat(64)],
            },
            ExecutorMessage::ProtocolIncompatible {
                expected: 1,
                received: 2,
                manifest_url: Some("/api/executor/artifact/manifest".into()),
            },
            ExecutorMessage::ResidentProcessEvent {
                event: ResidentProcessEvent {
                    holder: sample_holder(),
                    incarnation_id: "incarnation".into(),
                    cell_epoch: 3,
                    process_key: "main".into(),
                    process_generation: 4,
                    event: ResidentProcessEventKind::Output {
                        sequence: 5,
                        stream: ResidentProcessStream::Stdout,
                        data: b"hello".to_vec(),
                    },
                },
            },
            ExecutorMessage::RuntimePolicyRequest {
                correlation_id: "policy".into(),
                policy: ExecutorRuntimePolicy::default(),
            },
            ExecutorMessage::RuntimePolicyResponse {
                correlation_id: "policy".into(),
                result: Ok(ExecutorRuntimePolicy::default()),
            },
            ExecutorMessage::DrainModeRequest {
                correlation_id: "drain".into(),
                enabled: true,
            },
            ExecutorMessage::DrainModeResponse {
                correlation_id: "drain".into(),
                result: Ok(true),
            },
            ExecutorMessage::CellOutput {
                event: CellOutputEvent {
                    executor_id: "e".into(),
                    cell_id: "slot".into(),
                    request_id: "r".into(),
                    attempt_id: "a".into(),
                    stream_id: "stdout".into(),
                    chunk: "hello".into(),
                    emitted_at_unix_ms: 1,
                },
            },
            ExecutorMessage::Offer {
                entry_id: "entry".into(),
                summary: OfferSummary {
                    request_id: request.request_id.clone(),
                    attempt_id: request.attempt_id.clone(),
                    project_id: request.project_id.clone(),
                    repository: request.repository.clone(),
                    base_commit: request.base_commit.clone(),
                    reservation: request.resource_reservation.clone(),
                    admission_kind: CellAdmissionKind::Command,
                    interactive: false,
                    priority: request.priority,
                    waiting_since_unix_ms: request.waiting_since_unix_ms,
                    wait_horizon_unix_ms: request.wait_horizon_unix_ms,
                    predicted_cost_ms: Some(4_000),
                },
            },
            ExecutorMessage::Claim {
                entry_id: "entry".into(),
            },
            ExecutorMessage::Decline {
                entry_id: "entry".into(),
                reason: DeclineReason::Pressure,
                detail: Some("needs 2 GiB of memory, has 900 MiB".into()),
                disk: None,
            },
            ExecutorMessage::Decline {
                entry_id: "entry".into(),
                reason: DeclineReason::Disk,
                detail: Some("disk free 1 bytes (floor 2 bytes)".into()),
                disk: Some(DiskDecline {
                    free_bytes: 1,
                    floor_bytes: 2,
                    outlook: DiskReliefOutlook::Unrelieved { held_bytes: 3 },
                }),
            },
            ExecutorMessage::Granted {
                entry_id: "entry".into(),
                config: ExecutorConfig {
                    project_id: "p".into(),
                    project_key: "p".into(),
                    default_timeout_seconds: 30,
                    setup_commands: vec![],
                    populate: Default::default(),
                    population_source_root: None,
                },
                request: request.clone(),
                batch: None,
            },
            ExecutorMessage::Lost {
                entry_id: "entry".into(),
            },
            ExecutorMessage::Result {
                request_id: "r".into(),
                attempt_id: "a".into(),
                outcome,
            },
            ExecutorMessage::Cancel {
                request_id: "r".into(),
                attempt_id: "a".into(),
            },
            ExecutorMessage::CancelJob { job_id: "j".into() },
            ExecutorMessage::ResidencyRequest {
                granted_entry_id: None,
                correlation_id: "lease-request".into(),
                config: ResidencyRuntimeConfig::Install(ExecutorConfig {
                    project_id: "p".into(),
                    project_key: "p".into(),
                    default_timeout_seconds: 30,
                    setup_commands: vec![],
                    populate: Default::default(),
                    population_source_root: None,
                }),
                operation: ResidencyOperation::Acquire {
                    request: sample_acquire_request(),
                },
            },
            ExecutorMessage::ResidencyResponse {
                correlation_id: "lease-response".into(),
                result: ResidencyResult::Released {
                    holder: sample_holder(),
                    cell_epoch: 3,
                },
            },
            ExecutorMessage::MaterializationReadRequest {
                correlation_id: "read-request".into(),
                request: MaterializationReadRequest {
                    fence: ResidencyFence {
                        holder: sample_holder(),
                        incarnation_id: "incarnation".into(),
                        cell_epoch: 3,
                    },
                    cell_id: "cell".into(),
                    project_id: "p".into(),
                    repository: sample_request().repository.identity(),
                    base_commit: "base".into(),
                    materialization_generation: Some("generation".into()),
                    path: "ignored/output.txt".into(),
                    deadline_unix_ms: 10,
                    byte_cap: 1024,
                },
            },
            ExecutorMessage::MaterializationReadResponse {
                correlation_id: "read-response".into(),
                result: MaterializationReadResult::Bytes {
                    bytes: b"ok".to_vec(),
                },
            },
            ExecutorMessage::SnapshotRequest {
                correlation_id: "c".into(),
            },
            ExecutorMessage::SnapshotResponse {
                correlation_id: "c".into(),
                snapshot: snapshot.clone(),
                health: ExecutorSubstrateReport::default(),
            },
            ExecutorMessage::SnapshotUpdated {
                snapshot,
                health: ExecutorSubstrateReport::default(),
            },
            ExecutorMessage::Shutdown {
                reason: "the runner is recycling this link".into(),
            },
            ExecutorMessage::CallbackRequest {
                correlation_id: "denial".into(),
                callback: RunnerCallback::SandboxDenied {
                    runner_context_id: "ctx".into(),
                    command: "touch /outside".into(),
                    cwd: "/tmp/worktree".into(),
                    denial: SandboxDenial::Path("/outside".into()),
                },
            },
            ExecutorMessage::CallbackRequest {
                correlation_id: "c".into(),
                callback: RunnerCallback::CacheCheckpoint {
                    runner_context_id: "ctx".into(),
                    command: "echo ok".into(),
                    cwd: "/tmp/worktree".into(),
                    exit_code: Some(0),
                },
            },
            ExecutorMessage::CallbackResponse {
                correlation_id: "c".into(),
                result: RunnerCallbackResult::Completed,
            },
            ExecutorMessage::McpRelayRequest {
                correlation_id: "mcp-request".into(),
                runner_context_id: "ctx".into(),
                request: CallbackRequest {
                    cwd: "/tmp/worktree".into(),
                    run_id: Some("run".into()),
                    tool: "read".into(),
                    payload: serde_json::json!({"paths": ["cairn:~/todos"]}),
                    ..CallbackRequest::default()
                },
            },
            ExecutorMessage::McpRelayResponse {
                correlation_id: "mcp-success".into(),
                result: McpRelayResult::Success {
                    response: CallbackResponse {
                        result: "ok".into(),
                        ..CallbackResponse::default()
                    },
                },
            },
            ExecutorMessage::McpRelayResponse {
                correlation_id: "mcp-rejected".into(),
                result: McpRelayResult::Rejected {
                    diagnostic: "context expired".into(),
                },
            },
            ExecutorMessage::InfrastructureDiagnostic {
                diagnostic: "lost".into(),
            },
        ];
        for message in messages {
            let json = serde_json::to_string(&message).unwrap();
            assert_eq!(
                serde_json::from_str::<ExecutorMessage>(&json).unwrap(),
                message
            );
        }
    }

    #[test]
    fn tagged_occupants_round_trip_and_legacy_active_request_migrates_to_command() {
        let command = ActiveCellRequest {
            executor_id: "executor".into(),
            request_id: "request".into(),
            attempt_id: "attempt".into(),
            command: "true".into(),
            command_class: CellCommandClass::Other,
            owner: None,
            priority: CellPriority::ReviewCheck,
            requesting_job_id: None,
            affinity_key: None,
            queued_at_unix_ms: 1,
            started_at_unix_ms: Some(2),
            stage: Some(CellExecutionStage::Running),
            resource_reservation: ResourceReservation::default(),
            learned_estimate: None,
            subscriber_count: 1,
        };
        let occupancy = CellOccupancy {
            command: Some(command),
            command_ownership: None,
            draining: None,
            processes: std::collections::BTreeMap::from([(
                "shell".to_string(),
                ResidentProcess {
                    generation: 2,
                    kind: ResidentProcessKind::Terminal {
                        slug: "tests".into(),
                    },
                    spec: None,
                    status: ResidentProcessStatus::Running {
                        started_at_unix_ms: 7,
                        process_group_id: Some(41),
                        ownership: None,
                    },
                    reservation: None,
                },
            )]),
        };
        let value = serde_json::to_value(&occupancy).unwrap();
        assert!(
            value.get("command").is_some() && value.get("processes").is_some(),
            "a cell says what batch is running and which processes are resident, separately"
        );
        assert_eq!(
            serde_json::from_value::<CellOccupancy>(value).unwrap(),
            occupancy
        );

        let residency = CellResidency {
            holder: sample_holder(),
            repository: sample_request().repository,
            owner_ref: None,
            selector: Some("feature/branch".into()),
            incarnation_id: "incarnation".into(),
            current_base_commit: "b".into(),
            phase: ResidencyPhase::Active,
            last_heartbeat_unix_ms: 1,
            reclaim_deadline_unix_ms: 41_000,
            death_policy: OwnerDeathPolicy {
                heartbeat_timeout_ms: 30_000,
                reclaim_grace_ms: 10_000,
            },
            footprint: ResidencyFootprint {
                memory_bytes: 10,
                disk_growth_bytes: 20,
            },
            state_revision: 1,
            events: vec![ResidencyEvent {
                revision: 1,
                occurred_at_unix_ms: 1,
                event: ResidencyEventKind::Acquired,
            }],
        };
        let value = serde_json::to_value(&residency).unwrap();
        assert_eq!(
            serde_json::from_value::<CellResidency>(value).unwrap(),
            residency
        );
    }

    #[test]
    fn a_residency_is_its_holder_and_its_repository_and_nothing_else() {
        let residency = CellResidency {
            holder: ResidencyHolder::Job {
                job_id: "job".into(),
            },
            repository: sample_request().repository,
            owner_ref: None,
            selector: None,
            incarnation_id: "incarnation".into(),
            current_base_commit: "first".into(),
            phase: ResidencyPhase::Active,
            last_heartbeat_unix_ms: 1,
            reclaim_deadline_unix_ms: 2,
            death_policy: OwnerDeathPolicy {
                heartbeat_timeout_ms: 30_000,
                reclaim_grace_ms: 10_000,
            },
            footprint: ResidencyFootprint {
                memory_bytes: 10,
                disk_growth_bytes: 20,
            },
            state_revision: 0,
            events: Vec::new(),
        };
        let repository = sample_request().repository.identity();
        assert!(residency.identity_matches(
            &ResidencyHolder::Job {
                job_id: "job".into()
            },
            &repository
        ));
        assert!(!residency.identity_matches(
            &ResidencyHolder::Job {
                job_id: "other".into()
            },
            &repository
        ));
        assert!(
            !residency.identity_matches(
                &ResidencyHolder::DevInstance {
                    instance_id: "job".into()
                },
                &repository
            ),
            "a holder class is part of who holds the cell, not decoration on an id"
        );
    }

    #[test]
    fn scratch_only_repository_uses_its_owner_as_its_stable_identity() {
        let repository = RepositoryLocator::ScratchOnly {
            owner_id: "channel-imessage".into(),
        };
        assert_eq!(repository.project_id(), "channel-imessage");
        assert_eq!(repository.repository_id(), "channel-imessage");
        assert_eq!(repository.identity().project_id, "channel-imessage");
        assert_eq!(
            serde_json::to_value(repository).unwrap(),
            serde_json::json!({"kind": "scratchOnly", "ownerId": "channel-imessage"})
        );
    }

    #[test]
    fn holder_storage_keys_round_trip_for_every_class() {
        for holder in [
            ResidencyHolder::Service {
                service_id: "channel-imessage".into(),
            },
            ResidencyHolder::Job {
                job_id: "job".into(),
            },
            ResidencyHolder::DevInstance {
                instance_id: "instance".into(),
            },
            ResidencyHolder::ProjectTerminals {
                project_id: "project".into(),
            },
            ResidencyHolder::Workflow {
                run_id: "run".into(),
            },
        ] {
            assert_eq!(
                ResidencyHolder::parse_storage_key(&holder.storage_key()),
                Some(holder.clone()),
                "the storage key is how a holder survives a database column"
            );
        }
        assert_eq!(ResidencyHolder::parse_storage_key("job:"), None);
        assert_eq!(ResidencyHolder::parse_storage_key("nonsense"), None);
    }

    #[test]
    fn only_live_resident_processes_are_charged() {
        let charged = ResourceReservation {
            memory_bytes: 2_048,
            disk_growth_bytes: 4_096,
            concurrency_units: 1,
            source: ResourceReservationSource::Declared,
        };
        let process = |status: ResidentProcessStatus| ResidentProcess {
            generation: 1,
            kind: ResidentProcessKind::DevInstance {
                source_terminal_session_id: None,
            },
            spec: None,
            status,
            reservation: Some(charged.clone()),
        };
        let mut occupancy = CellOccupancy {
            command: None,
            command_ownership: None,
            draining: None,
            processes: std::collections::BTreeMap::from([(
                "server".to_string(),
                process(ResidentProcessStatus::Running {
                    started_at_unix_ms: 1,
                    process_group_id: None,
                    ownership: None,
                }),
            )]),
        };
        assert_eq!(occupancy.resident_reservation().concurrency_units, 1);

        // Recording the exit is the whole release. No separate call gives the
        // unit back, so no missed call can strand it.
        occupancy.processes.insert(
            "server".into(),
            process(ResidentProcessStatus::Exited {
                finished_at_unix_ms: 2,
                exit_code: Some(0),
                restartable: true,
                executor_lost: false,
            }),
        );
        assert_eq!(occupancy.resident_reservation().concurrency_units, 0);
        assert_eq!(occupancy.resident_reservation().memory_bytes, 0);
    }

    /// The requester's wait horizon and the instant its wait began are what a
    /// queued entry is held and ranked by, so both have to survive the wire under
    /// the names this protocol version pins. Spelling either the old way would
    /// decode as absent, read as zero, and evict every queued request on arrival
    /// — which is why the version constant moved with them.
    #[test]
    fn a_requests_wait_horizon_and_seniority_survive_the_wire() {
        // These names arrived at protocol 24 and have not changed since. The
        // constant has moved on for later contract changes, so this pins the
        // floor they were introduced at rather than an exact number that would
        // need editing on every unrelated bump.
        const { assert!(EXECUTOR_PROTOCOL_VERSION >= 24) };
        let request = sample_request();
        let json = serde_json::to_value(&request).unwrap();
        assert_eq!(json.get("waitHorizonUnixMs"), Some(&serde_json::json!(1)));
        assert_eq!(json.get("waitingSinceUnixMs"), Some(&serde_json::json!(7)));
        assert!(json.get("deadlineUnixMs").is_none());
        assert_eq!(
            serde_json::from_value::<CellRequest>(json).unwrap(),
            request
        );

        let acquire = sample_acquire_request();
        let json = serde_json::to_value(&acquire).unwrap();
        assert_eq!(json.get("waitHorizonUnixMs"), Some(&serde_json::json!(10)));
        assert_eq!(json.get("waitingSinceUnixMs"), Some(&serde_json::json!(4)));
        assert!(json.get("deadlineUnixMs").is_none());
        assert_eq!(
            serde_json::from_value::<ResidencyAcquireRequest>(json).unwrap(),
            acquire
        );

        // A requester that states no seniority is read as "now" by the executor,
        // never as the epoch — which would rank it senior to everything.
        let mut json = serde_json::to_value(sample_request()).unwrap();
        json.as_object_mut().unwrap().remove("waitingSinceUnixMs");
        assert_eq!(
            serde_json::from_value::<CellRequest>(json)
                .unwrap()
                .waiting_since_unix_ms,
            0
        );
    }

    /// The liveness report is a complete set, so an empty one has to round-trip
    /// as an empty set rather than as a missing field: "nobody is waiting for
    /// anything here" is the report that frees every queue slot.
    #[test]
    fn the_waiting_request_report_round_trips_including_the_empty_set() {
        for request_ids in [Vec::new(), vec!["r-1".to_string(), "r-2".to_string()]] {
            let frame = ExecutorMessage::WaitingRequests {
                request_ids: request_ids.clone(),
            };
            let json = serde_json::to_value(&frame).unwrap();
            assert_eq!(
                serde_json::from_value::<ExecutorMessage>(json).unwrap(),
                frame
            );
        }
    }

    #[test]
    fn request_and_delta_round_trip_and_cancellation_is_separate() {
        let request = sample_request();
        let json = serde_json::to_value(&request).unwrap();
        assert!(json.get("cancelled").is_none());
        assert!(json.get("cancellation").is_none());
        assert_eq!(
            serde_json::from_value::<CellRequest>(json).unwrap(),
            request
        );
        let outcome = sample_outcome();
        let json = serde_json::to_value(&outcome).unwrap();
        assert_eq!(
            json.get("mutation_delta"),
            Some(&serde_json::json!({
                "baseCommit": "b",
                "deltaCommit": "d"
            }))
        );
        assert_eq!(
            serde_json::from_value::<CellOutcome>(json).unwrap(),
            outcome
        );
    }

    /// Absence of a selector is not permission to move, and a peer that never
    /// heard of mobility must decode as the conservative thing rather than as
    /// the permissive one.
    #[test]
    fn a_request_without_a_stated_mobility_decodes_as_immobile() {
        const { assert!(EXECUTOR_PROTOCOL_VERSION >= 28) };
        let mut value = serde_json::to_value(sample_request()).unwrap();
        assert_eq!(
            value.get("placementMobility"),
            Some(&serde_json::json!("spillEligible")),
            "the fact travels on the wire under its own key"
        );
        value
            .as_object_mut()
            .unwrap()
            .remove("placementMobility")
            .unwrap();
        let decoded: CellRequest = serde_json::from_value(value).unwrap();
        assert_eq!(
            decoded.placement_mobility,
            PlacementMobility::PinnedOrColocated
        );
        assert!(!decoded.placement_mobility.may_spill());
    }

    #[test]
    fn placement_work_class_round_trips_and_defaults_conservatively() {
        let mut request = sample_request();
        request.placement_work_class = PlacementWorkClass::ReviewChecks;
        let mut value = serde_json::to_value(request).unwrap();
        assert_eq!(value["placementWorkClass"], "reviewChecks");
        assert_eq!(
            serde_json::from_value::<CellRequest>(value.clone())
                .unwrap()
                .placement_work_class,
            PlacementWorkClass::ReviewChecks
        );

        value.as_object_mut().unwrap().remove("placementWorkClass");
        assert_eq!(
            serde_json::from_value::<CellRequest>(value)
                .unwrap()
                .placement_work_class,
            PlacementWorkClass::AgentSessions
        );
    }

    /// The record is what an operator reads instead of guessing, so every part
    /// of it has to survive the wire: the readings with their own timestamps,
    /// the rationale behind the number the work was charged, and the typed
    /// reason each machine was passed over.
    #[test]
    fn a_placement_decision_round_trips_with_its_evidence() {
        let decision = PlacementDecision {
            request_id: "r".into(),
            attempt_id: "a".into(),
            decided_at_unix_ms: 1_700_000_000_000,
            mobility: PlacementMobility::SpillEligible,
            selector: None,
            pinned_executor_id: None,
            unpinned_winner: None,
            binding: Vec::new(),
            policy: None,
            outcome: PlacementOutcome::Selected(Box::new(PlacementSelection {
                cache_distance: None,
                prediction: None,
                memory_ceiling: None,
                executor_name: "bglab-ub".into(),
                executor_id: "executor-7b21ce".into(),
                colocated: false,
                reason: PlacementReason::PredictedEarliestVerdict,
                tie_break: Some(PlacementTieBreak {
                    candidates: vec!["bglab-ub".into(), "bbs".into()],
                    deciding_reason:
                        "least recently selected among candidates equal on every earlier placement key"
                            .into(),
                }),
                readings: PlacementReadings {
                    cpu: Measurement::measured(
                        10,
                        CpuPressure {
                            utilization: 0.03,
                            user: 0.02,
                            system: 0.01,
                            logical_cores: 16,
                        },
                    ),
                    memory: Measurement::measured(
                        11,
                        MachineMemory {
                            total_bytes: 64,
                            available_bytes: 48,
                        },
                    ),
                    volume: Measurement::unavailable(12, MeasurementGap::SamplingFailed),
                },
                reservation: ResourceReservation {
                    memory_bytes: 2_048,
                    disk_growth_bytes: 4_096,
                    concurrency_units: 1,
                    source: ResourceReservationSource::Learned,
                },
                reservation_rationale: ReservationRationale {
                    declared_concurrency_units: Some(1),
                    profile_key: Some("check:rust".into()),
                    profile_context: "device:executor on linux/x86_64".into(),
                    sample_count: 7,
                    upper_peak_rss_bytes: Some(1_600),
                    upper_disk_growth_bytes: Some(3_200),
                    upper_duration_ms: Some(90_000),
                    prior: ResourceReservation::default(),
                    headroom_percent: 25,
                    fallback: None,
                },
                sync_cost: PlacementSyncCost::Known { bytes: 12_345 },
                object_transfer: None,
                observation_reuse: ObservationReuse::UntrustedRemoteEnvironment,
            })),
            rejected: vec![PlacementRejection {
                prediction: None,
                executor_name: "local".into(),
                executor_id: "colocated".into(),
                reason: PlacementRejectionReason::TelemetryStale {
                    measurement: MachineMeasurement::Cpu,
                    age_ms: 120_000,
                    stale_after_ms: 90_000,
                },
            }],
        };
        let encoded = serde_json::to_string(&decision).unwrap();
        let wire: serde_json::Value = serde_json::from_str(&encoded).unwrap();
        assert_eq!(
            wire.pointer("/outcome/tieBreak"),
            Some(&serde_json::json!({
                "candidates": ["bglab-ub", "bbs"],
                "decidingReason":
                    "least recently selected among candidates equal on every earlier placement key"
            }))
        );
        assert_eq!(
            wire.pointer("/unpinnedWinner"),
            None,
            "older runner records omit additive shadow evidence"
        );
        let decoded: PlacementDecision = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, decision);
        assert!(decoded.mentions_executor("executor-7b21ce"));
        assert!(
            decoded.mentions_executor("colocated"),
            "a machine that was passed over still reads its own reason off this record"
        );
        assert!(!decoded.mentions_executor("bglab-win"));
        assert!(!ObservationReuse::UntrustedRemoteEnvironment.is_reusable());
    }

    fn sample_request() -> CellRequest {
        CellRequest {
            request_id: "r".into(),
            attempt_id: "a".into(),
            project_id: "p".into(),
            repository: RepositoryLocator::ColocatedPath {
                project_id: "p".into(),
                repository_id: "repo".into(),
                absolute_path: "/repo".into(),
            },
            base_commit: "b".into(),
            command: "true".into(),
            command_class: CellCommandClass::Other,
            placement_work_class: PlacementWorkClass::ReviewChecks,
            owner: None,
            cwd: String::new(),
            env: Vec::new(),
            priority: CellPriority::ReviewCheck,
            wait_horizon_unix_ms: 1,
            waiting_since_unix_ms: 7,
            timeout_ms: 2,
            mutation_policy: MutationPolicy::AllowDelta,
            requesting_job_id: Some("j".into()),
            affinity_key: Some("affinity".into()),
            executor: Some(ExecutorSelector {
                os: Some("linux".into()),
                required_toolchains: vec!["rust".into()],
                ..ExecutorSelector::default()
            }),
            placement_mobility: PlacementMobility::SpillEligible,
            verdict_platforms: Vec::new(),
            avoided_executor_ids: Vec::new(),
            project_requirements: Default::default(),
            pinned_executor_id: None,
            command_resource_identity: Some(CommandResourceIdentity {
                version: COMMAND_RESOURCE_IDENTITY_VERSION,
                key: "check-result-key".into(),
            }),
            resource_reservation: ResourceReservation {
                memory_bytes: 10,
                disk_growth_bytes: 20,
                concurrency_units: 1,
                source: ResourceReservationSource::Unmeasured,
            },
            learned_estimate: None,
        }
    }

    fn sample_holder() -> ResidencyHolder {
        ResidencyHolder::DevInstance {
            instance_id: "launcher".into(),
        }
    }

    fn sample_acquire_request() -> ResidencyAcquireRequest {
        ResidencyAcquireRequest {
            holder: sample_holder(),
            repository: sample_request().repository,
            executor: Some(ExecutorSelector {
                name: Some("executor-a".into()),
                ..ExecutorSelector::default()
            }),
            owner_ref: Some(CellOwnerRef {
                project_id: "project".into(),
                project_key: Some("CAIRN".into()),
                issue_number: Some(2873),
                job_id: Some("job".into()),
                execution_seq: Some(1),
                node_kind: Some("builder".into()),
            }),
            selector: Some("feature/branch".into()),
            initial_base_commit: "b".into(),
            footprint: ResidencyFootprint {
                memory_bytes: 10,
                disk_growth_bytes: 20,
            },
            death_policy: OwnerDeathPolicy {
                heartbeat_timeout_ms: 30_000,
                reclaim_grace_ms: 10_000,
            },
            priority: CellPriority::AgentInteractive,
            wait_horizon_unix_ms: 10,
            waiting_since_unix_ms: 4,
        }
    }

    fn sample_advertisement() -> ExecutorAdvertisement {
        ExecutorAdvertisement {
            identity: ExecutorIdentity {
                device_id: "d".into(),
                executor_id: "e".into(),
                display_name: "Executor".into(),
            },
            capabilities: ExecutorCapabilities {
                os: "linux".into(),
                arch: "x86_64".into(),
                logical_cores: 8,
                concurrency_capacity: None,
                toolchains: vec!["rust".into()],
                projects_served: vec!["p".into()],
                disk_budget_bytes: Some(10),
                memory_budget_bytes: None,
                sandbox: None,
                containment: None,
                toolchain_detection: None,
                build_prerequisites: None,
            },
            current_load: 1,
            warm_roots: vec![VerifiedWarmRoot {
                repository: RepositoryIdentity {
                    project_id: "p".into(),
                    repository_id: "repo".into(),
                    object_format: GitObjectFormat::Sha1,
                },
                commit: "b".into(),
            }],
            observed_at_unix_ms: 4,
            liveness_observed_at_unix_ms: None,
            facts_stale_refresh_blocked: None,
            last_refresh: None,
        }
    }

    #[test]
    fn transfer_coordinate_is_bound_to_the_exact_execution_and_generation() {
        let request = sample_request();
        let coordinate = ObjectTransferCoordinate {
            repository: request.repository.identity(),
            request_id: request.request_id.clone(),
            attempt_id: request.attempt_id.clone(),
            executor_id: "executor".into(),
            connection_generation: 7,
        };
        assert!(coordinate.matches_execution(&request, "executor", 7));

        let mut another_attempt = request.clone();
        another_attempt.attempt_id = "another-attempt".into();
        assert!(!coordinate.matches_execution(&another_attempt, "executor", 7));
        assert!(!coordinate.matches_execution(&request, "another-executor", 7));
        assert!(!coordinate.matches_execution(&request, "executor", 8));

        let mut another_repository = request;
        another_repository.repository = RepositoryLocator::ManagedObjects {
            project_id: "p".into(),
            repository_id: "another-repository".into(),
            object_format: GitObjectFormat::Sha1,
        };
        assert!(!coordinate.matches_execution(&another_repository, "executor", 7));
    }

    #[test]
    fn omitted_subscriber_counts_default_to_one() {
        let active: ActiveCellRequest = serde_json::from_value(serde_json::json!({
            "requestId": "r",
            "attemptId": "a",
            "command": "true",
            "priority": "reviewCheck",
            "requestingJobId": null,
            "queuedAtUnixMs": 1,
            "startedAtUnixMs": null
        }))
        .unwrap();
        assert_eq!(active.subscriber_count, 1);

        let queued: QueuedCellRequest = serde_json::from_value(serde_json::json!({
            "requestId": "r",
            "attemptId": "a",
            "projectId": "p",
            "command": "true",
            "priority": "reviewCheck",
            "requestingJobId": null,
            "queuedAtUnixMs": 1
        }))
        .unwrap();
        assert_eq!(queued.subscriber_count, 1);
    }

    /// A name is an address agents type, so normalization is forgiving on input
    /// and canonical on output. The cases here are the ones an operator label
    /// actually produces.
    #[test]
    fn a_label_normalizes_to_one_canonical_public_name() {
        for (label, expected) in [
            ("bglab-ub", Some("bglab-ub")),
            ("BGLab UB", Some("bglab-ub")),
            ("  Dell & Workstation  ", Some("dell-workstation")),
            ("Linux 'builder'", Some("linux-builder")),
            ("192.168.1.18", Some("192-168-1-18")),
            ("---", None),
            ("", None),
            ("\u{fffd}", None),
        ] {
            assert_eq!(
                normalize_executor_name(label).as_deref(),
                expected,
                "label {label:?}"
            );
        }
        // Normalization is idempotent: a name read out of the resource and fed
        // back in as a selector must address the same machine.
        for label in ["BGLab UB", "192.168.1.18"] {
            let once = normalize_executor_name(label).unwrap();
            assert_eq!(normalize_executor_name(&once), Some(once.clone()));
        }
        assert!(executor_names_match("BGLab UB", "bglab-ub"));
        assert!(!executor_names_match("bglab-ub", "bglab-mac"));
        // Two labels that normalize to nothing are not "equal by both being
        // empty": neither addresses anything.
        assert!(!executor_names_match("---", "***"));
    }

    /// A selector that asks for nothing, or for two contradictory things, is a
    /// caller error rather than a request to interpret.
    #[test]
    fn a_selector_states_one_machine_or_one_platform_and_never_nothing() {
        assert!(ExecutorSelector::default().validate().is_err());
        assert!(ExecutorSelector {
            name: Some("bglab-ub".into()),
            os: Some("linux".into()),
            ..ExecutorSelector::default()
        }
        .validate()
        .unwrap_err()
        .contains("never both"));
        assert!(ExecutorSelector {
            name: Some("---".into()),
            ..ExecutorSelector::default()
        }
        .validate()
        .is_err());
        for valid in [
            ExecutorSelector {
                name: Some("bglab-ub".into()),
                ..ExecutorSelector::default()
            },
            ExecutorSelector {
                os: Some("linux".into()),
                ..ExecutorSelector::default()
            },
            ExecutorSelector {
                required_toolchains: vec!["rust".into()],
                ..ExecutorSelector::default()
            },
        ] {
            assert!(valid.validate().is_ok(), "{valid:?}");
        }
    }

    /// Refusals are built from this, so it has to name what was asked for in
    /// words the caller wrote.
    #[test]
    fn a_selector_describes_itself_in_the_words_a_refusal_needs() {
        assert_eq!(
            ExecutorSelector {
                name: Some("bglab-ub".into()),
                required_toolchains: vec!["rust".into()],
                ..ExecutorSelector::default()
            }
            .describe(),
            "executor bglab-ub with toolchains rust"
        );
        assert_eq!(
            ExecutorSelector {
                os: Some("linux".into()),
                ..ExecutorSelector::default()
            }
            .describe(),
            "os linux"
        );
    }

    /// Probe evidence is additive, not a wire boundary. An executor built before
    /// it existed omits the key, and that has to decode as "this peer cannot
    /// explain itself" rather than failing the whole advertisement — the entire
    /// reason this field does not bump [`EXECUTOR_PROTOCOL_VERSION`].
    #[test]
    fn capabilities_without_probe_evidence_still_decode() {
        let older = serde_json::json!({
            "os": "windows",
            "arch": "x86_64",
            "logicalCores": 4,
            "toolchains": [],
            "projectsServed": [],
        });
        let decoded: ExecutorCapabilities = serde_json::from_value(older).unwrap();
        assert_eq!(decoded.toolchain_detection, None);
        assert_eq!(decoded.sandbox, None);

        // And an executor that probed and found nothing is a different fact,
        // which survives the same round trip.
        let probed = ExecutorCapabilities {
            sandbox: None,
            toolchain_detection: Some(ToolchainDetection {
                account: "mitch".into(),
                home: "C:\\Users\\mitch".into(),
                probes: Vec::new(),
            }),
            ..decoded
        };
        let round_tripped: ExecutorCapabilities =
            serde_json::from_str(&serde_json::to_string(&probed).unwrap()).unwrap();
        assert_eq!(round_tripped, probed);
        assert!(round_tripped.toolchain_detection.is_some());
    }

    /// The prerequisite verdict is additive on the same terms, and this is what
    /// makes leaving [`EXECUTOR_PROTOCOL_VERSION`] where it stands safe rather
    /// than merely convenient: an older executor's advertisement still decodes,
    /// and the field it omits reads as "this peer was never asked" — which a
    /// runner is then free to render honestly.
    #[test]
    fn capabilities_without_a_prerequisite_verdict_still_decode() {
        let older = serde_json::json!({
            "os": "linux",
            "arch": "aarch64",
            "logicalCores": 20,
            "toolchains": ["rust", "bun"],
            "projectsServed": [],
        });
        let decoded: ExecutorCapabilities = serde_json::from_value(older).unwrap();
        assert_eq!(decoded.build_prerequisites, None);
        // The machine this stages advertises `rust` and cannot build: an absent
        // verdict must never be mistaken for a satisfied one.
        assert_eq!(decoded.toolchains, vec!["rust", "bun"]);

        let refused = ExecutorCapabilities {
            build_prerequisites: Some(BuildPrerequisiteCheck {
                script: "scripts/setup-self-hosted-runner-linux.sh --check".into(),
                status: BuildPrerequisiteStatus::Missing,
                detail: "Missing Linux self-hosted runner prerequisites on bglab-sp1: libwebkit2gtk-4.1-dev".into(),
            }),
            ..decoded
        };
        let encoded = serde_json::to_string(&refused).unwrap();
        // Spelled the way every other key on this struct is spelled, because a
        // camelCase slip here decodes as the absent field and is invisible.
        assert!(encoded.contains("\"buildPrerequisites\""), "{encoded}");
        assert!(encoded.contains("\"missing\""), "{encoded}");
        let round_tripped: ExecutorCapabilities = serde_json::from_str(&encoded).unwrap();
        assert_eq!(round_tripped, refused);
    }

    /// The public selector carries no opaque identity, and a caller reaching for
    /// the retired keys is refused rather than silently placed anywhere.
    #[test]
    fn the_selector_wire_shape_admits_only_the_public_vocabulary() {
        let selector = ExecutorSelector {
            name: Some("bglab-ub".into()),
            required_toolchains: vec!["rust".into()],
            requires_macos_nested_sandbox_write: true,
            ..ExecutorSelector::default()
        };
        assert_eq!(
            serde_json::to_value(&selector).unwrap(),
            serde_json::json!({
                "name": "bglab-ub",
                "requiredToolchains": ["rust"],
                "requiresMacosNestedSandboxWrite": true
            })
        );
        for retired in ["executorId", "deviceId", "arch"] {
            let raw = serde_json::json!({ retired: "anything" });
            assert!(
                serde_json::from_value::<ExecutorSelector>(raw).is_err(),
                "{retired} must be refused, not ignored"
            );
        }
    }

    #[test]
    fn an_omitted_executor_selector_places_a_request_untargeted() {
        let mut value = serde_json::to_value(sample_request()).unwrap();
        value.as_object_mut().unwrap().remove("executor");
        assert_eq!(
            serde_json::from_value::<CellRequest>(value)
                .unwrap()
                .executor,
            None
        );
    }

    /// The retired key is not an alias. A peer still sending `constraints`
    /// would have its placement silently dropped and its work run wherever the
    /// fleet felt like, so the version number moved with the field name.
    #[test]
    fn the_retired_placement_key_is_not_decoded_as_a_selector() {
        const { assert!(EXECUTOR_PROTOCOL_VERSION >= 26) };
        let mut value = serde_json::to_value(sample_request()).unwrap();
        let object = value.as_object_mut().unwrap();
        let selector = object.remove("executor").unwrap();
        object.insert("constraints".into(), selector);
        assert_eq!(
            serde_json::from_value::<CellRequest>(value)
                .unwrap()
                .executor,
            None
        );
    }

    /// A resident process a running list would show, plus the spec and
    /// reservation that hang off it.
    fn sample_resident_process(kind: ResidentProcessKind) -> ResidentProcess {
        ResidentProcess {
            generation: 4,
            kind,
            spec: Some(ResidentProcessSpec {
                runtime_packages: Vec::new(),
                program: "bun".into(),
                args: vec!["run".into(), "dev".into()],
                launch_command: None,
                cwd: "/cell".into(),
                cwd_root: ResidentProcessCwdRoot::ResidencyScratch,
                env: vec![("CAIRN_HOME".into(), "/home".into())],
                sandbox_mode: ProcessSandboxMode::ReadOnlyCheckout,
                sandbox_policy: Some(ResidentSandboxPolicy {
                    worktree: "/cell".into(),
                    writable_extra: vec!["/tmp".into()],
                    deny_read: vec!["/secrets".into()],
                    writable_regex: vec!["^/cell/target".into()],
                    worktree_writable: true,
                }),
                runtime_assets: vec![ResidentRuntimeAsset {
                    path: "asset".into(),
                    data: vec![1],
                }],
                io: ResidentProcessIoMode::Pty {
                    size: ResidentPtySize {
                        rows: 40,
                        cols: 120,
                        pixel_width: 0,
                        pixel_height: 0,
                    },
                },
            }),
            status: ResidentProcessStatus::Running {
                started_at_unix_ms: 1_785_185_750_222,
                process_group_id: Some(62_961),
                ownership: None,
            },
            reservation: Some(ResourceReservation {
                memory_bytes: 1024,
                disk_growth_bytes: 2048,
                concurrency_units: 0,
                source: ResourceReservationSource::Declared,
            }),
        }
    }

    /// A snapshot that reaches every optional branch of the tree the desktop UI
    /// reads, because a key that is never serialized is a key no wire-shape
    /// assertion can see. The previous version of this fixture passed
    /// `FleetSnapshot::default()`, which is why a snake_case resident-process
    /// status shipped for a whole release without a Rust test noticing.
    /// Real object ids, not placeholder words. The desktop fixture is generated
    /// from this snapshot, and a Running-panel row rendering a commit as a
    /// person's identity is only provable against a snapshot that carries the
    /// commits a real one does.
    const BASE_COMMIT: &str = "70a193f86e1ae4fdf1cdaa21fbd99767765bb01e";
    const SEALED_COMMIT: &str = "66f3d9e875a1c4f0a1b2c3d4e5f60718293a4b5c";
    const DEV_INSTANCE_COMMIT: &str = "2255c68e6b01afcca30d6cd21c646fba4d9ee0ef";

    /// A cell whose residency is a dev instance, with the instance's process
    /// running inside it. `selector` is the instance's identity: the branch an
    /// implicit launch resolved, or — when the launch was given a commit — the
    /// commit itself, which names nothing a person can read.
    fn dev_instance_cell(
        cell_id: &str,
        selector: &str,
        owner_ref: Option<CellOwnerRef>,
        template: &PersistentCellState,
    ) -> PersistentCellState {
        let mut processes = std::collections::BTreeMap::new();
        processes.insert(
            "dev-instance".to_string(),
            sample_resident_process(ResidentProcessKind::DevInstance {
                source_terminal_session_id: None,
            }),
        );
        PersistentCellState {
            cell_id: cell_id.into(),
            residency: Some(CellResidency {
                holder: ResidencyHolder::DevInstance {
                    instance_id: format!("project:{selector}"),
                },
                owner_ref,
                selector: Some(selector.into()),
                events: Vec::new(),
                ..template
                    .residency
                    .clone()
                    .expect("template holds a residency")
            }),
            occupancy: CellOccupancy {
                command: None,
                command_ownership: None,
                draining: None,
                processes,
            },
            ..template.clone()
        }
    }

    /// A cell held by a placed service, with the service's watch running in it.
    ///
    /// This is the CAIRN-3435 specimen: a service residency carries no owner
    /// ref, because a channel watch belongs to no issue, so the panel has only
    /// what the process itself declares to attribute it by. The desktop fixture
    /// needs it to prove Cairn-placed work never renders as anonymous.
    fn service_cell(cell_id: &str, template: &PersistentCellState) -> PersistentCellState {
        let mut processes = std::collections::BTreeMap::new();
        processes.insert(
            "imsg-watch".to_string(),
            sample_resident_process(ResidentProcessKind::Service {
                name: "iMessage channel".into(),
                role: "watch".into(),
            }),
        );
        PersistentCellState {
            cell_id: cell_id.into(),
            residency: Some(CellResidency {
                holder: ResidencyHolder::Service {
                    service_id: "channel-imessage".into(),
                },
                repository: RepositoryLocator::ScratchOnly {
                    owner_id: "channel-imessage".into(),
                },
                owner_ref: None,
                selector: None,
                events: Vec::new(),
                ..template
                    .residency
                    .clone()
                    .expect("template holds a residency")
            }),
            occupancy: CellOccupancy {
                command: None,
                command_ownership: None,
                draining: None,
                processes,
            },
            ..template.clone()
        }
    }

    /// Every owner survives the wire with its live reading and its peak intact.
    ///
    /// The peaks are the half most easily lost — they are the only fields that
    /// still say something once an incident has settled — so a round trip that
    /// checked cardinality alone would pass while dropping the evidence.
    #[test]
    fn retained_state_round_trips_every_owner_with_its_peaks() {
        let mut retained = ExecutorRetainedState {
            measured_at_unix_ms: 1_700_000_000_000,
            ..ExecutorRetainedState::default()
        };
        // Distinct values per owner, so a serializer that crossed two fields
        // fails instead of silently agreeing with itself.
        for (index, (kind, _)) in retained.owners().into_iter().enumerate() {
            let base = (index as u64 + 1) * 10;
            let owner = RetainedOwner {
                entries: base,
                estimated_bytes: base * 1_000,
                peak_entries: base * 2,
                peak_estimated_bytes: base * 5_000,
            };
            match kind {
                RetainedOwnerKind::QueuedRequests => retained.queued_requests = owner,
                RetainedOwnerKind::ExecutingRequests => retained.executing_requests = owner,
                RetainedOwnerKind::CompletedRequests => retained.completed_requests = owner,
                RetainedOwnerKind::Cancellations => retained.cancellations = owner,
                RetainedOwnerKind::CommandProcesses => retained.command_processes = owner,
                RetainedOwnerKind::ResidentProcesses => retained.resident_processes = owner,
                RetainedOwnerKind::CommandOwnership => retained.command_ownership = owner,
                RetainedOwnerKind::Projects => retained.projects = owner,
                RetainedOwnerKind::Cells => retained.cells = owner,
                RetainedOwnerKind::WaitSamples => retained.wait_samples = owner,
                RetainedOwnerKind::ResidencyOperationLocks => {
                    retained.residency_operation_locks = owner
                }
                RetainedOwnerKind::ResidencyAcquisitionLocks => {
                    retained.residency_acquisition_locks = owner
                }
                RetainedOwnerKind::ResidencyReclaimContention => {
                    retained.residency_reclaim_contention = owner
                }
                RetainedOwnerKind::CallbackWaiters => retained.callback_waiters = owner,
                RetainedOwnerKind::OutgoingCallbackQueue => {
                    retained.outgoing_callback_queue = owner
                }
                RetainedOwnerKind::ActiveProcessAttempts => {
                    retained.active_process_attempts = owner
                }
                RetainedOwnerKind::CapturedProcessOutput => {
                    retained.captured_process_output = owner
                }
                RetainedOwnerKind::QueuedOutputChunks => retained.queued_output_chunks = owner,
                RetainedOwnerKind::ManagedManifests => retained.managed_manifests = owner,
                RetainedOwnerKind::ProcessCensus => retained.process_census = owner,
                RetainedOwnerKind::PendingSnapshot => retained.pending_snapshot = owner,
                RetainedOwnerKind::OutgoingControlQueue => retained.outgoing_control_queue = owner,
            }
        }

        let json = serde_json::to_string(&retained).unwrap();
        let decoded: ExecutorRetainedState = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, retained);
        for ((kind, before), (_, after)) in retained.owners().into_iter().zip(decoded.owners()) {
            assert_eq!(before, after, "{} did not survive the wire", kind.as_str());
            assert!(
                after.peak_entries >= after.entries,
                "{}'s peak cannot be below its live reading",
                kind.as_str()
            );
        }
        assert!(!decoded.is_unreported());
    }

    /// An executor too old to publish retained state decodes as "not reported",
    /// which is a different fact from "reported, and holding nothing". Rendering
    /// the first as the second would tell an operator a leaking daemon was
    /// clean.
    #[test]
    fn a_peer_that_omits_retained_state_decodes_as_unreported() {
        // Built by deleting the new keys from this serializer's own output
        // rather than by hand, so the fixture cannot drift away from the
        // encoding it is meant to be an older version of.
        let mut older_peer = serde_json::to_value(ProcessTelemetry {
            resident_bytes: Measurement::measured(7, 9),
            physical_footprint_bytes: Measurement::measured(7, 9),
            ..ProcessTelemetry::default()
        })
        .unwrap();
        let fields = older_peer.as_object_mut().unwrap();
        assert!(fields.remove("virtualBytes").is_some());
        assert!(fields.remove("allocator").is_some());
        assert!(fields.remove("retained").is_some());

        let telemetry: ProcessTelemetry = serde_json::from_value(older_peer)
            .expect("an older peer's process telemetry still decodes");
        assert!(telemetry.retained.is_unreported());
        assert_eq!(telemetry.retained.total_estimated_bytes(), 0);
        // And the readings it never took are named gaps, not zeros. An
        // allocator split of all zeros would read as a process holding nothing
        // and retaining nothing — the healthiest possible machine — which is
        // the opposite of "this peer is too old to have looked".
        assert_eq!(telemetry.virtual_bytes.value(), None);
        assert_eq!(telemetry.allocator.value(), None);
    }

    fn fully_populated_health_snapshot() -> SubstrateHealthSnapshot {
        let reservation = ResourceReservation {
            memory_bytes: 32,
            disk_growth_bytes: 64,
            concurrency_units: 1,
            source: ResourceReservationSource::Learned,
        };
        let learned_estimate = LearnedResourceEstimate {
            sample_count: 5,
            upper_duration_ms: Some(1),
            upper_peak_rss_bytes: Some(2),
            upper_disk_growth_bytes: Some(3),
        };
        let owner = CellOwnerRef {
            project_id: "project".into(),
            project_key: Some("CAIRN".into()),
            issue_number: Some(3195),
            job_id: Some("job".into()),
            execution_seq: Some(1),
            node_kind: Some("builder".into()),
        };
        let substrate_state = ExecutorSubstrateEvidence {
            state: ExecutorSubstrateState::ExecutionRunning,
            since_unix_ms: 1,
            last_progress_unix_ms: 2,
            diagnostic: Some("running".into()),
            queue_depth: Some(1),
            queue_position: Some(0),
            active_cell_count: Some(1),
            oldest_running_started_at_unix_ms: Some(3),
        };
        let bounded = BoundedDurationSummary {
            sample_count: 1,
            p50_ms: Some(0),
            p95_ms: Some(1),
            max_ms: Some(2),
        };
        let inventory = CellInventoryHealth {
            authority: InventoryAuthorityState::Authoritative,
            checked_out_count: 2,
            held_count: 1,
            idle_count: 1,
            idle_retention_budget_per_project: 16,
            idle_retention_budget_total: 16,
            idle_retention_pressured: false,
            excess_idle_count: 0,
            transient_occupancy: 1,
            resident_occupancy: 2,
            active_transient_reservation: reservation.clone(),
            active_resident_reservation: reservation.clone(),
            retirement_in_progress: false,
            sweep_status: StorageSweepStatus::Completed,
            last_reclaimed_cell_id: Some("slot-1".into()),
            last_reclaimed_at_unix_ms: Some(4),
            last_reclaimed_bytes: Some(5),
        };
        let host = HostHealth {
            pressure: Some(HostPressureEvidence {
                conditions: vec![
                    HostPressureCondition::MemoryAvailable {
                        available_bytes: 1,
                        floor_bytes: 2,
                        unmeasured_hold_bytes: 0,
                    },
                    HostPressureCondition::DiskFree {
                        free_bytes: 3,
                        floor_bytes: 4,
                    },
                    HostPressureCondition::ResidentOccupancy {
                        process_count: 2,
                        reservation: reservation.clone(),
                    },
                ],
            }),
            cpu_admission: CpuAdmissionHealth {
                state: CpuAdmissionState::Pressured,
                utilization: Some(0.94),
                state_since_unix_ms: Some(3),
                measured_at_unix_ms: Some(4),
            },
            logical_cores: Some(10),
            tokio_worker_count: Some(4),
            tokio_alive_tasks: Some(12),
            tokio_global_queue_depth: Some(0),
            io_pressure: Measurement::measured(
                5,
                IoPressure {
                    full_avg10_millis: 235,
                    full_avg60_millis: 190,
                    slowest_device: Some(DeviceWriteLatency {
                        device: "nvme1n1".into(),
                        mean_write_ms: Some(4_200),
                        writes: 10,
                        in_flight: 4,
                        window_ms: 15_000,
                    }),
                },
            ),
        };
        let machine = MachineTelemetry {
            cpu: Measurement::measured(
                40,
                CpuPressure {
                    utilization: 1.5,
                    user: 1.125,
                    system: 0.375,
                    logical_cores: 10,
                },
            ),
            memory: Measurement::measured(
                41,
                MachineMemory {
                    total_bytes: 64,
                    available_bytes: 6,
                },
            ),
            volume: Measurement::measured(
                42,
                MachineVolume {
                    total_bytes: 100,
                    free_bytes: 50,
                },
            ),
            disk_accounting: Measurement::measured(
                9,
                DiskAccounting {
                    used_bytes: 30,
                    categories: DiskCategoryAccounting {
                        managed_objects_bytes: 1,
                        live_cells_bytes: 2,
                        warm_caches_bytes: 3,
                        external_managed_caches_bytes: 7,
                        quarantines_bytes: 4,
                        temporary_other_bytes: 5,
                    },
                    skipped: vec![SkippedEntry {
                        path: "build-slots/slot-3".into(),
                        operation: SkippedEntryOperation::ReadMetadata,
                        reason: SkippedEntryReason::PermissionDenied,
                    }],
                    skipped_truncated: 0,
                    vanished_entries: 6,
                    other_top: vec![DiskPathUsage {
                        path: "scratch".into(),
                        bytes: 5,
                    }],
                },
            ),
            process: ProcessTelemetry {
                resident_bytes: Measurement::measured(43, 7),
                physical_footprint_bytes: Measurement::unavailable(
                    43,
                    MeasurementGap::UnsupportedPlatform,
                ),
                virtual_bytes: Measurement::measured(43, 4_096),
                allocator: Measurement::measured(
                    43,
                    AllocatorState {
                        in_use_bytes: 1_024,
                        free_retained_bytes: 2_048,
                        arena_bytes: 3_072,
                        mmapped_bytes: 512,
                        mmap_chunk_count: 1,
                    },
                ),
                retained: ExecutorRetainedState {
                    measured_at_unix_ms: 43,
                    cells: RetainedOwner {
                        entries: 2,
                        estimated_bytes: 512,
                        peak_entries: 9,
                        peak_estimated_bytes: 4_096,
                    },
                    ..ExecutorRetainedState::default()
                },
            },
        };
        let disk = DiskHealth {
            budget_bytes: Some(80),
            status: DiskHealthStatus::Pressured,
            sweep_status: StorageSweepStatus::InFlight,
            sweep_generation: 2,
            cleanup_blocked: true,
            cleanup_last_error: Some("blocked".into()),
            cleanup_failing_path: Some("/path".into()),
            cleanup_skipped_entries: Some(2),
            cleanup_skipped: vec![SkippedEntry {
                path: "build-slots/p/slot-3.quarantine-1".into(),
                operation: SkippedEntryOperation::Remove,
                reason: SkippedEntryReason::PermissionDenied,
            }],
            cleanup_skipped_truncated: 1,
            reclamation: None,
            accounting_cadence: None,
            watermark_bytes: 2,
            critical_free_bytes: 16,
            pressure_free_bytes: 64,
            pressure: Some(StoragePressureReport {
                at_unix_ms: 9,
                band: StoragePressureBand::Critical,
                target_bytes: 100,
                free_bytes_before: 1,
                free_bytes_after: 5,
                reclaimed_by_rung: vec![StorageRungReclaim {
                    rung: StorageReclaimRung::QuarantineTargets,
                    bytes: 4,
                    count: 1,
                }],
                outcome: StoragePressureOutcome::Unrelieved,
                rearm_below_free_bytes: Some(1),
                held_bytes: 3,
                withheld: vec![StorageWithheld {
                    reason: StorageWithheldReason::FencedCells,
                    bytes: 3,
                    count: 1,
                    unmeasured: false,
                }],
                occupied_skips: vec![OccupiedTreeSkip {
                    path: "build-slots/p/slot-4.quarantine-2".into(),
                    pid: Some(1653),
                    unknown: true,
                    rule: Some(OccupancyRule::WithheldInLineage),
                    process: Some("sudo".into()),
                    executable: Some("/usr/bin/sudo".into()),
                    held_path: Some("target/a".into()),
                    container: Some("coalition 19563".into()),
                }],
            }),
            quarantine_targets_stripped: vec![QuarantineTargetStrip {
                path: "build-slots/p/slot-3.quarantine-1".into(),
                bytes: 4,
                at_unix_ms: 9,
                withheld_outside_lineage: vec![WithheldProcess {
                    pid: 20461,
                    name: Some("login".into()),
                    executable: Some("/usr/bin/login".into()),
                    container: "coalition 803".into(),
                }],
                withheld_outside_lineage_truncated: 1,
            }],
            occupancy_lineage: Some(OccupancyLineageReport {
                current: Some("coalition 19563".into()),
                containers: vec!["coalition 19563".into()],
                unknown: Some("never both, but every key is exercised".into()),
            }),
        };
        let command = ActiveCellRequest {
            executor_id: "executor-a".into(),
            request_id: "request".into(),
            attempt_id: "attempt".into(),
            command: "bun run check".into(),
            command_class: CellCommandClass::Typecheck,
            owner: Some(owner.clone()),
            priority: CellPriority::WriteCheck,
            requesting_job_id: Some("job".into()),
            affinity_key: Some("affinity".into()),
            queued_at_unix_ms: 10,
            started_at_unix_ms: Some(11),
            stage: Some(CellExecutionStage::Running),
            resource_reservation: reservation.clone(),
            learned_estimate: Some(learned_estimate.clone()),
            subscriber_count: 1,
        };
        let residency = CellResidency {
            holder: ResidencyHolder::Job {
                job_id: "job".into(),
            },
            // `ManagedObjects` is the only variant carrying `object_format`, so
            // it is the one that exercises the rename on a field the other two
            // do not have.
            repository: RepositoryLocator::ManagedObjects {
                project_id: "project".into(),
                repository_id: "repository".into(),
                object_format: GitObjectFormat::Sha1,
            },
            owner_ref: Some(owner.clone()),
            selector: Some("feature/branch".into()),
            incarnation_id: "incarnation".into(),
            current_base_commit: BASE_COMMIT.into(),
            phase: ResidencyPhase::Active,
            last_heartbeat_unix_ms: 12,
            reclaim_deadline_unix_ms: 13,
            death_policy: OwnerDeathPolicy {
                heartbeat_timeout_ms: 30_000,
                reclaim_grace_ms: 10_000,
            },
            footprint: ResidencyFootprint {
                memory_bytes: 14,
                disk_growth_bytes: 15,
            },
            state_revision: 3,
            events: vec![
                ResidencyEvent {
                    revision: 1,
                    occurred_at_unix_ms: 16,
                    event: ResidencyEventKind::ProcessStarting {
                        process_key: "tests".into(),
                        generation: 1,
                    },
                },
                ResidencyEvent {
                    revision: 2,
                    occurred_at_unix_ms: 17,
                    event: ResidencyEventKind::ProcessExited {
                        process_key: "tests".into(),
                        generation: 1,
                        restartable: true,
                        executor_lost: true,
                    },
                },
                ResidencyEvent {
                    revision: 3,
                    occurred_at_unix_ms: 18,
                    event: ResidencyEventKind::CheckoutRefreshed {
                        base_commit: BASE_COMMIT.into(),
                    },
                },
            ],
        };
        let mut processes = std::collections::BTreeMap::new();
        processes.insert(
            "session".to_string(),
            sample_resident_process(ResidentProcessKind::Terminal {
                slug: "tests".into(),
            }),
        );
        processes.insert(
            "analysis".to_string(),
            ResidentProcess {
                status: ResidentProcessStatus::Exited {
                    finished_at_unix_ms: 19,
                    exit_code: Some(1),
                    restartable: true,
                    executor_lost: true,
                },
                ..sample_resident_process(ResidentProcessKind::Repl {
                    slug: "analysis".into(),
                })
            },
        );
        processes.insert(
            "dev".to_string(),
            sample_resident_process(ResidentProcessKind::DevInstance {
                source_terminal_session_id: Some("session".into()),
            }),
        );
        processes.insert(
            "workflow".to_string(),
            sample_resident_process(ResidentProcessKind::WorkflowRuntime {
                workflow: "release".into(),
            }),
        );
        let cell = PersistentCellState {
            size_bytes: 4_294_967_296,
            warm_command_classes: Vec::new(),
            executor_id: "executor-a".into(),
            executor_display_name: Some("Executor A".into()),
            project_id: "project".into(),
            cell_id: "slot-1".into(),
            path: "/cell".into(),
            workspace_name: "workspace".into(),
            repository: "/repo".into(),
            checkout_kind: CellCheckoutKind::DetachedGitWorktree,
            git_common_dir: Some("/repo/.git".into()),
            authority_path: "/authority".into(),
            lifecycle: PersistentCellLifecycle::Active,
            cell_epoch: 9,
            last_sealed_commit: Some(SEALED_COMMIT.into()),
            last_distance: None,
            quarantine_reason: None,
            last_used_unix_ms: 20,
            last_affinity_key: Some("affinity".into()),
            preparation_fingerprint: Some("fingerprint".into()),
            residency: Some(residency),
            occupancy: CellOccupancy {
                command: Some(command.clone()),
                command_ownership: None,
                draining: None,
                processes,
            },
        };
        // Two more residencies, both dev instances: one launched against an
        // agent branch, which the row names by the work it belongs to, and one
        // launched against a bare commit, which names no work at all. The second
        // is the CAIRN-3241 specimen, and the desktop fixture needs it to prove
        // the panel refuses to paint a hash as somebody's identity.
        let branched = dev_instance_cell(
            "slot-2",
            "agent/CAIRN-3232-builder-1",
            Some(CellOwnerRef {
                issue_number: Some(3232),
                ..owner.clone()
            }),
            &cell,
        );
        let detached = dev_instance_cell("slot-3", DEV_INSTANCE_COMMIT, None, &cell);
        let service = service_cell("slot-4", &cell);
        SubstrateHealthSnapshot {
            schema_version: SUBSTRATE_HEALTH_SCHEMA_VERSION,
            captured_at_unix_ms: 42,
            // The CAIRN-3356 specimen: three enrolled machines, none attached,
            // for three different reasons. The panel fixture carries all three
            // so the rendering is exercised against the serializer's own output
            // rather than against a hand-built idea of it — and so the calm
            // tiers cannot quietly start painting like the loud one.
            enrolled_remotes: vec![
                EnrolledRemote {
                    executor_id: "bglab-mac".into(),
                    name: "bglab-mac".into(),
                    os: "macos".into(),
                    arch: "aarch64".into(),
                    link: RemoteLinkState::BootstrapFailed {
                        cause: "no aarch64-apple-darwin cairn-cmd artifact published".into(),
                    },
                    last_attempt: Some(
                        RemoteAttachAttempt::bootstrap_failure(
                            1_785_124_850_000,
                            "no aarch64-apple-darwin cairn-cmd artifact published for executor protocol v28",
                        )
                        .with_artifact_fault(Some(RemoteArtifactFault::Unpublished)),
                    ),
                    last_seen_unix_ms: Some(1_785_117_770_000),
                    connection_timeline: Vec::new(),
                    drained: false,
                },
                EnrolledRemote {
                    executor_id: "bglab-ub".into(),
                    name: "bglab-ub".into(),
                    os: "linux".into(),
                    arch: "x86_64".into(),
                    link: RemoteLinkState::Absent {
                        last_beat_unix_ms: Some(1_785_114_170_000),
                        last_reason: "the link went away; the machine reconnects on its own".into(),
                    },
                    last_attempt: Some(RemoteAttachAttempt::bootstrap_failure(
                        1_785_124_925_000,
                        "ssh: connect to host bglab-ub port 22: No route to host",
                    )),
                    last_seen_unix_ms: Some(1_785_114_170_000),
                    connection_timeline: Vec::new(),
                    // The machine an operator drained while it was unreachable:
                    // the fact travels on the wire whether or not the machine
                    // is there to confirm it.
                    drained: true,
                },
                EnrolledRemote {
                    executor_id: "bglab-win".into(),
                    name: "bglab-win".into(),
                    os: "windows".into(),
                    arch: "x86_64".into(),
                    link: RemoteLinkState::NeverConnected {
                        since_bootstrap_unix_ms: 1_785_124_925_000,
                    },
                    last_attempt: None,
                    last_seen_unix_ms: None,
                    connection_timeline: Vec::new(),
                    drained: false,
                },
            ],
            compile_cache: Some(CompileCacheHealth {
                service: "sccache".into(),
                state: CompileCacheState::Healthy,
                generation: 3,
                restart_count: 2,
                consecutive_failures: 0,
                next_attempt_unix_ms: None,
                state_changed_at_unix_ms: 1_785_124_970_000,
                stats: Measurement::measured(
                    1_785_124_975_000,
                    CompileCacheStats {
                        compile_requests: 4_812,
                        cache_hits: 4_101,
                        cache_misses: 502,
                        non_cacheable: 209,
                        cache_errors: 0,
                        compiles_executed: 502,
                        compilations: 502,
                        compile_failures: 0,
                        cache_size_bytes: Some(12_884_901_888),
                        max_cache_size_bytes: Some(53_687_091_200),
                    },
                ),
                condition: None,
            }),
            status: SubstrateHealthStatus::Degraded,
            warming_up: false,
            reasons: vec![
                SubstrateHealthReason::NoExecutors,
                SubstrateHealthReason::DiskAccountingPartial {
                    executor_id: "executor-a".into(),
                    skipped_entries: 2,
                    skipped: vec![SkippedEntry {
                        path: "build-slots/slot-3".into(),
                        operation: SkippedEntryOperation::ReadMetadata,
                        reason: SkippedEntryReason::PermissionDenied,
                    }],
                },
                SubstrateHealthReason::MeasurementUnavailable {
                    executor_id: "executor-a".into(),
                    measurement: MachineMeasurement::ProcessPhysicalFootprint,
                    reason: MeasurementGap::UnsupportedPlatform,
                },
                SubstrateHealthReason::StaleTelemetry {
                    executor_id: "executor-a".into(),
                    age_ms: 120_000,
                },
            ],
            executors: vec![ExecutorHealthSnapshot {
                identity: sample_advertisement().identity,
                public_name: "local".into(),
                colocated: true,
                status: ExecutorHealthStatus::Online,
                heartbeat_age_ms: 21,
                liveness_age_ms: Some(38),
                telemetry_stale: false,
                advertisement: ExecutorAdvertisement {
                    liveness_observed_at_unix_ms: Some(39),
                    ..sample_advertisement()
                },
                admission: AdmissionHealth {
                    concurrency_capacity: Some(8),
                    interactive_reserve: 0,
                    memory_capacity_bytes: Some(22),
                    disk_growth_capacity_bytes: Some(23),
                    active_reservation: reservation.clone(),
                    queued_reservation_bytes: 24,
                    accepted_count: 25,
                    rejected_count: 26,
                    timed_out_count: 27,
                },
                queues: vec![QueueClassHealth {
                    priority: CellPriority::AgentInteractive,
                    depth: 1,
                    oldest_age_ms: Some(28),
                    waits: bounded.clone(),
                }],
                host,
                disk,
                machine,
                inventory: inventory.clone(),
                connection_generation: 2,
                applied_policy: ExecutorRuntimePolicy::default(),
                drain_mode: true,
                resident_processes: ResidentProcessHealth::default(),
                command_processes: CommandProcessHealth::default(),
                evicted_cells: Vec::new(),
                build_skew: Some(BuildSkew {
                    runner_build_id: "runner".into(),
                    executor_build_id: "executor".into(),
                }),
            }],
            occupancy: CellLifecycleCensus {
                total: 1,
                running: 1,
                ..CellLifecycleCensus::default()
            },
            inventory,
            fleet: FleetSnapshot {
                cells: vec![cell, branched, detached, service],
                queued_requests: vec![QueuedCellRequest {
                    command_resource_identity: None,
                    admission_kind: CellAdmissionKind::Residency,
                    executor_id: "executor-a".into(),
                    request_id: "queued".into(),
                    attempt_id: "attempt".into(),
                    project_id: "project".into(),
                    command: "bun run test".into(),
                    command_class: CellCommandClass::Vitest,
                    owner: Some(owner.clone()),
                    priority: CellPriority::ReviewCheck,
                    effective_priority: Some(CellPriority::AgentInteractive),
                    requesting_job_id: Some("job".into()),
                    affinity_key: Some("affinity".into()),
                    queued_at_unix_ms: 29,
                    resource_reservation: reservation.clone(),
                    learned_estimate: Some(learned_estimate.clone()),
                    subscriber_count: 1,
                    substrate_hold: Some(substrate_state.clone()),
                    placement: None,
                }],
                executing_requests: vec![ExecutingCellRequest {
                    command_resource_identity: None,
                    executor_id: "executor-a".into(),
                    cell_id: "slot-1".into(),
                    request_id: "request".into(),
                    attempt_id: "attempt".into(),
                    owner: Some(owner.clone()),
                    command_class: CellCommandClass::Typecheck,
                    command: "bun run check".into(),
                    started_at_unix_ms: 30,
                    process_ids: vec![1234],
                    priority: Some(CellPriority::WriteCheck),
                    subscriber_count: 1,
                    resource_reservation: reservation.clone(),
                    learned_estimate: Some(learned_estimate.clone()),
                }],
                in_flight_residency_operations: HashMap::new(),
                recent_completions: vec![CellCompletion {
                    executor_id: "executor-a".into(),
                    request_id: "request".into(),
                    attempt_id: "attempt".into(),
                    owner: Some(owner),
                    command_class: CellCommandClass::CargoClippy,
                    command: "bun run check:rust".into(),
                    priority: CellPriority::ReviewCheck,
                    queued_at_unix_ms: 31,
                    started_at_unix_ms: Some(32),
                    finished_at_unix_ms: 33,
                    duration_ms: 1,
                    verdict: CellCompletionVerdict::Succeeded,
                    failure_output_tail: None,
                    transcript_event_uri: None,
                    resource_reservation: Some(reservation.clone()),
                    learned_estimate: Some(learned_estimate),
                    actuals: Some(CellExecutionMeta {
                        warmth: None,
                        load_context: None,
                        executor_id: "executor-a".into(),
                        executor_device_id: "device".into(),
                        executor_connection_generation: 2,
                        cell_id: "slot-1".into(),
                        cell_epoch: 9,
                        started_at_unix_ms: 32,
                        finished_at_unix_ms: 33,
                        duration_ms: Some(1),
                        peak_rss_bytes: Some(34),
                        peak_physical_footprint_bytes: Some(35),
                        cpu: Some(CpuTime::sampled(37)),
                        allocation: Some(ExecutionAllocation::uncapped(
                            2,
                            NoCapReason::PlatformHasNoCap,
                            Some(CpuTime::sampled(37)),
                            1,
                        )),
                        disk_delta_bytes: Some(36),
                        measurement_quality: Some(ExecutionMeasurementQuality {
                            duration: MeasurementQuality::Authoritative,
                            memory: MeasurementQuality::Sampled,
                            disk: MeasurementQuality::Approximate,
                            memory_platform: Some("macos".into()),
                            disk_boundary: "cell".into(),
                        }),
                        environment_fingerprint: String::new(),
                        verdict_platform: None,
                        verdict_arch: None,
                        toolchain_fingerprint: None,
                        verdict_environment_hash: None,
                        sandbox: None,
                        containment: None,
                    }),
                    cached: false,
                    subscriber_count: 1,
                    served_at_unix_ms: 37,
                }],
                granted_attempts: vec![GrantedAttempt {
                    request_id: "executing".into(),
                    attempt_id: "attempt".into(),
                }],
                resident_occupancy: Some(ResidentOccupancyEvidence {
                    process_count: 4,
                    reservation: reservation.clone(),
                }),
                substrate_state: Some(substrate_state),
                // Empty and zero, so they serialize to nothing and the checked-in
                // running-panel fixture stays the serializer's exact output.
                provisioning_holds: Vec::new(),
                orphaned_provisioning_releases: 0,
            },
            store_locks: vec![StoreLockHealth {
                store: "/tmp/store".into(),
                waiter_count: 1,
                waits: bounded,
                holds: BoundedDurationSummary::default(),
            }],
        }
    }

    /// Every object key in `value`, as a slash-joined path.
    ///
    /// Map *keys* are data rather than field names — a terminal slug or a REPL
    /// name may legally contain an underscore — so the one data-keyed map in
    /// this tree, a cell's `processes`, contributes its values but not its keys.
    fn serialized_key_paths(value: &serde_json::Value, path: &str, out: &mut Vec<String>) {
        match value {
            serde_json::Value::Object(fields) => {
                let data_keyed = path.ends_with("/processes");
                for (key, child) in fields {
                    if !data_keyed {
                        out.push(format!("{path}/{key}"));
                    }
                    serialized_key_paths(child, &format!("{path}/{key}"), out);
                }
            }
            serde_json::Value::Array(items) => {
                for item in items {
                    serialized_key_paths(item, &format!("{path}[]"), out);
                }
            }
            _ => {}
        }
    }

    /// The class behind CAIRN-3195, closed at the boundary rather than at the one
    /// type that happened to break: the snapshot the desktop UI reads is
    /// camelCase all the way down, so a struct-variant field that forgets
    /// `rename_all_fields` fails here instead of silently emptying a panel.
    ///
    /// The frontend's own tests cannot catch this. Their fixtures are
    /// hand-written camelCase objects, so they agree with the TypeScript types no
    /// matter what the Rust serializer actually emits. This test is the only
    /// place the two meet.
    #[test]
    fn ui_facing_snapshot_serializes_only_camel_case_keys() {
        let snapshot = fully_populated_health_snapshot();
        let value = serde_json::to_value(&snapshot).unwrap();
        let mut paths = Vec::new();
        serialized_key_paths(&value, "", &mut paths);

        let snake: Vec<&String> = paths
            .iter()
            .filter(|path| path.rsplit('/').next().is_some_and(|key| key.contains('_')))
            .collect();
        assert!(
            snake.is_empty(),
            "these snapshot keys reach the desktop UI in snake_case, where nothing reads them. \
             Add `rename_all_fields = \"camelCase\"` to the enum that owns each: {snake:#?}"
        );

        // A fixture that stopped reaching the interesting subtrees would pass the
        // assertion above while testing nothing, which is how the snake_case
        // status shipped in the first place.
        for expected in [
            "/buildSlots/slots[]/occupancy/processes/session/status/startedAtUnixMs",
            "/buildSlots/slots[]/occupancy/processes/analysis/status/finishedAtUnixMs",
            "/buildSlots/slots[]/occupancy/processes/session/spec/cwdRoot",
            "/buildSlots/slots[]/residency/repository/objectFormat",
            "/buildSlots/slots[]/residency/events[]/event/processKey",
            "/executors[]/host/pressure/conditions[]/memoryAvailable/availableBytes",
            "/executors[]/host/pressure/conditions[]/residentOccupancy/processCount",
            "/executors[]/livenessAgeMs",
            "/executors[]/telemetryStale",
            "/executors[]/advertisement/livenessObservedAtUnixMs",
            "/executors[]/machine/cpu/measuredAtUnixMs",
            "/executors[]/machine/memory/reading/value/availableBytes",
            "/executors[]/machine/diskAccounting/reading/value/skipped[]/path",
            "/executors[]/machine/process/physicalFootprintBytes/reading/reason",
            "/reasons[]/diskAccountingPartial/skippedEntries",
            "/reasons[]/diskAccountingPartial/skipped[]/operation",
            "/reasons[]/measurementUnavailable/measurement",
            "/reasons[]/staleTelemetry/ageMs",
        ] {
            assert!(
                paths.iter().any(|path| path == expected),
                "the fixture no longer serializes {expected}, so this test has stopped \
                 covering the subtree it names"
            );
        }

        assert_eq!(
            serde_json::from_value::<SubstrateHealthSnapshot>(value).unwrap(),
            snapshot
        );
    }

    /// The desktop's Running-panel fixture, written by the serializer the runner
    /// actually ships.
    ///
    /// The frontend's own fixtures are hand-written camelCase objects: they
    /// agree with the TypeScript types no matter what Rust emits, which is how
    /// CAIRN-3195 emptied the whole panel behind green tests. This test is the
    /// one place a desktop fixture is made to be the wire's own bytes — change
    /// the snapshot shape and it fails here until the fixture is regenerated,
    /// and the tests that read it see the change.
    ///
    /// Regenerate with `UPDATE_FIXTURES=1 cargo test -p cairn-common
    /// running_panel_fixture`.
    #[test]
    fn running_panel_fixture_is_the_wire_serializers_own_output() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../../fixtures/substrate-health-snapshot.json");
        let rendered = format!(
            "{}\n",
            serde_json::to_string_pretty(&fully_populated_health_snapshot()).unwrap()
        );
        if std::env::var_os("UPDATE_FIXTURES").is_some() {
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, &rendered).unwrap();
        }
        let checked_in = std::fs::read_to_string(&path).unwrap_or_default();
        assert_eq!(
            checked_in,
            rendered,
            "{} no longer matches what this serializer emits. Regenerate it with \
             `UPDATE_FIXTURES=1 cargo test -p cairn-common running_panel_fixture` and \
             rerun the desktop tests that read it.",
            path.display()
        );
    }

    /// The retired key is not an alias, for the same reason the placement
    /// selector's is not: a v31 executor requires `service` and would fail to
    /// decode a `StartProcess` naming `name`, so the version number moved with
    /// the field and the handshake refuses that peer before it is asked to
    /// place anything.
    ///
    /// On disk the same shape is tolerated instead — see the adoption test
    /// below. A live peer can renegotiate and a persisted cell cannot, so the
    /// old key is ignored there rather than refused, and never becomes the
    /// words a person reads.
    #[test]
    fn the_retired_service_key_is_not_decoded_as_an_identity() {
        const { assert!(EXECUTOR_PROTOCOL_VERSION >= 33) };
        let kind = ResidentProcessKind::Service {
            name: "iMessage channel".into(),
            role: "watch".into(),
        };
        assert_eq!(
            serde_json::to_value(&kind).unwrap(),
            serde_json::json!({"kind": "service", "name": "iMessage channel", "role": "watch"}),
            "the wire carries the words, and carries them under the new key alone"
        );
        assert_eq!(
            serde_json::from_value::<ResidentProcessKind>(
                serde_json::json!({"kind": "service", "service": "channel-imessage"})
            )
            .unwrap(),
            ResidentProcessKind::Service {
                name: String::new(),
                role: String::new(),
            },
            "a lease id must never be adopted as somebody's identity"
        );
    }

    /// A service process recorded before services declared an identity must
    /// still decode, for the same reason the camelCase case must: adoption
    /// skips a cell it cannot decode, and the operator whose iMessage watch
    /// prompted CAIRN-3435 has exactly this shape on disk right now. Skipping
    /// it would leave that watch running, invisible, and unaddressable.
    ///
    /// The old `service` key held the lease id, not words. It is deliberately
    /// not read into `name`: a row saying `channel-imessage` would be this
    /// issue's own bug wearing a different string. Absent words decode as
    /// absent, and the surface degrades honestly.
    #[test]
    fn a_service_process_predating_the_identity_contract_still_decodes() {
        let persisted = serde_json::json!({
            "generation": 1,
            "kind": { "kind": "service", "service": "channel-imessage" },
            "status": { "status": "running", "startedAtUnixMs": 1_785_124_970_255_u64 }
        });
        let decoded = serde_json::from_value::<ResidentProcess>(persisted)
            .expect("a service cell that cannot decode is a service cell adoption orphans");
        assert_eq!(
            decoded.kind,
            ResidentProcessKind::Service {
                name: String::new(),
                role: String::new(),
            },
            "the lease id must not arrive as the words a person reads"
        );
        assert!(decoded.is_live());
    }

    /// The three keys `useBuildFabric` reads off a resident process to decide
    /// whether it is running work, restartable work, or nothing at all. Reading
    /// any of them as `undefined` drops the row, so they are pinned by name.
    #[test]
    fn resident_process_status_exposes_the_keys_the_running_panel_reads() {
        let running = serde_json::to_value(ResidentProcessStatus::Running {
            started_at_unix_ms: 1_785_185_750_222,
            process_group_id: Some(62_961),
            ownership: None,
        })
        .unwrap();
        assert_eq!(running["status"], "running");
        assert_eq!(running["startedAtUnixMs"], 1_785_185_750_222_u64);
        assert_eq!(running["processGroupId"], 62_961);

        let exited = serde_json::to_value(ResidentProcessStatus::Exited {
            finished_at_unix_ms: 7,
            exit_code: Some(1),
            restartable: true,
            executor_lost: true,
        })
        .unwrap();
        assert_eq!(exited["status"], "exited");
        assert_eq!(exited["finishedAtUnixMs"], 7);
        assert_eq!(exited["exitCode"], 1);
        assert_eq!(exited["restartable"], true);
        assert_eq!(exited["executorLost"], true);
    }

    /// A `cairn-build-slot-state.json` written before the fields above became
    /// camelCase must still decode, or adoption skips the cell: its PTY process
    /// groups are never killed and its warm checkout is quarantined on the next
    /// provisioning attempt. The literal below is the shape found in real state
    /// files on disk, snake_case keys beside camelCase siblings and all.
    #[test]
    fn persisted_cell_state_predating_camel_case_fields_still_decodes() {
        let persisted = serde_json::json!({
            "executorId": "colocated",
            "projectId": "project",
            "slotId": "slot-1",
            "path": "/cell",
            "repository": "/repo",
            "lifecycle": "idle",
            "cellEpoch": 9,
            "lastSealedCommit": null,
            "lastUsedUnixMs": 20,
            "residency": {
                "holder": { "kind": "job", "jobId": "job-1" },
                "repository": {
                    "kind": "colocatedPath",
                    "project_id": "project",
                    "repository_id": "repository",
                    "absolute_path": "/repo"
                },
                "currentBaseCommit": "commit",
                "phase": "awaitingReclaim",
                "lastHeartbeatUnixMs": 12,
                "reclaimDeadlineUnixMs": 13,
                "deathPolicy": { "heartbeatTimeoutMs": 30000, "reclaimGraceMs": 10000 },
                "footprint": { "memoryBytes": 14, "diskGrowthBytes": 15 },
                "stateRevision": 3,
                "events": [{
                    "revision": 1,
                    "occurredAtUnixMs": 16,
                    "event": { "kind": "processExited", "process_key": "main", "generation": 1,
                               "restartable": true, "executor_lost": true }
                }]
            },
            "occupancy": {
                "processes": {
                    "main": {
                        "generation": 1,
                        "kind": { "kind": "terminal", "slug": "tests" },
                        "status": {
                            "status": "running",
                            "started_at_unix_ms": 1785124970255_u64,
                            "process_group_id": 21586
                        }
                    }
                }
            }
        });
        let state: PersistentCellState =
            serde_json::from_value(persisted).expect("a pre-rename state file must still decode");

        let residency = state.residency.expect("residency");
        assert_eq!(
            residency.repository,
            RepositoryLocator::ColocatedPath {
                project_id: "project".into(),
                repository_id: "repository".into(),
                absolute_path: "/repo".into(),
            }
        );
        assert_eq!(
            residency.events[0].event,
            ResidencyEventKind::ProcessExited {
                process_key: "main".into(),
                generation: 1,
                restartable: true,
                executor_lost: true,
            }
        );
        assert_eq!(
            state.occupancy.processes["main"].status,
            ResidentProcessStatus::Running {
                started_at_unix_ms: 1_785_124_970_255,
                process_group_id: Some(21_586),
                ownership: None,
            }
        );
    }

    /// The contract a compile-cache reading has to keep on the wire, and the
    /// one it must never break: a cache nobody could measure is a named gap,
    /// not a cache with no hits. Those render identically to a person if the
    /// distinction is lost in transit, and "0% hit rate" is precisely how a
    /// dead daemon would be mistaken for a working-but-useless one.
    #[test]
    fn compile_cache_reports_measured_and_unavailable_statistics_distinctly() {
        let measured = CompileCacheHealth {
            service: "sccache".into(),
            state: CompileCacheState::Healthy,
            generation: 2,
            restart_count: 1,
            consecutive_failures: 0,
            next_attempt_unix_ms: None,
            state_changed_at_unix_ms: 1_000,
            stats: Measurement::measured(
                2_000,
                CompileCacheStats {
                    compile_requests: 10,
                    cache_hits: 6,
                    cache_misses: 2,
                    non_cacheable: 2,
                    cache_errors: 0,
                    compiles_executed: 2,
                    compilations: 2,
                    compile_failures: 0,
                    cache_size_bytes: Some(1_024),
                    max_cache_size_bytes: Some(2_048),
                },
            ),
            condition: None,
        };
        let value = serde_json::to_value(&measured).unwrap();
        assert_eq!(value["state"], "healthy");
        assert_eq!(value["stats"]["reading"]["kind"], "measured");
        assert_eq!(value["stats"]["reading"]["value"]["cacheHits"], 6);
        assert_eq!(value["stats"]["measuredAtUnixMs"], 2_000);
        assert_eq!(
            serde_json::from_value::<CompileCacheHealth>(value).unwrap(),
            measured
        );

        let dead = CompileCacheHealth {
            state: CompileCacheState::RecoveryFailed,
            consecutive_failures: 7,
            next_attempt_unix_ms: Some(9_000),
            stats: Measurement::unavailable_with(
                2_000,
                MeasurementGap::NotSampled,
                "the compile cache is down",
            ),
            condition: Some("sccache port conflict".into()),
            ..measured.clone()
        };
        let value = serde_json::to_value(&dead).unwrap();
        assert_eq!(value["state"], "recoveryFailed");
        assert_eq!(value["stats"]["reading"]["kind"], "unavailable");
        assert_eq!(value["stats"]["reading"]["reason"], "notSampled");
        // The gap carries no counters at all, so nothing downstream can read a
        // zero out of it by accident.
        assert!(value["stats"]["reading"]["value"].is_null());
        assert_eq!(value["nextAttemptUnixMs"], 9_000);
        assert_eq!(
            serde_json::from_value::<CompileCacheHealth>(value).unwrap(),
            dead
        );

        // A cache asked nothing cacheable has not failed, so it states no rate
        // rather than a zero one.
        assert_eq!(CompileCacheStats::default().hit_rate(), None);
        assert_eq!(
            CompileCacheStats {
                cache_hits: 3,
                cache_misses: 1,
                ..CompileCacheStats::default()
            }
            .hit_rate(),
            Some(0.75)
        );
    }

    /// A UI invalidation must follow news, not the clock. The supervisor
    /// re-samples every tick, so a fresh timestamp over identical counters is
    /// the common case and must not wake anything.
    #[test]
    fn compile_cache_change_detection_ignores_a_restated_identical_sample() {
        let base = CompileCacheHealth {
            service: "sccache".into(),
            state: CompileCacheState::Healthy,
            generation: 1,
            restart_count: 0,
            consecutive_failures: 0,
            next_attempt_unix_ms: None,
            state_changed_at_unix_ms: 1_000,
            stats: Measurement::measured(2_000, CompileCacheStats::default()),
            condition: None,
        };
        let restated = CompileCacheHealth {
            stats: Measurement::measured(9_999, CompileCacheStats::default()),
            ..base.clone()
        };
        assert!(!base.materially_differs(&restated));

        for changed in [
            CompileCacheHealth {
                state: CompileCacheState::Degraded,
                ..base.clone()
            },
            CompileCacheHealth {
                generation: 2,
                ..base.clone()
            },
            CompileCacheHealth {
                stats: Measurement::measured(
                    2_000,
                    CompileCacheStats {
                        cache_hits: 1,
                        ..CompileCacheStats::default()
                    },
                ),
                ..base.clone()
            },
            CompileCacheHealth {
                stats: Measurement::unavailable(2_000, MeasurementGap::NotSampled),
                ..base.clone()
            },
        ] {
            assert!(
                base.materially_differs(&changed),
                "a changed sample must invalidate: {changed:?}"
            );
        }
    }

    #[test]
    fn substrate_health_round_trip_preserves_contract_and_nulls() {
        let snapshot = SubstrateHealthSnapshot {
            schema_version: SUBSTRATE_HEALTH_SCHEMA_VERSION,
            captured_at_unix_ms: 42,
            compile_cache: None,
            // A machine with no attempt and no sighting yet: both nulls are the
            // honest answer and both have to survive the wire, because a client
            // that read them as zero would render a machine last seen in 1970.
            enrolled_remotes: vec![EnrolledRemote {
                executor_id: "bglab-win".into(),
                name: "bglab-win".into(),
                os: "windows".into(),
                arch: "x86_64".into(),
                link: RemoteLinkState::NeverConnected {
                    since_bootstrap_unix_ms: 1_785_124_925_000,
                },
                last_attempt: None,
                last_seen_unix_ms: None,
                connection_timeline: Vec::new(),
                drained: false,
            }],
            status: SubstrateHealthStatus::Degraded,
            warming_up: false,
            reasons: vec![
                SubstrateHealthReason::MeasurementUnavailable {
                    executor_id: "executor-1".into(),
                    measurement: MachineMeasurement::Cpu,
                    reason: MeasurementGap::UnsupportedPlatform,
                },
                SubstrateHealthReason::DiskAccountingPartial {
                    executor_id: "executor-1".into(),
                    skipped_entries: 2,
                    skipped: vec![SkippedEntry {
                        path: "warm-caches/repo-a".into(),
                        operation: SkippedEntryOperation::ReadDirectory,
                        reason: SkippedEntryReason::PermissionDenied,
                    }],
                },
            ],
            executors: vec![],
            occupancy: CellLifecycleCensus::default(),
            inventory: CellInventoryHealth::default(),
            fleet: FleetSnapshot::default(),
            store_locks: vec![StoreLockHealth {
                store: "/tmp/store".into(),
                waiter_count: 0,
                waits: BoundedDurationSummary {
                    sample_count: 1,
                    p50_ms: Some(0),
                    p95_ms: Some(0),
                    max_ms: Some(0),
                },
                holds: BoundedDurationSummary::default(),
            }],
        };
        let value = serde_json::to_value(&snapshot).unwrap();
        assert_eq!(value["schemaVersion"], SUBSTRATE_HEALTH_SCHEMA_VERSION);
        assert_eq!(value["capturedAtUnixMs"], 42);
        assert_eq!(value["status"], "degraded");
        assert_eq!(
            value["reasons"][0]["measurementUnavailable"]["measurement"],
            "cpu"
        );
        assert_eq!(
            value["reasons"][0]["measurementUnavailable"]["reason"],
            "unsupportedPlatform"
        );
        assert_eq!(
            value["reasons"][1]["diskAccountingPartial"]["executorId"],
            "executor-1"
        );
        assert_eq!(
            value["reasons"][1]["diskAccountingPartial"]["skippedEntries"],
            2
        );
        assert_eq!(
            value["reasons"][1]["diskAccountingPartial"]["skipped"][0]["path"],
            "warm-caches/repo-a"
        );
        assert_eq!(value["storeLocks"][0]["waits"]["p50Ms"], 0);
        assert!(value["storeLocks"][0]["holds"]["p50Ms"].is_null());
        assert_eq!(
            serde_json::from_value::<SubstrateHealthSnapshot>(value).unwrap(),
            snapshot
        );
    }

    /// The whole point of the measurement envelope: four states that a nullable
    /// number collapses into one another must stay four states on the wire.
    #[test]
    fn a_measured_zero_a_gap_and_a_partial_scan_are_four_distinct_wire_states() {
        // An unsampled reading is not a zero, and it does not pretend to have
        // been taken at a plausible time.
        let unsampled = serde_json::to_value(Measurement::<u64>::default()).unwrap();
        assert_eq!(unsampled["reading"]["kind"], "unavailable");
        assert_eq!(unsampled["reading"]["reason"], "notSampled");
        assert_eq!(unsampled["measuredAtUnixMs"], 0);

        // A measured zero is a fact, and it survives the round trip as one.
        let zero = Measurement::measured(
            700,
            MachineVolume {
                total_bytes: 10,
                free_bytes: 0,
            },
        );
        let zero_value = serde_json::to_value(&zero).unwrap();
        assert_eq!(zero_value["reading"]["kind"], "measured");
        assert_eq!(zero_value["reading"]["value"]["freeBytes"], 0);
        assert_eq!(
            serde_json::from_value::<Measurement<MachineVolume>>(zero_value).unwrap(),
            zero
        );
        assert_eq!(zero.value().unwrap().free_bytes, 0);
        assert_eq!(zero.gap(), None);

        // A failed reading names the API failure and stamps the attempt, so its
        // age is the age of the failure rather than of the last good value.
        let failed = Measurement::<MachineMemory>::unavailable_with(
            900,
            MeasurementGap::SamplingFailed,
            "GlobalMemoryStatusEx returned 0",
        );
        let failed_value = serde_json::to_value(&failed).unwrap();
        assert_eq!(failed_value["reading"]["reason"], "samplingFailed");
        assert_eq!(
            failed_value["reading"]["detail"],
            "GlobalMemoryStatusEx returned 0"
        );
        assert_eq!(failed.age_ms(1_000), 100);
        assert_eq!(failed.gap(), Some(MeasurementGap::SamplingFailed));

        // A partial scan is measured, not failed: it carries totals and the
        // bounded evidence of what it could not price.
        let partial = DiskAccounting {
            used_bytes: 12,
            categories: DiskCategoryAccounting::default(),
            skipped: vec![SkippedEntry {
                path: "build-slots/slot-3".into(),
                operation: SkippedEntryOperation::ReadMetadata,
                reason: SkippedEntryReason::PermissionDenied,
            }],
            skipped_truncated: 4,
            vanished_entries: 0,
            other_top: Vec::new(),
        };
        assert!(partial.is_partial());
        assert_eq!(partial.skipped_count(), 5);
        assert!(!DiskAccounting::default().is_partial());
        // Entries that stopped existing are churn, not an unmeasured tree: a
        // walk that raced a thousand of them still priced every byte there was.
        let churned = DiskAccounting {
            vanished_entries: 1_000,
            ..DiskAccounting::default()
        };
        assert!(!churned.is_partial());
        assert_eq!(churned.skipped_count(), 0);
        let partial_value = serde_json::to_value(Measurement::measured(5, partial)).unwrap();
        assert_eq!(partial_value["reading"]["kind"], "measured");
        assert_eq!(
            partial_value["reading"]["value"]["skipped"][0]["reason"],
            "permissionDenied"
        );

        // Disk governance keeps only the derived verdict; the bytes are
        // measurements now, and an unknown verdict means the volume reading is a
        // gap rather than a zero.
        let disk = serde_json::to_value(DiskHealth::default()).unwrap();
        assert_eq!(disk["status"], "unknown");
        assert!(disk.get("totalBytes").is_none());
        assert!(disk.get("categories").is_none());
    }

    /// Custodial storage health crosses the wire exactly, and a peer that
    /// predates it is read as silent rather than as healthy. Collapsing those
    /// two is the failure CAIRN-4217 is about, so it is asserted here at the
    /// boundary where the collapse would be cheapest to make.
    #[test]
    fn reclamation_health_round_trips_and_an_older_peer_reads_as_unreported() {
        let legacy = serde_json::json!({
            "budgetBytes": null,
            "status": "ok",
            "sweepStatus": "completed",
            "sweepGeneration": 3,
            "cleanupBlocked": false,
            "cleanupLastError": null,
            "cleanupFailingPath": null,
            "cleanupSkippedEntries": null
        });
        let decoded: DiskHealth = serde_json::from_value(legacy).unwrap();
        assert!(
            decoded.reclamation.is_none(),
            "a peer that cannot speak about reclamation must not be heard saying it is fine"
        );
        assert!(decoded.accounting_cadence.is_none());

        let health = DiskHealth {
            status: DiskHealthStatus::Ok,
            reclamation: Some(StorageReclamationHealth {
                outstanding: 2,
                alarms: vec![StorageReclamationAlarm {
                    path: "/cairn/build-slots/acme/slot-3.quarantine-1750000000000".into(),
                    first_permanent_unix_ms: 100_000,
                    attempts: 7,
                    bytes: 19_327_352_832,
                    last_error: "Permission denied (os error 13)".into(),
                    survivors: "target/debug/build".into(),
                    verification: StorageAlarmVerification::RestoredUnverified,
                }],
            }),
            accounting_cadence: Some(StorageAccountingCadence {
                last_pass_duration_ms: Some(120_000),
                last_drift_bytes: Some(0),
                interval_ms: Some(6_000_000),
                next_due_unix_ms: Some(1_700_000_000_000),
                consecutive_failures: 0,
            }),
            ..DiskHealth::default()
        };
        let value = serde_json::to_value(&health).unwrap();
        assert_eq!(value["reclamation"]["outstanding"], 2);
        let alarm = &value["reclamation"]["alarms"][0];
        assert_eq!(alarm["firstPermanentUnixMs"], 100_000);
        assert_eq!(alarm["lastError"], "Permission denied (os error 13)");
        assert_eq!(alarm["verification"], "restoredUnverified");
        assert_eq!(
            value["accountingCadence"]["nextDueUnixMs"],
            1_700_000_000_000_u64
        );
        assert_eq!(value["accountingCadence"]["lastDriftBytes"], 0);
        assert_eq!(serde_json::from_value::<DiskHealth>(value).unwrap(), health);

        // Zero outstanding is a positive claim, and is not the same value as
        // absence even though both are "nothing to act on right now".
        let clean = DiskHealth {
            reclamation: Some(StorageReclamationHealth::default()),
            ..DiskHealth::default()
        };
        assert!(!clean.reclamation.as_ref().unwrap().is_outstanding());
        assert_ne!(clean.reclamation, DiskHealth::default().reclamation);
    }

    /// A report that predates the machine section decodes as unsampled rather
    /// than as a machine with zero memory on a zero-byte disk.
    #[test]
    fn a_report_without_machine_telemetry_decodes_as_unsampled_not_zero() {
        let mut value = serde_json::to_value(ExecutorSubstrateReport::default()).unwrap();
        value.as_object_mut().unwrap().remove("machine");
        let report: ExecutorSubstrateReport = serde_json::from_value(value).unwrap();
        assert_eq!(report.machine.memory.value(), None);
        assert_eq!(
            report.machine.gaps(),
            vec![
                (MachineMeasurement::Cpu, MeasurementGap::NotSampled),
                (MachineMeasurement::Memory, MeasurementGap::NotSampled),
                (MachineMeasurement::Volume, MeasurementGap::NotSampled),
                (
                    MachineMeasurement::DiskAccounting,
                    MeasurementGap::NotSampled
                ),
                (
                    MachineMeasurement::ProcessResident,
                    MeasurementGap::NotSampled
                ),
                (
                    MachineMeasurement::ProcessPhysicalFootprint,
                    MeasurementGap::NotSampled
                ),
                (
                    MachineMeasurement::ProcessVirtual,
                    MeasurementGap::NotSampled
                ),
                (
                    MachineMeasurement::ProcessAllocator,
                    MeasurementGap::NotSampled
                ),
            ]
        );
        assert_eq!(report.machine.oldest_measured_age_ms(1_000), None);
    }

    /// A reading that no platform outside macOS will ever have must not be able
    /// to speak for whether a machine can take work.
    ///
    /// `gaps` is the complete list, which is what the operator panel renders.
    /// `placement_gaps` is the subset a verdict may be built from, and the two
    /// differ exactly by the readings that are diagnosis rather than capacity.
    #[test]
    fn diagnostic_gaps_are_visible_but_are_not_placement_gaps() {
        let windows_shaped = MachineTelemetry {
            cpu: Measurement::measured(
                1,
                CpuPressure {
                    utilization: 0.4,
                    user: 0.3,
                    system: 0.1,
                    logical_cores: 16,
                },
            ),
            memory: Measurement::measured(
                1,
                MachineMemory {
                    total_bytes: 64,
                    available_bytes: 40,
                },
            ),
            volume: Measurement::measured(
                1,
                MachineVolume {
                    total_bytes: 900,
                    free_bytes: 400,
                },
            ),
            disk_accounting: Measurement::measured(1, DiskAccounting::default()),
            process: ProcessTelemetry {
                resident_bytes: Measurement::measured(1, 17),
                physical_footprint_bytes: Measurement::unavailable(
                    1,
                    MeasurementGap::UnsupportedPlatform,
                ),
                virtual_bytes: Measurement::unavailable(1, MeasurementGap::UnsupportedPlatform),
                allocator: Measurement::unavailable(1, MeasurementGap::UnsupportedPlatform),
                retained: ExecutorRetainedState::default(),
            },
        };
        assert_eq!(
            windows_shaped.gaps(),
            vec![
                (
                    MachineMeasurement::ProcessPhysicalFootprint,
                    MeasurementGap::UnsupportedPlatform
                ),
                (
                    MachineMeasurement::ProcessVirtual,
                    MeasurementGap::UnsupportedPlatform
                ),
                (
                    MachineMeasurement::ProcessAllocator,
                    MeasurementGap::UnsupportedPlatform
                )
            ],
            "the gaps are real and the panel still shows them"
        );
        assert_eq!(
            windows_shaped.placement_gaps(),
            vec![],
            "but the daemon's own size cannot make a healthy machine look unknown"
        );

        assert!(MachineMeasurement::Cpu.is_placement_input());
        assert!(MachineMeasurement::Memory.is_placement_input());
        assert!(MachineMeasurement::Volume.is_placement_input());
        for diagnostic in [
            MachineMeasurement::DiskAccounting,
            MachineMeasurement::ProcessResident,
            MachineMeasurement::ProcessPhysicalFootprint,
            MachineMeasurement::ProcessVirtual,
            MachineMeasurement::ProcessAllocator,
        ] {
            assert!(
                !diagnostic.is_placement_input(),
                "{} answers how the machine is doing, not whether it can take work",
                diagnostic.as_str()
            );
        }

        // A placement input that goes missing is still a placement gap.
        let blind = MachineTelemetry {
            volume: Measurement::unavailable(2, MeasurementGap::SamplingFailed),
            ..windows_shaped
        };
        assert_eq!(
            blind.placement_gaps(),
            vec![(MachineMeasurement::Volume, MeasurementGap::SamplingFailed)]
        );
    }

    /// Telemetry age is the oldest *value*, so a gap cannot make a machine look
    /// stale and a fresh gap cannot make it look fresh.
    #[test]
    fn telemetry_age_reads_the_oldest_value_and_ignores_gaps() {
        let telemetry = MachineTelemetry {
            cpu: Measurement::measured(
                9_000,
                CpuPressure {
                    utilization: 0.25,
                    user: 0.1875,
                    system: 0.0625,
                    logical_cores: 8,
                },
            ),
            memory: Measurement::measured(
                4_000,
                MachineMemory {
                    total_bytes: 16,
                    available_bytes: 8,
                },
            ),
            volume: Measurement::unavailable(9_900, MeasurementGap::SamplingFailed),
            ..MachineTelemetry::default()
        };
        assert_eq!(telemetry.oldest_measured_age_ms(10_000), Some(6_000));
        assert_eq!(
            telemetry.gaps(),
            vec![
                (MachineMeasurement::Volume, MeasurementGap::SamplingFailed),
                (
                    MachineMeasurement::DiskAccounting,
                    MeasurementGap::NotSampled
                ),
                (
                    MachineMeasurement::ProcessResident,
                    MeasurementGap::NotSampled
                ),
                (
                    MachineMeasurement::ProcessPhysicalFootprint,
                    MeasurementGap::NotSampled
                ),
                (
                    MachineMeasurement::ProcessVirtual,
                    MeasurementGap::NotSampled
                ),
                (
                    MachineMeasurement::ProcessAllocator,
                    MeasurementGap::NotSampled
                ),
            ]
        );
    }

    #[test]
    fn a_legacy_occupant_cell_carries_neither_residency_nor_occupancy() {
        let legacy = serde_json::json!({
            "projectId": "p",
            "slotId": "slot-7",
            "path": "/tmp/slot-7",
            "workspaceName": "slot-7",
            "repository": "/tmp/repo",
            "materializationKind": "jujutsuWorkspace",
            "lifecycle": "active",
            "cellEpoch": 3,
            "lastSealedCommit": null,
            "lastUsedUnixMs": 4,
            "occupant": {
                "kind": "lifetime",
                "state": { "declaration": { "leaseId": "job:job-1" } }
            }
        });
        let state: PersistentCellState = serde_json::from_value(legacy).unwrap();
        assert_eq!(state.cell_id, "slot-7");
        assert_eq!(state.residency, None);
        assert!(state.occupancy.is_empty());

        let value = serde_json::to_value(state).unwrap();
        assert_eq!(value["slotId"], "slot-7");
        assert_eq!(value["materializationKind"], "jujutsuWorkspace");
        assert!(value.get("occupant").is_none());
        assert!(value.get("occupancy").is_none());
        assert!(value.get("cellId").is_none());
        assert!(value.get("checkoutKind").is_none());
    }

    #[test]
    fn occupancy_evidence_counts_processes_not_holders() {
        let fleet = FleetSnapshot {
            resident_occupancy: Some(ResidentOccupancyEvidence {
                process_count: 2,
                ..ResidentOccupancyEvidence::default()
            }),
            ..FleetSnapshot::default()
        };
        let fleet_value = serde_json::to_value(&fleet).unwrap();
        assert_eq!(fleet_value["residentOccupancy"]["processCount"], 2);
        let fleet_round_trip: FleetSnapshot = serde_json::from_value(fleet_value).unwrap();
        assert_eq!(
            fleet_round_trip.resident_occupancy.unwrap().process_count,
            2
        );

        let inventory = CellInventoryHealth {
            resident_occupancy: 3,
            ..CellInventoryHealth::default()
        };
        let inventory_value = serde_json::to_value(&inventory).unwrap();
        assert_eq!(inventory_value["residentOccupancy"], 3);
        let inventory_round_trip: CellInventoryHealth =
            serde_json::from_value(inventory_value).unwrap();
        assert_eq!(inventory_round_trip.resident_occupancy, 3);
    }

    #[test]
    fn renamed_checkout_variants_preserve_wire_tags() {
        assert_eq!(
            serde_json::to_value(CellExecutionStage::CheckingOut).unwrap(),
            "materializing"
        );
        assert_eq!(
            serde_json::to_value(StorageFailureStage::ProvisioningCheckout).unwrap(),
            "provisioningMaterialization"
        );
        let operation = ResidencyOperation::RefreshCheckout {
            fence: ResidencyFence {
                holder: sample_holder(),
                incarnation_id: "incarnation".into(),
                cell_epoch: 1,
            },
            base_commit: "base".into(),
            require_clean: false,
        };
        assert_eq!(
            serde_json::to_value(operation).unwrap()["operation"],
            "refreshMaterialization"
        );
    }

    /// One policy for one question: the watermark, the critical line, and the
    /// pressure line must stand in that order, and a policy written before the
    /// critical line existed still loads, with the default filled in.
    #[test]
    fn runtime_policy_orders_watermark_critical_and_pressure_lines() {
        let valid = ExecutorRuntimePolicy::default();
        assert!(valid.free_disk_watermark_bytes < valid.storage_critical_free_bytes);
        assert!(valid.storage_critical_free_bytes < valid.idle_retention_pressure_free_bytes);
        assert!(ExecutorRuntimePolicy {
            storage_critical_free_bytes: valid.idle_retention_pressure_free_bytes,
            ..valid.clone()
        }
        .validate()
        .is_err());
        assert!(ExecutorRuntimePolicy {
            storage_critical_free_bytes: valid.free_disk_watermark_bytes,
            ..valid.clone()
        }
        .validate()
        .is_err());

        let mut written_before = serde_json::to_value(&valid).unwrap();
        written_before
            .as_object_mut()
            .unwrap()
            .remove("storageCriticalFreeBytes");
        let loaded: ExecutorRuntimePolicy = serde_json::from_value(written_before).unwrap();
        assert_eq!(
            loaded.storage_critical_free_bytes,
            STORAGE_CRITICAL_FREE_BYTES
        );
        assert!(loaded.validate().is_ok());
    }

    /// `tight` is a verdict an older runner cannot decode, and the typed disk
    /// refusal and thresholds default when absent from an older executor.
    #[test]
    fn tight_verdict_and_disk_refusal_round_trip_and_default() {
        assert_eq!(
            serde_json::to_value(DiskHealthStatus::Tight).unwrap(),
            "tight"
        );
        let decline = DiskDecline {
            free_bytes: 1,
            floor_bytes: 2,
            outlook: DiskReliefOutlook::Unrelieved { held_bytes: 3 },
        };
        let value = serde_json::to_value(decline).unwrap();
        assert_eq!(value["outlook"]["state"], "unrelieved");
        assert_eq!(value["outlook"]["heldBytes"], 3);
        assert_eq!(
            serde_json::from_value::<DiskDecline>(value).unwrap(),
            decline
        );

        let older: DiskHealth = serde_json::from_value(serde_json::json!({
            "budgetBytes": null,
            "status": "ok",
            "sweepStatus": "completed",
            "sweepGeneration": 1,
            "cleanupBlocked": false,
            "cleanupLastError": null,
            "cleanupFailingPath": null,
            "cleanupSkippedEntries": null
        }))
        .unwrap();
        assert_eq!(older.watermark_bytes, 0);
        assert_eq!(older.pressure, None);
        assert!(older.quarantine_targets_stripped.is_empty());
    }

    #[test]
    fn runtime_policy_validation_rejects_zero_without_weakening_optional_budgets() {
        let valid = ExecutorRuntimePolicy::default();
        assert!(valid.validate().is_ok());
        assert!(ExecutorRuntimePolicy {
            concurrency_units: 0,
            ..valid.clone()
        }
        .validate()
        .is_err());
        assert!(ExecutorRuntimePolicy {
            idle_retention_floor_per_project: 0,
            ..valid.clone()
        }
        .validate()
        .is_err());
        assert!(
            ExecutorRuntimePolicy {
                idle_retention_floor_per_project: 4,
                idle_retention_ceiling_per_project: 2,
                ..valid.clone()
            }
            .validate()
            .is_err(),
            "a ceiling under the floor would evict below the guaranteed warm cell"
        );
        assert!(ExecutorRuntimePolicy {
            idle_retention_pressure_free_bytes: 0,
            ..valid.clone()
        }
        .validate()
        .is_err());
        assert!(ExecutorRuntimePolicy {
            maximum_queue_depth: 0,
            ..valid.clone()
        }
        .validate()
        .is_err());
        assert!(ExecutorRuntimePolicy {
            free_disk_watermark_bytes: 0,
            ..valid.clone()
        }
        .validate()
        .is_err());
        assert!(ExecutorRuntimePolicy {
            memory_budget_bytes: Some(0),
            ..valid.clone()
        }
        .validate()
        .is_err());
        assert!(ExecutorRuntimePolicy {
            disk_growth_budget_bytes: Some(0),
            ..valid
        }
        .validate()
        .is_err());
    }

    #[test]
    fn cpu_admission_policy_defaults_validate_and_legacy_runtime_policy_decodes() {
        let expected = CpuAdmissionPolicy::default();
        assert_eq!(expected.entry_utilization, 0.90);
        assert_eq!(expected.clear_utilization, 0.75);
        assert_eq!(expected.entry_window_ms, 15_000);
        assert_eq!(expected.clear_window_ms, 10_000);
        assert!(expected.validate().is_ok());

        let mut legacy = serde_json::to_value(ExecutorRuntimePolicy::default()).unwrap();
        legacy.as_object_mut().unwrap().remove("cpuAdmission");
        let decoded: ExecutorRuntimePolicy = serde_json::from_value(legacy).unwrap();
        assert_eq!(decoded.cpu_admission, expected);
    }

    #[test]
    fn cpu_admission_policy_rejects_invalid_bars_and_windows() {
        let valid = CpuAdmissionPolicy::default();
        for invalid in [
            CpuAdmissionPolicy {
                entry_utilization: f64::NAN,
                ..valid
            },
            CpuAdmissionPolicy {
                entry_utilization: 1.01,
                ..valid
            },
            CpuAdmissionPolicy {
                clear_utilization: -0.01,
                ..valid
            },
            CpuAdmissionPolicy {
                clear_utilization: valid.entry_utilization,
                ..valid
            },
            CpuAdmissionPolicy {
                entry_window_ms: 0,
                ..valid
            },
            CpuAdmissionPolicy {
                clear_window_ms: 0,
                ..valid
            },
        ] {
            assert!(invalid.validate().is_err(), "{invalid:?} must be rejected");
        }
    }

    #[test]
    fn cpu_admission_health_and_pressure_evidence_round_trip() {
        let health = CpuAdmissionHealth {
            state: CpuAdmissionState::Pressured,
            utilization: Some(0.94),
            state_since_unix_ms: Some(1_000),
            measured_at_unix_ms: Some(2_000),
        };
        let condition = HostPressureCondition::CpuUtilization {
            utilization: 0.94,
            entry_utilization: 0.90,
            clear_utilization: 0.75,
            state_since_unix_ms: 1_000,
            measured_at_unix_ms: 2_000,
        };

        let health_value = serde_json::to_value(&health).unwrap();
        assert_eq!(health_value["state"], "pressured");
        assert_eq!(
            serde_json::from_value::<CpuAdmissionHealth>(health_value).unwrap(),
            health
        );
        let condition_value = serde_json::to_value(&condition).unwrap();
        assert_eq!(condition_value["cpuUtilization"]["utilization"], 0.94);
        assert_eq!(
            serde_json::from_value::<HostPressureCondition>(condition_value).unwrap(),
            condition
        );
    }

    #[test]
    fn legacy_health_defaults_new_operator_fields() {
        let mut value = serde_json::to_value(ExecutorSubstrateReport::default()).unwrap();
        value.as_object_mut().unwrap().remove("appliedPolicy");
        value.as_object_mut().unwrap().remove("drainMode");
        let report: ExecutorSubstrateReport = serde_json::from_value(value).unwrap();
        assert_eq!(report.applied_policy, ExecutorRuntimePolicy::default());
        assert!(!report.drain_mode);

        let mut host_value = serde_json::to_value(HostHealth::default()).unwrap();
        host_value.as_object_mut().unwrap().remove("cpuAdmission");
        let host: HostHealth = serde_json::from_value(host_value).unwrap();
        assert_eq!(host.cpu_admission, CpuAdmissionHealth::default());
    }

    fn sample_outcome() -> CellOutcome {
        CellOutcome::Completed {
            request_id: "r".into(),
            attempt_id: "a".into(),
            exit_code: Some(1),
            output: "failed".into(),
            timed_out: false,
            metadata: CellExecutionMeta {
                warmth: None,
                load_context: None,
                executor_id: "e".into(),
                executor_device_id: "d".into(),
                executor_connection_generation: 1,
                cell_id: "s".into(),
                cell_epoch: 2,
                started_at_unix_ms: 3,
                finished_at_unix_ms: 4,
                duration_ms: None,
                peak_rss_bytes: None,
                peak_physical_footprint_bytes: None,
                cpu: None,
                allocation: None,
                disk_delta_bytes: None,
                measurement_quality: None,
                environment_fingerprint: String::new(),
                verdict_platform: None,
                verdict_arch: None,
                toolchain_fingerprint: None,
                verdict_environment_hash: None,
                sandbox: None,
                containment: None,
            },
            mutation_delta: Some(Box::new(MutationDelta {
                base_commit: "b".into(),
                delta_commit: "d".into(),
                upload_receipt: None,
            })),
            sandbox_denials: Vec::new(),
            tracked_modifications: None,
        }
    }

    #[test]
    fn lifetime_pipe_runtime_shape_round_trips_with_stream_tags() {
        let process = ResidentProcessSpec {
            runtime_packages: Vec::new(),
            program: "bun".into(),
            args: vec!["main.ts".into()],
            launch_command: None,
            cwd: "package".into(),
            cwd_root: ResidentProcessCwdRoot::ResidencyScratch,
            env: Vec::new(),
            sandbox_mode: ProcessSandboxMode::Confined,
            sandbox_policy: None,
            runtime_assets: vec![ResidentRuntimeAsset {
                path: "package/main.ts".into(),
                data: b"console.log('ok')".to_vec(),
            }],
            io: ResidentProcessIoMode::Pipe,
        };
        let value = serde_json::to_value(&process).unwrap();
        assert_eq!(value["cwdRoot"], "residencyScratch");
        assert_eq!(value["io"]["mode"], "pipe");
        assert_eq!(value["runtimeAssets"][0]["path"], "package/main.ts");
        assert_eq!(
            serde_json::from_value::<ResidentProcessSpec>(value).unwrap(),
            process
        );
        assert_eq!(
            serde_json::to_value(ResidencyHolder::Workflow {
                run_id: "run".into()
            })
            .unwrap(),
            serde_json::json!({ "kind": "workflow", "runId": "run" })
        );
        let output = ResidentProcessEventKind::Output {
            sequence: 7,
            stream: ResidentProcessStream::Stderr,
            data: b"diagnostic".to_vec(),
        };
        let output_value = serde_json::to_value(output).unwrap();
        assert_eq!(output_value["event"], "output");
        assert_eq!(output_value["stream"], "stderr");
    }

    #[test]
    fn legacy_cell_execution_meta_decodes_without_execution_facts() {
        let meta: CellExecutionMeta = serde_json::from_value(serde_json::json!({
            "slotId": "cell-1",
            "cellEpoch": 2,
            "startedAtUnixMs": 3,
            "finishedAtUnixMs": 4
        }))
        .unwrap();

        assert_eq!(meta.cell_id, "cell-1");
        assert_eq!(meta.verdict_platform, None);
        assert_eq!(meta.verdict_arch, None);
        assert_eq!(meta.toolchain_fingerprint, None);
        assert_eq!(meta.verdict_environment_hash, None);
    }

    #[test]
    fn command_batch_ownership_round_trips_physical_and_parent_identity() {
        let ownership = CommandBatchOwnership {
            executor_id: "executor-1".into(),
            cell_id: "cell-1".into(),
            request_id: "request-1".into(),
            attempt_id: "lease-attempt-1".into(),
            batch_launch_nonce: "batch-nonce".into(),
            launched_at_unix_ms: 10,
            items: vec![CommandItemOwnership {
                item_index: 2,
                stream_id: "stderr".into(),
                attempts: vec![CommandAttemptOwnership {
                    attempt_index: 1,
                    state: CommandAttemptLaunchState::Running,
                    launch_nonce: "attempt-nonce".into(),
                    launched_at_unix_ms: 11,
                    identity_error: None,
                    processes: vec![CommandPhysicalProcessIdentity {
                        pid: 42,
                        process_group_id: Some(42),
                        session_id: Some(7),
                        start_token: "kernel-start".into(),
                        launch_nonce: "attempt-nonce".into(),
                        observed_at_unix_ms: 12,
                        parent: CommandProcessParentEvidence::Descendant {
                            parent_pid: 41,
                            parent_start_token: "parent-start".into(),
                        },
                    }],
                }],
            }],
        };
        let value = serde_json::to_value(&ownership).unwrap();
        assert_eq!(value["items"][0]["attempts"][0]["state"], "running");
        assert_eq!(
            value["items"][0]["attempts"][0]["processes"][0]["parent"]["kind"],
            "descendant"
        );
        assert_eq!(
            serde_json::from_value::<CommandBatchOwnership>(value).unwrap(),
            ownership
        );
    }

    #[test]
    fn legacy_command_occupancy_decodes_as_unverified_without_signal_authority() {
        let occupancy: CellOccupancy = serde_json::from_value(serde_json::json!({
            "command": {
                "requestId": "request-1",
                "attemptId": "attempt-1",
                "command": "cargo test",
                "commandClass": "cargoTest",
                "priority": "reviewCheck",
                "requestingJobId": null,
                "queuedAtUnixMs": 0,
                "startedAtUnixMs": 1
            }
        }))
        .unwrap();
        assert!(occupancy.command.is_some());
        assert_eq!(occupancy.command_ownership, None);
        assert!(!occupancy.is_empty());
    }

    #[test]
    fn legacy_health_defaults_command_process_health_separately() {
        let mut value = serde_json::to_value(ExecutorSubstrateReport::default()).unwrap();
        value.as_object_mut().unwrap().remove("commandProcesses");
        let report: ExecutorSubstrateReport = serde_json::from_value(value).unwrap();
        assert_eq!(report.command_processes, CommandProcessHealth::default());
        assert_eq!(report.resident_processes, ResidentProcessHealth::default());
    }

    fn edge_fingerprint(edge: EdgeId) -> Fingerprint {
        Fingerprint {
            algorithm: FingerprintAlgorithm::Sha256,
            digest: "digest".into(),
            inputs: edge.definition().invalidation_inputs.to_vec(),
        }
    }

    /// A machine whose advertised identity agrees with every observation it
    /// carries, so a test that wants a refusal has to introduce the
    /// disagreement it is testing rather than inherit one.
    fn matrix(observations: Vec<EdgeObservation>) -> ContainmentObservations {
        ContainmentObservations {
            executor_build_id: "build".into(),
            boot_generation: "boot".into(),
            platform: Some(RemotePlatform::DarwinArm64),
            current_fingerprints: observations
                .iter()
                .map(|observation| EdgeFingerprint {
                    edge: observation.edge,
                    fingerprint: Some(edge_fingerprint(observation.edge)),
                })
                .collect(),
            observations,
        }
    }

    fn refused(edge: EdgeId) -> EdgeObservation {
        EdgeObservation {
            edge,
            disposition: EdgeDisposition::Refused {
                scope: positive_scope(edge),
                observed: ObservedEffect {
                    effect: EffectClass::Write,
                    positive_control_passed: true,
                    detail: "in-bounds write succeeded, out-of-bounds write refused".into(),
                },
            },
            observed_at_unix_ms: 7,
        }
    }

    fn requires(edge: EdgeId, maximum_disposition: MaximumDisposition) -> WorkloadEnvelope {
        WorkloadEnvelope {
            process_class: edge.definition().key.process_class,
            edges: vec![EdgeRequirement {
                edge,
                maximum_disposition,
            }],
        }
    }

    fn positive_scope(edge: EdgeId) -> PositiveScope {
        let definition = edge.definition();
        PositiveScope {
            executor_build_id: "build".into(),
            boot_generation: "boot".into(),
            platform: RemotePlatform::DarwinArm64,
            process_class: definition.key.process_class,
            carrier: definition.key.carrier,
            policy_mode: definition.key.policy_mode,
            probe_version: ProbeVersion::V1,
            claim_revision: definition.claim_revision,
            invalidation_fingerprint: Fingerprint {
                algorithm: FingerprintAlgorithm::Sha256,
                digest: "digest".into(),
                inputs: definition.invalidation_inputs.to_vec(),
            },
        }
    }

    #[test]
    fn containment_edge_table_is_exhaustive_and_structurally_indexed() {
        assert_eq!(CONTAINMENT_EDGE_IDS.len(), EdgeId::COUNT);
        for (index, edge) in CONTAINMENT_EDGE_IDS.into_iter().enumerate() {
            assert_eq!(edge as usize, index);
            assert_eq!(edge.definition().id, edge);
            assert_eq!(containment_edge_definition(edge), edge.definition());
        }
        let markdown = containment_inventory_markdown();
        assert!(markdown.contains("BatchSpawnFilesystem"));
        assert!(markdown.contains("BuildServiceStateDir"));
    }

    /// Confirmed shipped crossings are pinned by direction, not just by name.
    ///
    /// This exists because of a real omission: the inventory had exactly one
    /// `ProcessEnv` row, `BuildServiceClientEnv`, which runs `Cell → External`
    /// and represents a guest reaching a service. Wholesale inheritance of the
    /// runner's environment into guest work runs the other way, `Runner → Cell`,
    /// and was simply absent. An absent edge is worse than an unprobed one: an
    /// `Unmeasured` row is a refusal an envelope can name and evaluate, while a
    /// missing row has no `EdgeId` at all and cannot be required, refused, or
    /// even discussed. A category that reads plausibly from its name while
    /// pointing the wrong way is exactly how the gap survived, so the assertion
    /// is on the direction.
    #[test]
    fn confirmed_ingress_crossings_are_present_and_pointed_the_right_way() {
        for class in [
            ProcessClass::BatchSpawn,
            ProcessClass::PipedResident,
            ProcessClass::PtyResident,
            ProcessClass::Repl,
            ProcessClass::Workflow,
            ProcessClass::DevInstance,
        ] {
            let inheritance = CONTAINMENT_EDGE_IDS
                .into_iter()
                .map(EdgeId::definition)
                .find(|definition| {
                    definition.key.carrier == Carrier::ProcessEnv
                        && definition.key.source == TrustDomain::Runner
                        && definition.key.destination == TrustDomain::Cell
                        && definition.key.process_class == class
                })
                .unwrap_or_else(|| {
                    panic!("{class:?} has no runner-to-cell environment inheritance edge")
                });
            assert_eq!(inheritance.key.direction, Direction::DestToGuest);
            assert!(
                inheritance
                    .invalidation_inputs
                    .contains(&InvalidationInput::EnvironmentAssemblyPolicy),
                "{class:?} inheritance must invalidate on what the runner assembles"
            );
        }

        // The other ingress-direction crossings this landing confirmed.
        let ingress = EdgeId::DevInstanceExternalIngress.definition();
        assert_eq!(ingress.key.source, TrustDomain::ExternalNetwork);
        assert_eq!(ingress.key.destination, TrustDomain::Cell);
        let desktop = EdgeId::DesktopAutomationControl.definition();
        assert_eq!(desktop.key.destination, TrustDomain::ExecutorHost);
    }

    /// Every filesystem seam is represented once per runtime policy mode, so a
    /// measurement of one mode can never be the evidence for another.
    #[test]
    fn every_filesystem_seam_is_represented_once_per_policy_mode() {
        for class in [
            ProcessClass::BatchSpawn,
            ProcessClass::PipedResident,
            ProcessClass::PtyResident,
            ProcessClass::Repl,
            ProcessClass::Workflow,
            ProcessClass::DevInstance,
        ] {
            for mode in [
                PolicyMode::Confined,
                PolicyMode::ReadOnlyCheckout,
                PolicyMode::Unconfined,
            ] {
                let found = CONTAINMENT_EDGE_IDS
                    .into_iter()
                    .map(EdgeId::definition)
                    .filter(|definition| {
                        definition.key.carrier == Carrier::Filesystem
                            && definition.key.destination == TrustDomain::ExecutorHost
                            && definition.key.process_class == class
                            && definition.key.policy_mode == mode
                    })
                    .count();
                assert_eq!(
                    found, 1,
                    "{class:?} under {mode:?} must be exactly one edge"
                );
            }
        }
    }

    #[test]
    fn containment_wire_rejects_unknown_closed_identifiers() {
        for unknown in [
            serde_json::from_str::<RemotePlatform>("\"plan9-riscv\"").unwrap_err(),
            serde_json::from_str::<EdgeId>("\"unknownEdge\"").unwrap_err(),
            serde_json::from_str::<EnvelopeId>("\"genericMediator\"").unwrap_err(),
            serde_json::from_str::<ProbeVersion>("\"v2\"").unwrap_err(),
        ] {
            assert!(unknown.to_string().contains("unknown variant"), "{unknown}");
        }
    }

    #[test]
    fn positive_observations_require_exact_scope_and_complete_fingerprint() {
        let edge = EdgeId::BatchSpawnFilesystemConfined;
        let valid = refused(edge);
        assert!(matrix(vec![valid.clone()]).validate().is_ok());

        let mut wrong_seam = valid.clone();
        if let EdgeDisposition::Refused { scope, .. } = &mut wrong_seam.disposition {
            scope.process_class = ProcessClass::PtyResident;
        }
        assert!(matrix(vec![wrong_seam])
            .validate()
            .unwrap_err()
            .contains("another seam"));

        let mut partial = valid;
        if let EdgeDisposition::Refused { scope, .. } = &mut partial.disposition {
            scope.invalidation_fingerprint.inputs.pop();
        }
        assert!(matrix(vec![partial])
            .validate()
            .unwrap_err()
            .contains("incomplete invalidation fingerprint"));
    }

    /// The evidence rules are only real if each one refuses on its own. These
    /// look alike deliberately: each removes exactly one property of a
    /// measurement that is otherwise sufficient, so a regression that stops
    /// checking any single property fails here rather than silently widening
    /// what qualifies.
    #[test]
    fn a_positive_disposition_refuses_once_any_binding_property_is_broken() {
        let edge = EdgeId::BatchSpawnFilesystemConfined;
        let wanted = requires(edge, MaximumDisposition::Refused);

        // The control: a coherent, current, positively-controlled measurement.
        assert!(matrix(vec![refused(edge)]).satisfies(&wanted).is_ok());

        let deficiency = |mutate: &dyn Fn(&mut ContainmentObservations)| {
            let mut observations = matrix(vec![refused(edge)]);
            mutate(&mut observations);
            observations.satisfies(&wanted).unwrap_err().deficiency
        };

        assert_eq!(
            deficiency(&|observations| observations.observations.clear()),
            ContainmentDeficiency::NotObserved
        );
        assert_eq!(
            deficiency(&|observations| observations.executor_build_id = "rebuilt".into()),
            ContainmentDeficiency::StaleBuild
        );
        assert_eq!(
            deficiency(&|observations| observations.boot_generation = "rebooted".into()),
            ContainmentDeficiency::PriorBoot
        );
        assert_eq!(
            deficiency(&|observations| observations.platform = Some(RemotePlatform::LinuxArm64)),
            ContainmentDeficiency::PlatformMismatch
        );
        assert_eq!(
            deficiency(&|observations| observations.current_fingerprints[0].fingerprint = None),
            ContainmentDeficiency::FingerprintUncomputable
        );
        assert_eq!(
            deficiency(&|observations| {
                observations.current_fingerprints.clear();
            }),
            ContainmentDeficiency::FingerprintUncomputable
        );
        assert_eq!(
            deficiency(&|observations| {
                if let Some(fingerprint) = &mut observations.current_fingerprints[0].fingerprint {
                    fingerprint.digest = "the profile changed".into();
                }
            }),
            ContainmentDeficiency::FingerprintChanged
        );
        assert_eq!(
            deficiency(&|observations| {
                if let EdgeDisposition::Refused { observed, .. } =
                    &mut observations.observations[0].disposition
                {
                    observed.positive_control_passed = false;
                }
            }),
            ContainmentDeficiency::PositiveControlFailed
        );
    }

    /// A machine measured at one seam has said nothing about another, so a
    /// terminal workload cannot ride a batch measurement. This is the mechanical
    /// reason no machine-wide "filesystem confined" claim is representable.
    #[test]
    fn a_measured_batch_seam_is_not_evidence_about_a_terminal() {
        let observations = matrix(vec![
            refused(EdgeId::BatchSpawnFilesystemConfined),
            EdgeObservation {
                edge: EdgeId::PtyResidentFilesystemConfined,
                disposition: EdgeDisposition::Unmeasured {
                    reason: "no probe attempts the pseudoterminal seam yet".into(),
                },
                observed_at_unix_ms: 7,
            },
        ]);

        assert!(observations
            .satisfies(&requires(
                EdgeId::BatchSpawnFilesystemConfined,
                MaximumDisposition::Refused
            ))
            .is_ok());

        let refusal = observations
            .satisfies(&requires(
                EdgeId::PtyResidentFilesystemConfined,
                MaximumDisposition::Refused,
            ))
            .unwrap_err();
        assert_eq!(refusal.edge, EdgeId::PtyResidentFilesystemConfined);
        assert!(refusal.describe().contains("PtyResidentFilesystem"));
        assert!(refusal.describe().contains("no probe attempts"));
    }

    /// An envelope cannot be satisfied by asserting it. Ambient is what the
    /// egress edges honestly are today, and a request for anything stronger is
    /// refused rather than rounded down to the label.
    #[test]
    fn an_ambient_edge_cannot_satisfy_a_stronger_request() {
        let edge = EdgeId::BatchSpawnEgress;
        let observations = matrix(vec![EdgeObservation {
            edge,
            disposition: EdgeDisposition::Ambient {
                observed: ObservedEffect {
                    effect: EffectClass::Invoke,
                    positive_control_passed: true,
                    detail: "the probe reached an external host".into(),
                },
                scope: None,
            },
            observed_at_unix_ms: 7,
        }]);

        assert!(observations
            .satisfies(&requires(edge, MaximumDisposition::Ambient))
            .is_ok());
        let refusal = observations
            .satisfies(&requires(edge, MaximumDisposition::Refused))
            .unwrap_err();
        assert!(matches!(
            refusal.deficiency,
            ContainmentDeficiency::TooPermissive(_)
        ));
        assert!(refusal.describe().contains("more permissive"));
    }

    /// Evidence has to be about the kind of run it is authorizing.
    ///
    /// Splitting the filesystem edges by policy mode only closes the bypass if
    /// something ties the requested edge to the mode the spawn actually
    /// applies. Without that tie, a perfectly current and satisfiable
    /// `Confined` requirement could ride an unconfined batch: every check would
    /// pass, because each was asking about the confined edge while the process
    /// ran under no policy at all. The evidence would be true and the
    /// conclusion false, which is the most dangerous shape a security check can
    /// take.
    #[test]
    fn an_envelope_only_authorizes_the_mode_it_is_evidence_about() {
        let modes = [
            (PolicyMode::Confined, EdgeId::BatchSpawnFilesystemConfined),
            (
                PolicyMode::ReadOnlyCheckout,
                EdgeId::BatchSpawnFilesystemReadOnlyCheckout,
            ),
            (
                PolicyMode::Unconfined,
                EdgeId::BatchSpawnFilesystemUnconfined,
            ),
        ];
        for (required_mode, edge) in modes {
            let envelope = requires(edge, MaximumDisposition::Refused);
            for (actual_mode, _) in modes {
                let verdict = envelope.matches_policy_mode(actual_mode);
                if actual_mode == required_mode {
                    assert!(verdict.is_ok(), "{edge:?} must authorize {actual_mode:?}");
                } else {
                    let refusal = verdict.expect_err(&format!(
                        "{edge:?} must not authorize a {actual_mode:?} run"
                    ));
                    assert!(refusal.contains(&format!("{required_mode:?}")), "{refusal}");
                    assert!(refusal.contains(&format!("{actual_mode:?}")), "{refusal}");
                }
            }
        }

        // Edges that do not run under a filesystem policy are unaffected, so
        // the relation does not start refusing network or relay requirements
        // for a reason that does not apply to them.
        let egress = requires(EdgeId::BatchSpawnEgress, MaximumDisposition::Ambient);
        for mode in [
            PolicyMode::Confined,
            PolicyMode::ReadOnlyCheckout,
            PolicyMode::Unconfined,
        ] {
            assert!(egress.matches_policy_mode(mode).is_ok());
        }
    }

    #[test]
    fn envelopes_are_edge_owned_and_selector_validation_reaches_them() {
        let invalid = WorkloadEnvelope {
            process_class: ProcessClass::BatchSpawn,
            edges: vec![EdgeRequirement {
                edge: EdgeId::BatchSpawnEgress,
                maximum_disposition: MaximumDisposition::Constrained {
                    envelope: EnvelopeId::CellFilesystem,
                },
            }],
        };
        assert!(invalid.validate().unwrap_err().contains("not declared"));

        let valid = WorkloadEnvelope {
            process_class: ProcessClass::BatchSpawn,
            edges: vec![EdgeRequirement {
                edge: EdgeId::BatchSpawnFilesystemConfined,
                maximum_disposition: MaximumDisposition::Constrained {
                    envelope: EnvelopeId::CellFilesystem,
                },
            }],
        };
        let selector = ExecutorSelector {
            containment: Some(valid),
            ..ExecutorSelector::default()
        };
        assert!(!selector.is_empty());
        assert!(selector.validate().is_ok());
        assert!(selector.describe().contains("containment envelope"));
        assert!(serde_json::to_value(selector).unwrap()["containment"]["processClass"].is_string());
    }

    #[test]
    fn containment_carriage_distinguishes_absence_from_empty() {
        let older = serde_json::json!({
            "os": "linux",
            "arch": "x86_64",
            "logicalCores": 4,
            "toolchains": [],
            "projectsServed": [],
        });
        let decoded: ExecutorCapabilities = serde_json::from_value(older).unwrap();
        assert_eq!(decoded.containment, None);

        // A machine that reports an empty matrix has said something different
        // from one that reports nothing: it names its build, boot, and platform
        // and states that it measured no edge. Both refuse, but an operator
        // reading them is sent to different places, so the wire keeps them
        // distinguishable.
        let mut value = serde_json::to_value(decoded).unwrap();
        value["containment"] = serde_json::json!({
            "executorBuildId": "build",
            "bootGeneration": "boot",
            "platform": "linux-arm64",
            "currentFingerprints": [],
            "observations": [],
        });
        let reported: ExecutorCapabilities = serde_json::from_value(value).unwrap();
        let reported = reported
            .containment
            .expect("an empty matrix is still a matrix");
        assert_ne!(reported, ContainmentObservations::default());
        assert!(reported.observations.is_empty());
        assert_eq!(
            reported
                .satisfies(&requires(
                    EdgeId::BatchSpawnFilesystemConfined,
                    MaximumDisposition::Refused
                ))
                .unwrap_err()
                .deficiency,
            ContainmentDeficiency::NotObserved
        );
    }
}
