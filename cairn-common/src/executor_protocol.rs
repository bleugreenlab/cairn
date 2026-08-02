//! Versioned wire contract between the runner and enrolled executors.
//!
//! Build-cell requests are immutable. Cancellation is deliberately represented
//! as a separate control message so dropping a runner-side waiter cannot mutate
//! or ambiguously replay an admitted request.

use crate::protocol::{CallbackRequest, CallbackResponse};
use serde::{Deserialize, Serialize};
use std::future::Future;

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
pub const EXECUTOR_PROTOCOL_VERSION: u32 = 33;

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
// were.
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
/// the `externalBin` binaries (`cairn-cmd`, `cairn-executor`, `cairn-runner`) that
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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
}
pub const MANAGED_OBJECT_REQUEST_TIMEOUT_SECONDS: u64 = 60;
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
}

impl ExecutorSelector {
    pub fn is_empty(&self) -> bool {
        self.name.is_none() && self.os.is_none() && self.required_toolchains.is_empty()
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
                "an executor selector must state at least one of name, os, or requiredToolchains"
                    .into(),
            );
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ExecutorCapabilities {
    pub os: String,
    pub arch: String,
    pub logical_cores: usize,
    #[serde(default)]
    pub toolchains: Vec<String>,
    #[serde(default)]
    pub projects_served: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk_budget_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_budget_bytes: Option<u64>,
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
}

impl ExecutorCapabilities {
    /// The machine-wide concurrency budget this executor admits against.
    pub const fn admission_concurrency_budget(&self) -> u32 {
        admission_concurrency_budget(self.logical_cores)
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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "mode", rename_all = "camelCase")]
pub enum ExecutorEnrollmentIdentity {
    Colocated,
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
    pub timeout_ms: u32,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ObjectChannelCredential {
    pub base_url: String,
    pub bearer_token: String,
    pub expires_at_unix_ms: u64,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum HostPressureCondition {
    MemoryAvailable {
        available_bytes: u64,
        floor_bytes: u64,
    },
    DiskFree {
        free_bytes: u64,
        floor_bytes: u64,
    },
    ResidentOccupancy {
        process_count: usize,
        reservation: ResourceReservation,
    },
}

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
    ProtocolAttaching,
    InitialStorageSweep,
    StorageAccounting,
    DispatchPreparing,
    SlotAdoption,
    CapacityBusy,
    ExecutionRunning,
    ConnectedStalled,
    Draining,
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
    Preparation,
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
    Provisioning,
    Idle,
    Queued,
    Running,
    AwaitingReclaim,
    Releasing,
    Recovering,
    Retired,
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
    pub program: String,
    #[serde(default)]
    pub args: Vec<String>,
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
    DevInstance,
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
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub processes: std::collections::BTreeMap<String, ResidentProcess>,
}

impl CellOccupancy {
    pub fn is_empty(&self) -> bool {
        self.command.is_none() && self.processes.is_empty()
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
            let Some(reservation) = process.reservation.as_ref() else {
                continue;
            };
            total.memory_bytes = total.memory_bytes.saturating_add(reservation.memory_bytes);
            total.disk_growth_bytes = total
                .disk_growth_bytes
                .saturating_add(reservation.disk_growth_bytes);
            total.concurrency_units = total
                .concurrency_units
                .saturating_add(reservation.concurrency_units);
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
    pub last_used_unix_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_affinity_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preparation_fingerprint: Option<String>,
    /// Who holds this cell. Absent for a free cell and for a transient batch
    /// that took a cell of its own.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub residency: Option<CellResidency>,
    /// What is running in this cell.
    #[serde(default, skip_serializing_if = "CellOccupancy::is_empty")]
    pub occupancy: CellOccupancy,
}

impl PersistentCellState {
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
    pub queued_at_unix_ms: u64,
    #[serde(default)]
    pub resource_reservation: ResourceReservation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub learned_estimate: Option<LearnedResourceEstimate>,
    #[serde(default = "default_subscriber_count")]
    pub subscriber_count: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub substrate_hold: Option<ExecutorSubstrateEvidence>,
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
    pub queued_requests: Vec<QueuedCellRequest>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub executing_requests: Vec<ExecutingCellRequest>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub recent_completions: Vec<CellCompletion>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resident_occupancy: Option<ResidentOccupancyEvidence>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub substrate_state: Option<ExecutorSubstrateEvidence>,
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
pub const SUBSTRATE_HEALTH_SCHEMA_VERSION: u32 = 5;

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
    Ok,
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
            Self::DiskAccounting | Self::ProcessResident | Self::ProcessPhysicalFootprint => false,
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
    pub logical_cores: Option<usize>,
    pub tokio_worker_count: Option<usize>,
    pub tokio_alive_tasks: Option<usize>,
    pub tokio_global_queue_depth: Option<usize>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DiskCategoryAccounting {
    pub managed_objects_bytes: u64,
    #[serde(rename = "liveSlotsBytes")]
    pub live_cells_bytes: u64,
    pub warm_caches_bytes: u64,
    pub quarantines_bytes: u64,
    pub temporary_other_bytes: u64,
}

/// Storage governance: the budget, the derived pressure verdict, and the
/// janitor's state.
///
/// The bytes themselves are measurements and live on
/// [`MachineTelemetry::volume`] and [`MachineTelemetry::disk_accounting`], where
/// each carries its own collection time. `status` is derived from the volume
/// reading, so `Unknown` here means that reading is a gap.
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
    pub cleanup_skipped_entries: Option<usize>,
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
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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
    /// Free bytes at or below which retention collapses from ceiling to floor.
    #[serde(default = "default_idle_retention_pressure_free_bytes")]
    pub idle_retention_pressure_free_bytes: u64,
    /// How long a quarantined build cell is held for forensics before the
    /// storage sweep reclaims it.
    #[serde(default = "default_quarantine_forensic_window_ms")]
    pub quarantine_forensic_window_ms: u64,
}

/// Idle cells an executor keeps for one project no matter how little disk is
/// free. A warm cell is a populated build cache, and discarding the last one
/// costs that project's next command a full cold build — the cost this floor
/// exists to refuse. Pressure eviction stops here; the storage sweep reclaims
/// quarantines and slot targets to find bytes beyond this point.
pub const IDLE_RETENTION_FLOOR_PER_PROJECT: usize = 1;

/// Idle cells an executor keeps for one project while disk is not pressured.
///
/// This was 1 until CAIRN-3188, inherited from the single-worktree era. Under
/// concurrent-children load a cell falls idle for moments between batches and
/// check waves, so a limit of 1 retired nearly every cell the instant it became
/// reusable: 2026-07-26 measured 238 warm-cell retirements in a day against 27
/// the day before, and build times went from roughly ten minutes to thirty.
/// Warm caches are the most valuable thing on a build volume, so retention is
/// generous by default and only measured disk pressure takes it back. The
/// ceiling survives as a runaway guard: a project that cycles cells faster than
/// the storage sweep reclaims them would otherwise grow inventory without
/// bound.
pub const IDLE_RETENTION_CEILING_PER_PROJECT: usize = 16;

/// Free bytes on the executor volume at or below which idle retention collapses
/// to its floor and the coldest cells above it are evicted.
///
/// Deliberately far above `free_disk_watermark_bytes` (2 GiB by default), which
/// is where admission starts refusing work: warm inventory should be handed
/// back while there is still room to run, not once the volume has already
/// become an admission problem. A cell's checkout plus its build caches runs to
/// a few gibibytes, so this leaves several cells of headroom below the
/// threshold. Raising it evicts sooner and colder; lowering it keeps caches
/// longer at the risk of meeting the admission watermark first.
pub const IDLE_RETENTION_PRESSURE_FREE_BYTES: u64 = 64 * 1024 * 1024 * 1024;

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
/// The window is the only thing that authorizes deleting a quarantine. Nothing
/// — aggregate size, disk pressure, an admission that would otherwise be
/// refused — shortens it, because a hold that yields when the volume gets tight
/// is not a hold, and the incidents worth keeping evidence for are exactly the
/// ones that fill a disk. Headroom comes from expired quarantines and then from
/// warm build caches; when neither has anything left, admission refuses and
/// names the quarantines holding the bytes.
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

const fn default_idle_retention_pressure_free_bytes() -> u64 {
    IDLE_RETENTION_PRESSURE_FREE_BYTES
}

impl Default for ExecutorRuntimePolicy {
    fn default() -> Self {
        Self {
            memory_budget_bytes: None,
            disk_growth_budget_bytes: None,
            free_disk_watermark_bytes: 2 * 1024 * 1024 * 1024,
            concurrency_units: u32::MAX,
            maximum_queue_depth: 512,
            idle_retention_floor_per_project: default_idle_retention_floor_per_project(),
            idle_retention_ceiling_per_project: default_idle_retention_ceiling_per_project(),
            idle_retention_pressure_free_bytes: default_idle_retention_pressure_free_bytes(),
            quarantine_forensic_window_ms: default_quarantine_forensic_window_ms(),
        }
    }
}

impl ExecutorRuntimePolicy {
    pub fn validate(&self) -> Result<(), String> {
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
        if self.idle_retention_pressure_free_bytes == 0 {
            return Err(
                "executor idle-retention disk-pressure threshold must be greater than zero".into(),
            );
        }
        if self.free_disk_watermark_bytes == 0 {
            return Err("executor free-disk watermark must be greater than zero".into());
        }
        if self.quarantine_forensic_window_ms == 0 {
            return Err("executor quarantine forensic window must be greater than zero".into());
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

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ExecutorSubstrateReport {
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
    pub applied_policy: ExecutorRuntimePolicy,
    #[serde(default)]
    pub drain_mode: bool,
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
    #[serde(default)]
    pub drain_mode: bool,
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
    /// This executor's own cells, queued work, and running work.
    pub occupancy: FleetSnapshot,
    /// The one instant every age on this record is derived from.
    pub captured_at_unix_ms: u64,
}

/// Why a machine this runner is enrolled with is not attached right now.
///
/// The three states are deliberately not one alarm. [`Self::Unreachable`] is the
/// ordinary case on a fleet whose runner is a laptop: the host did not answer,
/// which is what happens when a machine is off or this one has left the network
/// its machines live on. [`Self::AttachFailed`] is the runner having *reached*
/// the host and failed to bring an executor up on it, which only a person can
/// resolve. [`Self::Pending`] is the runner not having tried yet since it
/// started — an absence of evidence, not a verdict.
///
/// Collapsing them would produce a fleet view that reads as broken every time a
/// laptop leaves a desk, and a view nobody believes is how three machines stayed
/// gone for hours without anyone noticing (CAIRN-3356).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum RemoteLinkState {
    /// The host did not answer. Ordinary, and not on its own a fault.
    Unreachable,
    /// The host answered and the executor could not be brought up on it.
    AttachFailed,
    /// No attempt has completed since this runner started.
    Pending,
}

/// The runner's most recent attempt on a machine, and what stopped it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RemoteAttachAttempt {
    pub attempted_at_unix_ms: u64,
    /// Why the attempt did not produce an attached executor, in the words the
    /// runner would have logged. Already fit to show a person.
    pub reason: String,
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
    /// The public address of this machine, and the only name a placement
    /// request needs. It reads identically before and after the machine
    /// attaches.
    pub name: String,
    pub os: String,
    pub arch: String,
    pub link: RemoteLinkState,
    /// `None` only while [`RemoteLinkState::Pending`] holds.
    pub last_attempt: Option<RemoteAttachAttempt>,
    /// When this machine's link was last up, or `None` if it has not attached
    /// since this runner started.
    pub last_seen_unix_ms: Option<u64>,
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
    pub outcome: PlacementOutcome,
    /// Every machine that was considered and passed over, with the reason.
    #[serde(default)]
    pub rejected: Vec<PlacementRejection>,
}

impl PlacementDecision {
    /// Whether this decision names the given executor at all — as the machine
    /// that won it, or as one that was passed over.
    pub fn mentions_executor(&self, executor_id: &str) -> bool {
        match &self.outcome {
            PlacementOutcome::Selected(selection) if selection.executor_id == executor_id => true,
            _ => self
                .rejected
                .iter()
                .any(|rejection| rejection.executor_id == executor_id),
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
    /// The public name, which is what a follow-up request would address.
    pub executor_name: String,
    pub executor_id: String,
    pub colocated: bool,
    pub reason: PlacementReason,
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
    /// Won a measured ranking against other usable machines.
    MeasuredIdle,
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
            Self::MeasuredIdle => "measuredIdle",
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
}

impl ReservationFallback {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NoCommandIdentity => "noCommandIdentity",
            Self::NoProfileRecorded => "noProfileRecorded",
            Self::ProfileLookupFailed => "profileLookupFailed",
            Self::BelowConfidenceFloor => "belowConfidenceFloor",
            Self::CallerDeclared => "callerDeclared",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PlacementRejection {
    pub executor_name: String,
    pub executor_id: String,
    pub reason: PlacementRejectionReason,
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
    /// Usable, measured, and simply beaten by the machine that won.
    OutrankedBy { executor_name: String },
}

impl PlacementRejectionReason {
    /// A single line an operator can act on, for the readable surfaces.
    pub fn describe(&self) -> String {
        match self {
            Self::ConnectionClosed => "connection closed".into(),
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
            Self::OutrankedBy { executor_name } => format!("outranked by {executor_name}"),
        }
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
    #[serde(rename = "materializedCount")]
    pub checked_out_count: usize,
    pub idle_count: usize,
    /// Idle cells this executor will currently keep per project. Derived from
    /// the retention policy and the last free-space measurement, so it moves
    /// between the policy's floor and ceiling as the volume fills and drains.
    #[serde(default)]
    pub idle_retention_budget_per_project: usize,
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
    },
    MaterializeConflict {
        fence: ResidencyFence,
        request: ConflictMaterializationRequest,
    },
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
    pub populate: cairn_worktree::PopulateConfig,
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
pub struct ProcessBatch {
    pub sequential: bool,
    pub stop_on_error: bool,
    pub sandbox_mode: ProcessSandboxMode,
    pub items: Vec<ProcessBatchItem>,
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
    pub timeout_ms: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command_resource_identity: Option<CommandResourceIdentity>,
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
    pub disk_delta_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sandbox_denials: Vec<SandboxDenialEvidence>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tracked_modifications: Option<TrackedModificationEvidence>,
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
    ProtocolIncompatible {
        expected: u32,
        received: u32,
    },
    Configure {
        config: ExecutorConfig,
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
    Submit {
        request: CellRequest,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        batch: Option<ProcessBatch>,
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
        operation: ResidencyOperation,
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
    Shutdown,
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
    fn every_message_variant_round_trips() {
        let request = sample_request();
        let outcome = sample_outcome();
        let snapshot = FleetSnapshot::default();
        let advertisement = sample_advertisement();
        let messages = vec![
            ExecutorMessage::Hello {
                protocol_version: EXECUTOR_PROTOCOL_VERSION,
                advertisement: advertisement.clone(),
                enrollment: ExecutorEnrollmentIdentity::Colocated,
                executor_build_id: Some("executor-build".into()),
            },
            ExecutorMessage::Ready {
                protocol_version: EXECUTOR_PROTOCOL_VERSION,
                identity: advertisement.identity.clone(),
                runner_device_id: "runner".into(),
                generation: 2,
                issued_credential: None,
                object_channel: None,
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
            },
            ExecutorMessage::AdvertisementUpdated { advertisement },
            ExecutorMessage::ProtocolIncompatible {
                expected: 1,
                received: 2,
            },
            ExecutorMessage::Configure {
                config: ExecutorConfig {
                    project_id: "p".into(),
                    project_key: "p".into(),
                    default_timeout_seconds: 30,
                    setup_commands: vec!["bun install".into()],
                    populate: Default::default(),
                    population_source_root: None,
                },
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
            ExecutorMessage::Submit {
                request: request.clone(),
                batch: None,
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
                correlation_id: "lease-request".into(),
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
            ExecutorMessage::Shutdown,
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
            kind: ResidentProcessKind::DevInstance,
            spec: None,
            status,
            reservation: Some(charged.clone()),
        };
        let mut occupancy = CellOccupancy {
            command: None,
            processes: std::collections::BTreeMap::from([(
                "server".to_string(),
                process(ResidentProcessStatus::Running {
                    started_at_unix_ms: 1,
                    process_group_id: None,
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
            outcome: PlacementOutcome::Selected(Box::new(PlacementSelection {
                executor_name: "bglab-ub".into(),
                executor_id: "executor-7b21ce".into(),
                colocated: false,
                reason: PlacementReason::MeasuredIdle,
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
                toolchains: vec!["rust".into()],
                projects_served: vec!["p".into()],
                disk_budget_bytes: Some(10),
                memory_budget_bytes: None,
                toolchain_detection: None,
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

        // And an executor that probed and found nothing is a different fact,
        // which survives the same round trip.
        let probed = ExecutorCapabilities {
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

    /// The public selector carries no opaque identity, and a caller reaching for
    /// the retired keys is refused rather than silently placed anywhere.
    #[test]
    fn the_selector_wire_shape_admits_only_the_public_vocabulary() {
        let selector = ExecutorSelector {
            name: Some("bglab-ub".into()),
            required_toolchains: vec!["rust".into()],
            ..ExecutorSelector::default()
        };
        assert_eq!(
            serde_json::to_value(&selector).unwrap(),
            serde_json::json!({"name": "bglab-ub", "requiredToolchains": ["rust"]})
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
                program: "bun".into(),
                args: vec!["run".into(), "dev".into()],
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
            sample_resident_process(ResidentProcessKind::DevInstance),
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
                processes,
            },
            ..template.clone()
        }
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
            idle_count: 1,
            idle_retention_budget_per_project: 16,
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
            logical_cores: Some(10),
            tokio_worker_count: Some(4),
            tokio_alive_tasks: Some(12),
            tokio_global_queue_depth: Some(0),
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
                },
            ),
            process: ProcessTelemetry {
                resident_bytes: Measurement::measured(43, 7),
                physical_footprint_bytes: Measurement::unavailable(
                    43,
                    MeasurementGap::UnsupportedPlatform,
                ),
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
            sample_resident_process(ResidentProcessKind::DevInstance),
        );
        processes.insert(
            "workflow".to_string(),
            sample_resident_process(ResidentProcessKind::WorkflowRuntime {
                workflow: "release".into(),
            }),
        );
        let cell = PersistentCellState {
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
            lifecycle: PersistentCellLifecycle::Running,
            cell_epoch: 9,
            last_sealed_commit: Some(SEALED_COMMIT.into()),
            last_used_unix_ms: 20,
            last_affinity_key: Some("affinity".into()),
            preparation_fingerprint: Some("fingerprint".into()),
            residency: Some(residency),
            occupancy: CellOccupancy {
                command: Some(command.clone()),
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
                    name: "bglab-mac".into(),
                    os: "macos".into(),
                    arch: "aarch64".into(),
                    link: RemoteLinkState::AttachFailed,
                    last_attempt: Some(RemoteAttachAttempt {
                        attempted_at_unix_ms: 1_785_124_850_000,
                        reason: "executor protocol v28 has no published artifact".into(),
                    }),
                    last_seen_unix_ms: Some(1_785_117_770_000),
                },
                EnrolledRemote {
                    name: "bglab-ub".into(),
                    os: "linux".into(),
                    arch: "x86_64".into(),
                    link: RemoteLinkState::Unreachable,
                    last_attempt: Some(RemoteAttachAttempt {
                        attempted_at_unix_ms: 1_785_124_925_000,
                        reason: "ssh: connect to host bglab-ub port 22: No route to host".into(),
                    }),
                    last_seen_unix_ms: Some(1_785_114_170_000),
                },
                EnrolledRemote {
                    name: "bglab-win".into(),
                    os: "windows".into(),
                    arch: "x86_64".into(),
                    link: RemoteLinkState::Pending,
                    last_attempt: None,
                    last_seen_unix_ms: None,
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
                }],
                executing_requests: vec![ExecutingCellRequest {
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
                    resource_reservation: Some(reservation.clone()),
                    learned_estimate: Some(learned_estimate),
                    actuals: Some(CellExecutionMeta {
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
                        disk_delta_bytes: Some(36),
                        measurement_quality: Some(ExecutionMeasurementQuality {
                            duration: MeasurementQuality::Authoritative,
                            memory: MeasurementQuality::Sampled,
                            disk: MeasurementQuality::Approximate,
                            memory_platform: Some("macos".into()),
                            disk_boundary: "cell".into(),
                        }),
                    }),
                    cached: false,
                    subscriber_count: 1,
                    served_at_unix_ms: 37,
                }],
                resident_occupancy: Some(ResidentOccupancyEvidence {
                    process_count: 4,
                    reservation: reservation.clone(),
                }),
                substrate_state: Some(substrate_state),
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
            "lifecycle": "awaitingReclaim",
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
                name: "bglab-win".into(),
                os: "windows".into(),
                arch: "x86_64".into(),
                link: RemoteLinkState::Pending,
                last_attempt: None,
                last_seen_unix_ms: None,
            }],
            status: SubstrateHealthStatus::Degraded,
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
            },
        };
        assert_eq!(
            windows_shaped.gaps(),
            vec![(
                MachineMeasurement::ProcessPhysicalFootprint,
                MeasurementGap::UnsupportedPlatform
            )],
            "the gap is real and the panel still shows it"
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
            "lifecycle": "running",
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
        };
        assert_eq!(
            serde_json::to_value(operation).unwrap()["operation"],
            "refreshMaterialization"
        );
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
    fn legacy_health_defaults_new_operator_fields() {
        let mut value = serde_json::to_value(ExecutorSubstrateReport::default()).unwrap();
        value.as_object_mut().unwrap().remove("appliedPolicy");
        value.as_object_mut().unwrap().remove("drainMode");
        let report: ExecutorSubstrateReport = serde_json::from_value(value).unwrap();
        assert_eq!(report.applied_policy, ExecutorRuntimePolicy::default());
        assert!(!report.drain_mode);
    }

    fn sample_outcome() -> CellOutcome {
        CellOutcome::Completed {
            request_id: "r".into(),
            attempt_id: "a".into(),
            exit_code: Some(1),
            output: "failed".into(),
            timed_out: false,
            metadata: CellExecutionMeta {
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
                disk_delta_bytes: None,
                measurement_quality: None,
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
}

#[cfg(test)]
mod lifetime_pipe_protocol_tests {
    use super::*;

    #[test]
    fn lifetime_pipe_runtime_shape_round_trips_with_stream_tags() {
        let process = ResidentProcessSpec {
            program: "bun".into(),
            args: vec!["main.ts".into()],
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
}
