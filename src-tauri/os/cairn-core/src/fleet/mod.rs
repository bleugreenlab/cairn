//! Runner-side fleet-placement facade for supervised and enrolled executors.
//!
//! Core owns request construction, settings resolution, result correlation, and
//! the cached UI snapshot. Scheduling, workspaces, processes, cancellation, and
//! mutation sealing exist only in the executor process.

mod assignments;
mod check_adoptions;
pub(crate) use check_adoptions::{CheckConsumerGuard, RETAINED_RESULT_RETRY};
mod coalescing;
pub(crate) mod counters;
mod dispatcher;
mod drain_projection;
pub use drain_projection::AdmissionFenceToken;
pub(crate) mod inference;
pub mod initialization;
pub(crate) mod learned_requirements;
pub(crate) mod lift_drain;
pub mod link_clock;
pub mod load;
pub mod machine_services;
pub mod management;
pub mod models;
pub(crate) mod occupancy;
mod operator_act;
pub(crate) mod output_guard;
pub(crate) mod placement;
pub(crate) mod placement_default;
pub mod pools;
pub(crate) mod preparation;
mod profile_reads;
pub(crate) mod purpose;
pub(crate) mod queue;
mod reliability;
pub(crate) mod repair;
pub(crate) mod residency;
pub mod resident_relay;
mod resource_profiles;
pub(crate) use cairn_common::executor_protocol::memory_reservation_cap;
pub(crate) mod rollout;
pub use rollout::ArtifactUnavailable;
pub mod runner_events;
pub mod self_update;
pub(crate) mod service_placement;
pub mod telemetry;
pub mod unreturned;
mod warm_roots;

pub use cairn_config::fleet::*;

use crate::mcp::handlers::run::{ResolvedRunBatch, RunSpec};
use crate::orchestrator::Orchestrator;
use cairn_common::executor_protocol::needs_build_volume;
use cairn_common::executor_protocol::MachineSample;
pub(crate) use cairn_common::executor_protocol::PublicationCustody;
use cairn_common::executor_protocol::SandboxCapabilityEvidence;
use cairn_common::executor_protocol::{
    executor_names_match, AdmissionHoldEvidence, ClaimedAttemptAccount, MachineModelResident,
};
use cairn_common::executor_protocol::{
    CacheWarmthEvidence, CellCheckoutKind, CellResidency, DeclineReason, EnrolledRemote,
    ExecutionWarmth, ExecutorAdvertisement, ExecutorCapabilities, ExecutorConfig,
    ExecutorHealthSnapshot, ExecutorHealthStatus, ExecutorIdentity, ExecutorInspection,
    ExecutorListenerAdvertisement, ExecutorMessage, ExecutorSelector, ExecutorSubstrateEvidence,
    ExecutorSubstrateReport, ExecutorSubstrateState, GenerationEndMemory, InventoryAuthorityState,
    MachineMeasurement, MaterializationReadFailureKind, MaterializationReadRequest,
    MaterializationReadResult, ObjectTransferCoordinate, PlacementCounterfactualWinner,
    PlacementDecision, PlacementMobility, PlacementOutcome, PlacementPolicyEvidence,
    PlacementReadings, PlacementRejection, PlacementRejectionReason, ProcessBatch,
    ProcessBatchExecution, ProcessBatchFile, ProcessBatchItem, ProcessSandboxMode,
    ProcessTelemetry, QueuedPlacementEvidence, QueuedPlacementRejection, QueuedPlacementTarget,
    RemoteAttachAttempt, RemoteConnectionTransition, RemoteLinkState, RepositoryLocator,
    ReservationFallback, ReservationRationale, ResidencyAcquireRequest, ResidencyFailureKind,
    ResidencyFence, ResidencyHolder, ResidencyOperation, ResidencyResult, ResidencyRuntimeConfig,
    ResidentProcessEvent, ResidentProcessEventKind, RunnerCallback, RunnerCallbackResult,
    UpdateReport, WarmthUnknownReason, EXECUTOR_HEARTBEAT_INTERVAL_MS,
    EXECUTOR_LINK_STALL_REMEDIATION_MS, EXECUTOR_PROGRESS_FRESHNESS_MS,
    EXECUTOR_TELEMETRY_STALL_REMEDIATION_MS, LOCAL_EXECUTOR_NAME, RESIDENCY_ACQUIRE_ATTEMPT_ID,
};
#[cfg(test)]
use cairn_common::executor_protocol::{
    CellCommandClass, ObservationReuse, PlacementReason, PlacementSelection, PlacementSyncCost,
};
use cairn_common::executor_protocol::{HomeAffinity, ScratchSyncEstimate};
use cairn_common::executor_protocol::{
    PublishIsolationEvidence, PublishIsolationVerdict, PUBLISH_ISOLATION_STALE_AFTER_MS,
};
use cairn_common::pipe_health::PipeFloorWork;
use cairn_common::wire_vocabulary::CellAdmissionKind;
use cairn_uri::normalize_executor_name;
use serde::{Deserialize, Serialize};
#[cfg(test)]
use std::collections::BTreeMap;
use std::collections::{HashMap, HashSet, VecDeque};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot, Notify};
use tokio::time::Instant;

pub use cairn_common::executor_protocol::{
    ActiveCellRequest, CellExecutionMeta, CellOutcome, CellRequest, CellUnavailableReason,
    CommandResourceIdentity, ExecutingCellRequest, FleetSnapshot, MutationDelta, MutationPolicy,
    PersistentCellLifecycle, PersistentCellState, PlacementWorkClass, ProjectCapability,
    ProjectPlacementRequirements, QueuedCellRequest, ResourceReservation,
    ResourceReservationSource,
};
use cairn_common::toolchain_requirements::project_placement_requirements;
pub use cairn_common::wire_vocabulary::CellPriority;
use cairn_platform::stack::off_frame;

use crate::mcp::handlers::permission::HandoffClosed;

// The placement facade's responsibilities live in these sibling files. Each
// item is re-exported below at the reach it had when they were one file, so
// callers keep naming `fleet::Item`; a new item belongs in the file whose
// responsibility it serves, re-exported here only if callers outside need it.
mod colocated;
mod dispatch;
mod health;
mod listener;
mod purpose_debts;
mod relay_authority;
mod requirements;
mod residency_routes;
mod selection;

use colocated::require_colocated_population;
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
use colocated::PendingTreeAdoptionResult;
pub(crate) use colocated::{kernel_wait_diagnostic, NO_COLOCATED_EXECUTOR};
#[cfg(test)]
pub(crate) use dispatch::verdict_disagreement;
pub use dispatch::CancelOutcome;
use dispatch::ItemSettlements;
#[cfg(test)]
use dispatch::{
    account_of_claimed_attempt, batch_runs_javascript, cell_build_service_env, outcome_matches,
    restamp_outcome, runs_in_live_checkout, scrub_cell_outcome, serialize_process_batch,
    unfinished_leader_line, with_cell_client_env, ClaimedAttemptSilence, ClaimedAttemptWatch,
    CoalescedLeaderCompletionGuard, WithdrawalCause,
};
use dispatch::{
    describe_runner_stop, executor_unavailable, lock_through_poison, runner_stopping,
    selected_link_dispatch_error, withdraw_claim, ActivePlacementPolicy,
    ClaimedAttemptSilenceBound, Detachment, EntryDropGuard, LeaderPreparation, RunnerStop,
};
pub(crate) use dispatch::{
    BatchPace, ExecutedRequest, GrantWithoutResult, PlacementStop, RequestReport,
};
pub(crate) use health::{evidence_context, SnapshotReceipt};
#[cfg(test)]
use health::{executor_link_status, held_request_key, EXECUTOR_LINK_STALE_AFTER_MS};
use health::{
    join_runner_work, snapshot_work, unexplained_facts_frozen_ms,
    EXECUTOR_TELEMETRY_STALE_AFTER_MS, TELEMETRY_STALL_BOUNCE_SPACING_MS,
};
use listener::{EnrolledRemoteRecord, ExecutorListenerPublication};
pub use listener::{
    ExecutorDisconnectOrigin, LinkRemediation, LinkStallBounds, LinkStallCause,
    ListenerRotationControl, ListenerRotationPendingMachine, ListenerRotationStatus,
    StalledLinkAbandonment,
};
pub(crate) use purpose_debts::default_wait_horizon_unix_ms;
pub use purpose_debts::BlockedQuestion;
use purpose_debts::{PurposeAdmissionGuard, PurposeDebtsLoaded, PurposeGate, PurposeJoiners};
#[cfg(test)]
#[cfg(test)]
use relay_authority::{BatchLink, RunnerContextExecutorBinding};
use relay_authority::{RunnerCallbackContext, RunnerContextEntry};
pub use requirements::GITHUB_PROBE_REPOSITORY_LIMIT;
use requirements::{
    applicable_refusal, build_prerequisite_deficiency, containment_deficiency,
    describe_preparation_rejections, github_credential_deficiency, is_preparation_rejection,
    matches_selector, nested_sandbox_fit, nested_sandbox_preference, nested_sandbox_summary,
    project_eligibility, refused, runs_project_setup, selector_shortfall, serves_project,
    setup_toolchain_deficiency, stamp_preparation_refusals, NestedSandboxFit, SelectorShortfall,
};
#[cfg(test)]
use requirements::{
    github_probe_repositories, matches_sandbox_capability, preparation_environment_identity,
};
#[cfg(test)]
use residency_routes::residency_placement_request;
use residency_routes::{
    acquired_residency_holder, judge_link, link_silence_ms, pipe_floor_work,
    residency_core_failure, resident_process_label, GrantedCell, PreviousHome, ResidencyRoute,
    GRANT_CONFIRMATION_GRACE_MS,
};
pub(crate) use residency_routes::{
    describe_quiet_ms, residency_queue_entry_id, EnvironmentEviction, EvictionCause, HomePrices,
    WithdrawnHome, ENVIRONMENT_LEASE_GRACE_MS,
};
#[cfg(test)]
pub(crate) use residency_routes::{
    residency_placement_request_for_test, MaterializationReadCandidate, HOME_WITHDRAWAL_MS,
};
pub use residency_routes::{DrainResidents, HeldEnvironment};
#[cfg(test)]
use selection::{
    absent_enrolled_inventory, account_from_journal, aggregate_batch_learned_estimates,
    clamp_declared_concurrency, describe_declines, judge_process_evidence, kernel_kills_for_memory,
    model_room_rejection, no_matching_executor_diagnostic, own_label_refusal,
    pipe_health_rejection, placement_rejection_signature, unresolved_rationale,
    RECENT_PLACEMENT_DECISIONS,
};
use selection::{
    candidate_rejection, deadline_evidence, declared_project_requirements_at, environment_identity,
    memory_brake_exists, missing_wait_reason_is_new, pipe_health_assessment, placement_decision,
    presence_cause, process_fit, queued_placement_evidence, record_command_outcome,
    repository_is_transferable, selected_executor, survey_candidates, waits_on_machine, Placement,
    PlacementRejectionLog, ProcessFit, ReservationPlan, SelectedExecutor,
};
pub(crate) use selection::{
    describe_room_taken_first, project_config_refusal, publication_role_rejection,
    publisher_ineligibility, AboveMemoryCeiling, PlacementScope, PROJECT_CONFIG_REFUSAL,
};

/// `5h57m`, `3m12s`, `41s`: a duration read at a glance in a diagnostic.
pub(crate) fn compact_duration(ms: u64) -> String {
    let seconds = ms / 1_000;
    match seconds {
        0..=59 => format!("{seconds}s"),
        60..=3_599 => format!("{}m{:02}s", seconds / 60, seconds % 60),
        _ => format!("{}h{:02}m", seconds / 3_600, (seconds % 3_600) / 60),
    }
}

fn check_index_from_stream_id(stream_id: &str) -> Option<usize> {
    stream_id
        .rsplit_once(":check-")
        .and_then(|(_, index)| index.parse().ok())
}

/// The item index of an agent run batch's stream, `{tool_use_id}:{index}`
/// ([`serialize_process_batch`]).
fn run_item_index_from_stream_id(stream_id: &str) -> Option<usize> {
    stream_id
        .rsplit_once(':')
        .and_then(|(_, index)| index.parse().ok())
}

fn format_duration_annotation(duration_ms: u64) -> String {
    if duration_ms >= 1_000 {
        format!("{:.1}s", duration_ms as f64 / 1_000.0)
    } else {
        format!("{duration_ms}ms")
    }
}

type RequestIdentity = (String, String);

/// What a reattached connection still owes an accounting for: the generation
/// that owes it, and the executions carried across the gap into it.
type HeldExecutions = (u64, HashMap<RequestIdentity, queue::HeldClaim>);

struct PendingLifetimeResult {
    executor_id: String,
    generation: u64,
    waiter: oneshot::Sender<ResidencyResult>,
    /// Runner queue claim whose response survives a link gap and follows its
    /// rebound generation. Plain RPCs carry no claim.
    granted_entry_id: Option<String>,
}
type PendingResidencyResults = HashMap<String, PendingLifetimeResult>;

struct PendingMaterializationRead {
    executor_id: String,
    generation: u64,
    waiter: oneshot::Sender<MaterializationReadResult>,
}

type PendingMaterializationReads = HashMap<String, PendingMaterializationRead>;

struct PendingPolicyResult {
    executor_id: String,
    generation: u64,
    waiter: oneshot::Sender<Result<cairn_common::executor_protocol::ExecutorRuntimePolicy, String>>,
}

struct PendingMemoryDiagnosticResult {
    generation: u64,
    waiter: oneshot::Sender<Result<(), String>>,
}

struct PendingDrainResult {
    executor_id: String,
    generation: u64,
    waiter: oneshot::Sender<Result<bool, String>>,
}

use crate::execution::cache::CheckResultIdentity;
use coalescing::{CoalescingKey, PlacementDemand};

struct CoalescedSubscriber {
    waiter: oneshot::Sender<CoalescedDelivery>,
    priority: CellPriority,
    requesting_job_id: Option<String>,
}

/// One shared execution. Its leader is the request its key names.
struct InFlightExecution {
    /// What the leader was submitted to satisfy, which bounds who may join.
    demand: PlacementDemand,
    subscribers: HashMap<RequestIdentity, CoalescedSubscriber>,
    publication: PublicationCoordination,
}

/// Every coalesced execution in flight, and which one each subscriber waits on.
///
/// Keyed by content AND leader: one content identity may have several
/// executions in flight at once when their placement demands are incompatible,
/// such as one lane demanded on linux and another on macos.
#[derive(Default)]
struct InFlightRegistry {
    by_key: HashMap<CoalescingKey, InFlightExecution>,
    subscriber_keys: HashMap<RequestIdentity, CoalescingKey>,
}

impl InFlightRegistry {
    /// Admit `identity` as a subscriber to `result`: onto an execution whose
    /// leader's demand meets `demand`, or onto a new one `leader` leads.
    /// Returns the execution's key and whether it was opened by this call.
    ///
    /// An execution with the same demand is preferred over a merely stricter
    /// one, so that the join is deterministic when several qualify.
    fn admit(
        &mut self,
        result: CheckResultIdentity,
        demand: &PlacementDemand,
        leader: &RequestIdentity,
        identity: RequestIdentity,
        subscriber: CoalescedSubscriber,
    ) -> (CoalescingKey, bool) {
        let joinable = self
            .by_key
            .iter()
            .filter(|(key, execution)| key.result == result && execution.demand.meets(demand))
            .min_by_key(|(key, execution)| (execution.demand != *demand, key.leader.clone()))
            .map(|(key, _)| key.clone());
        let Some(key) = joinable else {
            let key = self.open(
                CoalescingKey::new(result, leader.clone()),
                demand.clone(),
                HashMap::from([(identity, subscriber)]),
            );
            return (key, true);
        };
        self.by_key
            .get_mut(&key)
            .expect("a joinable execution is in the registry it was found in")
            .subscribers
            .insert(identity.clone(), subscriber);
        self.subscriber_keys.insert(identity, key.clone());
        (key, false)
    }

    /// Open the execution `key` names, indexing each of `subscribers` to it.
    fn open(
        &mut self,
        key: CoalescingKey,
        demand: PlacementDemand,
        subscribers: HashMap<RequestIdentity, CoalescedSubscriber>,
    ) -> CoalescingKey {
        for identity in subscribers.keys() {
            self.subscriber_keys.insert(identity.clone(), key.clone());
        }
        let publication = PublicationCoordination::for_request(key.leader.clone());
        self.by_key.insert(
            key.clone(),
            InFlightExecution {
                demand,
                subscribers,
                publication,
            },
        );
        key
    }
}

/// Where the execution a pure-verdict check subscriber is waiting on stands,
/// read from facts the fleet already holds. See [`Fleet::check_dispatch_phase`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CheckDispatch {
    pub(crate) phase: CheckDispatchPhase,
    /// Set when the execution is another request's that this subscriber
    /// coalesced onto: that request's owner, as an occupant is named (for
    /// example `cairn-5178`), or `another request` when it states none.
    pub(crate) joined: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CheckDispatchPhase {
    /// The leader's inputs (objects, the sealed ref) are still being prepared,
    /// and nothing has entered the queue yet.
    Preparing { since_unix_ms: u64 },
    /// In the queue, waiting for a machine: queued or offered and unanswered.
    /// `since_unix_ms` is when the request began waiting.
    Placing {
        since_unix_ms: u64,
        /// The latest answer from each machine that declined it.
        declines: Vec<CheckDispatchDecline>,
        /// Where the request stands with every machine, read from the
        /// dispatcher as it is now. `None` while the request is not waiting
        /// in the dispatcher (being prepared, or between leaving it and its
        /// settlement).
        placement: Option<CheckPlacementAccount>,
        /// Main's cadence turn for freed room, when it bears on this request:
        /// the machines whose next freed room it takes first.
        turn: Option<String>,
    },
    /// A machine claimed it at `since_unix_ms`, so it is running there.
    Running {
        executor_name: String,
        since_unix_ms: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CheckDispatchDecline {
    pub(crate) executor_name: String,
    pub(crate) reason: DeclineReason,
    pub(crate) detail: Option<String>,
    pub(crate) at_unix_ms: u64,
    /// The room the machine had last announced when it declined.
    pub(crate) room_seq: Option<u64>,
}

impl CheckDispatchDecline {
    fn from_entry(executor_name: &str, decline: &queue::EntryDecline) -> Self {
        Self {
            executor_name: executor_name.to_string(),
            reason: decline.reason,
            detail: decline.detail.clone(),
            at_unix_ms: decline.at_unix_ms,
            room_seq: decline.room_seq,
        }
    }
}

/// Why a queued request is still queued, machine by machine (cairn/6499):
/// what each machine it may use, or was kept off, is doing with it now.
///
/// Worked out on read from the dispatcher's state and the connection table,
/// never recorded. Recording it would cost a statement per waiting entry
/// each time any machine announced room, which is the per-request cost the
/// dispatcher exists to remove. Every time in it is an event's (a refusal,
/// a room announced, a link last heard), so the account reads the same until
/// one of those changes and the check row that renders it is rewritten only
/// then.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CheckPlacementAccount {
    pub(crate) request_id: String,
    pub(crate) attempt_id: String,
    /// Every machine the request may run on, then every machine it is kept
    /// off, each once.
    pub(crate) machines: Vec<CheckPlacementCandidate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CheckPlacementCandidate {
    pub(crate) unreadable_memory: Vec<cairn_common::executor_protocol::MemoryEvidenceFailure>,
    pub(crate) executor_name: String,
    pub(crate) status: CheckCandidateStatus,
    /// The machine's latest refusal when it no longer stands: it came under
    /// a room older than the one the machine has announced since, or from a
    /// link since replaced. Told as superseded, never as the reason now.
    pub(crate) superseded: Option<SupersededDecline>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SupersededDecline {
    pub(crate) decline: CheckDispatchDecline,
    /// The room the machine has announced since, when it has.
    pub(crate) current_seq: Option<u64>,
}

/// Where a queued request stands with one machine. Only [`Self::Declined`]
/// is the machine's own answer; the rest are the runner's reading of what
/// the machine announced and of its own facts about it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CheckCandidateStatus {
    /// Offered to it, unanswered.
    Offered,
    /// Its refusal, which stands under the room it last announced.
    Declined(CheckDispatchDecline),
    /// An offer made under this room never reached the machine; it is
    /// offered there again on the machine's next announcement.
    OfferUndelivered { room_seq: u64 },
    /// What is left of the room it announced does not fit the request:
    /// `needs` says what the room is short of, from the shared `fits`.
    NoRoomFits {
        needs: String,
        room_seq: u64,
        since_unix_ms: u64,
    },
    /// What is left of its room fits the request only with the shares it
    /// keeps back: it refused two offers under its current figures with no
    /// claim between, and the second's share stays taken until it announces
    /// new figures (cairn/7339 comment 11).
    RefusedTwice { room_seq: u64 },
    /// It was asked to hold room for the request until its running work
    /// ends. `hold` is the hold its announced room carries, when it does.
    HoldingRoom { room_seq: u64, hold: Option<String> },
    /// Attached, and has announced no room yet: silence, not a lack of room.
    RoomNotAnnounced { attached_at_unix_ms: Option<u64> },
    /// Its link is stale by the placement liveness bound (tenet 1311): last
    /// heard at `since_unix_ms`. Nothing is offered over it.
    LinkStale { since_unix_ms: u64 },
    /// Away for now: drained, or emptied for its executor update. `sentence`
    /// says what it waits on and, for an update, when it is expected back.
    Away {
        reason: PlacementRejectionReason,
        sentence: String,
    },
    /// Its room fits the request; the runner's own last look turned it away
    /// there for nothing it records, and its next announcement walks it.
    Fits { room_seq: u64 },
    /// Its room fits the request, but the work waits for `executor_name`,
    /// a machine that could hold it and ranks ahead of this one on what its
    /// memory can do for this work (deferred on a class prior here, clamped
    /// here, or less single-process capacity for an unmeasured compile).
    WaitsFor {
        executor_name: String,
        room_seq: u64,
    },
    /// Kept off it for the reason placement records.
    NotOffered(PlacementRejectionReason),
}

impl CheckCandidateStatus {
    /// The status as a placement decision's rejection row, where the wire
    /// vocabulary has a reason that says it. A room shortfall other than
    /// memory or disk, and a machine that has announced no room, have none.
    fn rejection_reason(&self, now_unix_ms: u64) -> Option<PlacementRejectionReason> {
        match self {
            Self::Declined(decline) => Some(PlacementRejectionReason::Declined {
                reason: decline.reason,
                detail: decline.detail.clone(),
                declined_at_unix_ms: decline.at_unix_ms,
            }),
            Self::LinkStale { since_unix_ms } => Some(PlacementRejectionReason::ConnectionStale {
                silence_ms: now_unix_ms.saturating_sub(*since_unix_ms),
                stale_after_ms: EXECUTOR_PROGRESS_FRESHNESS_MS,
            }),
            Self::Away { reason, .. } | Self::NotOffered(reason) => Some(reason.clone()),
            Self::WaitsFor { executor_name, .. } => Some(PlacementRejectionReason::OutrankedBy {
                executor_name: executor_name.clone(),
            }),
            Self::Offered
            | Self::OfferUndelivered { .. }
            | Self::NoRoomFits { .. }
            | Self::RefusedTwice { .. }
            | Self::HoldingRoom { .. }
            | Self::RoomNotAnnounced { .. }
            | Self::Fits { .. } => None,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct PublicationCoordination {
    state: Arc<PublicationState>,
    consumer: Option<Arc<CheckConsumerGuard>>,
}

#[derive(Debug)]
struct PublicationState {
    id: String,
    request_identity: std::sync::OnceLock<RequestIdentity>,
    claimed: AtomicBool,
    published: AtomicBool,
    /// What the publisher recorded, so every coalesced sibling can name the same
    /// observation. Written before `published` is set and read after it is
    /// observed, so the release/acquire pair also publishes this value.
    observation: std::sync::Mutex<Option<crate::execution::cache::RecordedCheckObservation>>,
    notify: Notify,
}

pub(crate) struct PublicationGuard {
    coordination: PublicationCoordination,
    published: bool,
}

pub(crate) enum PublicationRole {
    Publisher(PublicationGuard),
    /// A sibling already recorded this verdict, and named the observation it
    /// wrote. `None` means it recorded nothing — the write failed, and the
    /// verdict stands without a durable row behind it.
    Published(Option<crate::execution::cache::RecordedCheckObservation>),
}

#[derive(Clone)]
pub(crate) struct CoalescedCellOutcome {
    pub outcome: CellOutcome,
    pub publication: PublicationCoordination,
    /// What the runner knows about how the shared execution ended beyond its
    /// outcome.
    pub account: CellAccount,
}

/// What a coalesced wait delivers: the shared execution's outcome, or why
/// there is none to judge.
pub(crate) type CoalescedDelivery = Result<CoalescedCellOutcome, CoalescedWaitError>;

/// Why a coalesced wait has no execution outcome for its requester.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum CoalescedWaitError {
    /// Answered without waiting on an execution: a deadline that elapsed
    /// before the work started, or a submission refused before dispatch.
    Refused(Box<CellOutcome>),
    /// Cairn withdrew this requester, or the work it waited on, and says why.
    Cancelled(CancelCause),
    /// The leader's task ended without publishing an outcome.
    LeaderVanished,
    /// The result channel closed with nothing sent. Every path that removes a
    /// waiter answers it first, so only a bug reaches this.
    Orphaned,
}

impl CoalescedWaitError {
    fn refused(outcome: CellOutcome) -> Self {
        Self::Refused(Box::new(outcome))
    }
}

/// What a coalesced leader hands the subscribers waiting on it.
///
/// Unboxed despite the size difference: one is built per leader settlement
/// and cloned once per subscriber, so boxing would only add an allocation.
#[derive(Clone)]
#[allow(clippy::large_enum_variant)]
enum LeaderSettlement {
    /// The execution's terminal outcome, with the runner's account of it.
    Settled {
        outcome: CellOutcome,
        account: CellAccount,
    },
    /// The leader's task ended without publishing one.
    Vanished,
}

impl From<CellOutcome> for LeaderSettlement {
    fn from(outcome: CellOutcome) -> Self {
        Self::Settled {
            outcome,
            account: CellAccount::default(),
        }
    }
}

/// Why Cairn itself ended a request before it produced an answer.
///
/// A cancellation is not a failure of anything — not of a machine, the
/// substrate, or the check — so it travels to the requester as a cause rather
/// than being synthesized into an outcome a reader takes for a lost machine.
/// Distinct from [`WithdrawalCause`], which is the runner abandoning one queue
/// entry for reasons of its own.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CancelCause {
    /// An operator, or a merge's resolution cascade, stopped the job.
    JobStopped,
    /// The job's issue resolved and its session closed.
    SessionClosed,
    /// The operator compacted the job's session. Ends the session's own work
    /// only; see [`CancelCause::ends_job_work_at`].
    OperatorCompact,
    /// The operator cancelled this one request.
    OperatorRetracted,
    /// A newer request for the same answer replaced this one.
    Superseded,
    /// This runner began shutting down.
    RunnerStopping,
    /// An inference deployment loading on its machine needed the memory this
    /// work held, measured, and the work is restartable elsewhere.
    Preempted(Arc<inference::Preemption>),
    /// A cancellation came back with no cause on record.
    Unrecorded,
}

impl CancelCause {
    /// The cause as the clause a requester reads after "cancelled: ".
    pub(crate) fn sentence(&self) -> String {
        match self {
            Self::JobStopped => "job stopped".into(),
            Self::SessionClosed => "its issue resolved and closed the job's session".into(),
            Self::OperatorCompact => "the operator compacted the job".into(),
            Self::OperatorRetracted => "the operator cancelled this request".into(),
            Self::Superseded => "a newer request for the same answer replaced it".into(),
            Self::RunnerStopping => "Cairn's runner is stopping".into(),
            Self::Preempted(preemption) => preemption.sentence(),
            Self::Unrecorded => "cause not recorded".into(),
        }
    }

    /// Whether a job-wide cancel for this cause ends the job's work submitted
    /// at `priority`.
    ///
    /// A compaction resets the session, so it ends only the session's own run
    /// batches (`AgentInteractive`). Check lanes at either cadence (turn-end
    /// waves, write-cadence lanes, merge-gate verification, a manual `cairn
    /// check run`) are verdicts on a sealed commit that read nothing from the
    /// session, and every one settles through the job (its observation, and a
    /// push addressed to the job), so they keep running for the rebuilt
    /// session to read. Every other cause ends the whole job.
    pub(crate) fn ends_job_work_at(&self, priority: CellPriority) -> bool {
        !matches!(self, Self::OperatorCompact) || priority == CellPriority::AgentInteractive
    }
}

/// What the runner knows about how a request ended that the executor's
/// outcome does not say.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct CellAccount {
    /// Why Cairn cancelled the request, when it did and recorded why.
    pub cancel_cause: Option<CancelCause>,
    /// The machine that claimed the request, once one did. Without it a lost
    /// result cannot honestly be blamed on a lost machine: no machine had it.
    pub placement: Option<CellPlacement>,
    /// The runner stopped without cancelling a durably claimed remote lane.
    pub left_running: Option<LeftRunningAttempt>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LeftRunningAttempt {
    pub(crate) machine: String,
    pub(crate) request_id: String,
    pub(crate) attempt_id: String,
    pub(crate) executor_id: String,
    pub(crate) stopped_at_ms: u64,
}

/// The durable consumer that can own an execution after this runner stops.
/// Additional claim kinds register here rather than adding shutdown exceptions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AdoptionOwner {
    MainCadence,
    /// A durable check request (`check_requests`), whose driver re-binds to
    /// the lane at boot.
    CheckRequest,
}

/// One eligible machine placement cannot decide on right now, because a
/// reading it decides on is stale or missing. A condition that clears on its
/// own when the machine reports again, unlike a machine that is gone or a
/// fleet with nothing that matches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MeasurementBlindness {
    pub executor_id: String,
    /// The machine as an operator names it.
    pub name: String,
    /// Placement's own words for the refusal, e.g. "cpu was measured 108276ms
    /// ago, past the 90000ms bound".
    pub reason: String,
    /// How long the machine has been past its freshness bound. `None` when a
    /// reading is missing outright, which carries no instant to measure from.
    pub blind_for_ms: Option<u64>,
}

/// The machine a request was placed on: the one that claimed it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CellPlacement {
    pub executor_id: String,
    /// The link generation the claim was made on.
    pub generation: u64,
    /// The machine as an operator names it.
    pub name: String,
    /// What the machine advertised itself as when it claimed the request.
    /// When its own account of the run is lost, this is the only honest
    /// answer to which platform the run belongs to. `None` only for a
    /// placement reconstructed after the machine was forgotten.
    pub platform: Option<ExecutorPlatform>,
}

/// One machine [`Fleet::known_machine_platforms`] reports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KnownMachinePlatform {
    pub executor_id: String,
    pub name: String,
    pub os: String,
    pub arch: String,
    /// What it advertises once attached; `None` for an enrolled machine that
    /// has not attached, whose toolchains are unknown until it does.
    pub toolchains: Option<Vec<String>>,
    /// What this runner holds it to be for.
    pub role: MachineRole,
    /// Its publisher isolation verdict, once attached and advertising one.
    pub publish_isolation: Option<PublishIsolationEvidence>,
}

/// The operating system and architecture a machine advertises, in the
/// lowercase vocabulary verdict platforms are recorded in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExecutorPlatform {
    pub os: String,
    pub arch: String,
}

impl ExecutorPlatform {
    pub(crate) fn advertised(capabilities: &ExecutorCapabilities) -> Self {
        Self {
            os: capabilities.os.to_ascii_lowercase(),
            arch: capabilities.arch.to_ascii_lowercase(),
        }
    }

    /// The platform of the process this runner is.
    pub(crate) fn this_process() -> Self {
        Self {
            os: std::env::consts::OS.to_string(),
            arch: std::env::consts::ARCH.to_string(),
        }
    }
}

impl std::fmt::Display for ExecutorPlatform {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.os, self.arch)
    }
}

/// How many cancellation causes the runner holds while their outcomes are in
/// flight. Bounded because an outcome that never comes back must not hold its
/// entry forever; a cause evicted early reads as `Unrecorded`, which is still
/// a cancellation and still moves nothing.
const RECENT_CANCEL_CAUSES: usize = 256;

/// Why Cairn sent `Cancel` for requests whose terminal outcome has not yet
/// come back. The executor's `Cancelled` carries no cause, so the runner keeps
/// it here and joins it to the outcome when the outcome arrives.
#[derive(Default)]
struct CancelCauses {
    order: VecDeque<RequestIdentity>,
    causes: HashMap<RequestIdentity, CancelCause>,
}

impl CancelCauses {
    /// The first cause recorded for a request is the one that stopped it.
    fn record(&mut self, identity: RequestIdentity, cause: CancelCause) {
        if self.causes.contains_key(&identity) {
            return;
        }
        if self.order.len() >= RECENT_CANCEL_CAUSES {
            if let Some(evicted) = self.order.pop_front() {
                self.causes.remove(&evicted);
            }
        }
        self.causes.insert(identity.clone(), cause);
        self.order.push_back(identity);
    }

    fn take(&mut self, identity: &RequestIdentity) -> Option<CancelCause> {
        let cause = self.causes.remove(identity)?;
        self.order.retain(|recorded| recorded != identity);
        Some(cause)
    }

    /// Whether this request was preempted and has not ended yet.
    fn preempted(&self, identity: &RequestIdentity) -> bool {
        matches!(self.causes.get(identity), Some(CancelCause::Preempted(_)))
    }
}

pub(crate) struct PureVerdictBatchItem {
    pub result_identity: CheckResultIdentity,
    pub process: ProcessBatchItem,
}

async fn register_manual_check_publications(
    db: &crate::storage::LocalDb,
    project_id: String,
    publications: Vec<(String, String)>,
) -> Result<(), String> {
    db.write(move |conn| {
        let project_id = project_id.clone();
        let publications = publications.clone();
        Box::pin(async move {
            for (handle, publication_id) in publications {
                // An unfinished request can retry on a new shared execution.
                // Once its source or response is attached, admission cannot move it.
                let changed = conn
                    .execute(
                        "UPDATE check_requests SET publication_id=?3
                     WHERE project_id=?1 AND id=?2
                       AND observation_id IS NULL AND outcome_json IS NULL",
                        cairn_db::turso::params![
                            project_id.as_str(),
                            handle.as_str(),
                            publication_id
                        ],
                    )
                    .await?;
                if changed != 1 {
                    return Err(crate::storage::DbError::internal(format!(
                        "manual check publication registration failed for {handle}"
                    )));
                }
            }
            Ok(())
        })
    })
    .await
    .map_err(|error| error.to_string())
}

impl PublicationCoordination {
    fn new() -> Self {
        Self {
            state: Arc::new(PublicationState {
                id: uuid::Uuid::new_v4().to_string(),
                request_identity: std::sync::OnceLock::new(),
                claimed: AtomicBool::new(false),
                published: AtomicBool::new(false),
                observation: std::sync::Mutex::new(None),
                notify: Notify::new(),
            }),
            consumer: None,
        }
    }

    pub(crate) fn with_consumer_guard(mut self, guard: Arc<CheckConsumerGuard>) -> Self {
        self.consumer = Some(guard);
        self
    }

    pub(crate) fn consumer_guard(&self) -> Option<Arc<CheckConsumerGuard>> {
        self.consumer.clone()
    }

    pub(crate) fn id(&self) -> &str {
        &self.state.id
    }

    pub(crate) fn for_request(identity: RequestIdentity) -> Self {
        let publication = Self::new();
        let _ = publication.state.request_identity.set(identity);
        publication
    }

    pub(crate) fn request_identity(&self) -> Option<RequestIdentity> {
        self.state.request_identity.get().cloned()
    }

    pub(crate) async fn acquire(&self) -> PublicationRole {
        loop {
            if self.state.published.load(Ordering::Acquire) {
                return PublicationRole::Published(self.state.observation.lock().unwrap().clone());
            }
            if self
                .state
                .claimed
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return PublicationRole::Publisher(PublicationGuard {
                    coordination: self.clone(),
                    published: false,
                });
            }
            self.state.notify.notified().await;
        }
    }
}

impl PublicationGuard {
    /// Declare this verdict recorded, naming the observation written for it so a
    /// coalesced sibling reports the same row rather than none.
    pub(crate) fn published(
        mut self,
        observation: Option<crate::execution::cache::RecordedCheckObservation>,
    ) {
        *self.coordination.state.observation.lock().unwrap() = observation;
        self.coordination
            .state
            .published
            .store(true, Ordering::Release);
        self.coordination.state.notify.notify_waiters();
        self.published = true;
    }
}

impl Drop for PublicationGuard {
    fn drop(&mut self) {
        if !self.published {
            self.coordination
                .state
                .claimed
                .store(false, Ordering::Release);
            self.coordination.state.notify.notify_waiters();
        }
    }
}

type ResidentProcessSubscriber = Arc<dyn Fn(ResidentProcessEvent) + Send + Sync>;

/// The resident-process subscribers, each under the id its
/// [`ResidentProcessSubscription`] removes it by.
#[derive(Default)]
struct ResidentProcessSubscribers {
    next_id: u64,
    entries: Vec<(u64, ResidentProcessSubscriber)>,
}

/// A registration on the fleet's resident-process event stream, held for as
/// long as its subscriber should hear events.
///
/// Dropping it unregisters the subscriber, so a per-process subscriber leaves
/// with the session, run, or lease that owns it rather than accumulating for the
/// life of the runner. A dispatch already in progress may still deliver to it
/// once after the drop: dispatch calls subscribers outside the registry lock so
/// that a subscriber, or anything it releases, can drop a subscription without
/// deadlocking.
#[must_use = "dropping the subscription unregisters the subscriber at once; hold it for as long as events matter, or `detach` it for a subscriber that lives as long as the fleet"]
pub(crate) struct ResidentProcessSubscription {
    id: u64,
    subscribers: std::sync::Weak<Mutex<ResidentProcessSubscribers>>,
}

impl ResidentProcessSubscription {
    /// Keep the subscriber registered for as long as the fleet exists.
    pub(crate) fn detach(mut self) {
        self.subscribers = std::sync::Weak::new();
    }
}

impl Drop for ResidentProcessSubscription {
    fn drop(&mut self) {
        if let Some(subscribers) = self.subscribers.upgrade() {
            let id = self.id;
            subscribers
                .lock()
                .unwrap()
                .entries
                .retain(|(entry, _)| *entry != id);
        }
    }
}

pub(crate) mod node_scratch;
pub use inference::DeploymentReportReceived;
pub(crate) use node_scratch::NodeScratchEvent;

/// Consecutive refusals of one machine on the same pair of protocols.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProtocolRefusalRun {
    expected: u32,
    received: u32,
    first_unix_ms: u64,
    last_unix_ms: u64,
    count: u32,
}

impl ProtocolRefusalRun {
    fn extended(previous: Option<Self>, expected: u32, received: u32, now_unix_ms: u64) -> Self {
        match previous {
            Some(run) if run.expected == expected && run.received == received => Self {
                last_unix_ms: now_unix_ms,
                count: run.count.saturating_add(1),
                ..run
            },
            _ => Self {
                expected,
                received,
                first_unix_ms: now_unix_ms,
                last_unix_ms: now_unix_ms,
                count: 1,
            },
        }
    }

    /// The stranding this run amounts to: a refusal is stranding exactly when
    /// the runner has resolved the machine's target and has nothing to serve.
    /// A target not yet resolved claims nothing either way.
    fn stranding(
        self,
        target: &str,
        artifacts: &HashMap<String, rollout::ArtifactBuild>,
    ) -> Option<cairn_common::executor_protocol::StrandedExecutor> {
        let rollout::ArtifactBuild::Unavailable(unavailable) = artifacts.get(target)? else {
            return None;
        };
        Some(cairn_common::executor_protocol::StrandedExecutor {
            target: target.to_string(),
            expected_protocol: self.expected,
            received_protocol: self.received,
            newest_held_protocol: unavailable.newest_held_protocol,
            reason: unavailable.reason.clone(),
            refused_since_unix_ms: self.first_unix_ms,
            last_refused_unix_ms: self.last_unix_ms,
            refusals: self.count,
        })
    }
}

/// The artifact target an enrolled machine's platform runs.
fn enrolled_artifact_target(os: &str, arch: &str) -> Option<&'static str> {
    cairn_common::executor_protocol::RemotePlatform::from_os_arch(
        &os.to_ascii_lowercase(),
        &arch.to_ascii_lowercase(),
    )
    .map(cairn_common::executor_protocol::RemotePlatform::target)
}

/// See [`Fleet::predicted_job_home`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PredictedJobHome {
    pub executor_id: String,
    /// Whether a residency route names this machine, as opposed to it being the
    /// colocated machine guessed for an owner that has no environment yet.
    pub recorded: bool,
}

/// One generation's most recent process telemetry, waiting for that generation
/// to end so it can be stamped onto the transition that ended it.
#[derive(Debug, Clone)]
struct GenerationProcessReport {
    generation: u64,
    received_at_unix_ms: u64,
    process: ProcessTelemetry,
}

const REMOTE_CONNECTION_TIMELINE_LIMIT: usize = 32;

/// What a drain write reached.
///
/// A drain is durable runner state either way: the runner has stopped placing
/// work on the machine the moment it is recorded. This says only whether the
/// machine itself was told, which is a courtesy the runner extends when it can
/// and reports honestly when it cannot.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", tag = "application")]
pub enum DrainApplication {
    /// Committed projection published; no machine acknowledgement was awaited.
    Published,
    /// The executor acknowledged the change on its own admission path too.
    Applied,
    /// The runner holds the drain and the machine does not know yet.
    HeldWithoutMachine { reason: String },
}

/// How long a successor waits for a displaced predecessor's debt to settle
/// before it is refused.
///
/// A bound on one caller's wait, not on the debt. An ordinary retirement
/// resolves in the time a message takes to cross the link plus the time the
/// item takes to reach its next phase transition. Reaching the budget settles
/// nothing: the successor is refused with a diagnostic naming every
/// outstanding debt, the debt keeps blocking its question, and only evidence
/// that the displaced work stopped (or an operator naming the overlap) clears
/// it. Dispatching anyway would knowingly create the overlap this gate exists
/// to prevent.
const RETIREMENT_CONFIRMATION_BUDGET: std::time::Duration = std::time::Duration::from_secs(120);

/// What a refusal to dispatch a second answer to one question says that
/// question still owes. The main cadence recognises a hold by it.
pub(crate) const PURPOSE_OWES_DEATH: &str = "still owes a confirmed death";

#[derive(Clone, Default)]
pub struct Fleet {
    /// Where this runner's executor listener answers, once it is bound.
    ///
    /// Published by the runner when it binds or its identity/endpoints change,
    /// and read by everything that needs
    /// to tell a machine where to reach it: the `Ready` a connected executor
    /// receives, and the bootstrap ceremony that writes a new machine's config.
    /// One home, because those two must never describe different endpoints — a
    /// machine bootstrapped against one address and told to reconnect at another
    /// would be stranded by the disagreement alone.
    ///
    /// Empty on a runner that serves no listener, which is every runner with no
    /// remote fleet.
    executor_listener: Arc<ExecutorListenerPublication>,
    listener_rotation_control: Arc<Mutex<Option<Arc<dyn ListenerRotationControl>>>>,
    connections: Arc<Mutex<HashMap<String, ExecutorConnectionState>>>,
    connection_generations: Arc<Mutex<HashMap<String, u64>>>,
    /// One lock per executor serializing its synced registry writes; see
    /// [`Fleet::registry_write_permit`].
    registry_write_fences: Arc<Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
    /// Each machine's recent load, by executor identity. Beside
    /// `connections` rather than in them, so a link that drops and reattaches
    /// keeps the machine's history: the ring is the runner's account of the
    /// machine, not the link's.
    load_rings: Arc<Mutex<HashMap<String, load::LoadRing>>>,
    disconnect_origins: Arc<Mutex<HashMap<(String, u64), ExecutorDisconnectOrigin>>>,
    /// Woken when a machine attaches or its link ends, for what waits for a
    /// machine to be present (enrollment, a named executor, a residency
    /// response's link). Placement never waits on it.
    connection_ready: Arc<tokio::sync::Notify>,
    /// Every waiting entry's placement and every machine's announced room
    /// (see [`assignments`] and [`dispatcher`]). Lock order: `connections`,
    /// then this, then the queue.
    placing: Arc<Mutex<assignments::Placing>>,
    /// Re-arms the placement deadline task when an entry joins, since its
    /// deadlines may be earlier than the one the task sleeps to.
    placing_deadline: Arc<tokio::sync::Notify>,
    /// Whether this fleet's placement deadline task has been started.
    placing_task: Arc<std::sync::atomic::AtomicBool>,
    /// Every execution this runner has admitted but not yet settled: waiting to
    /// be placed, offered to a machine, or claimed and running. One home for
    /// what used to be two — a map of pre-dispatch leaders and a map of
    /// in-flight requests — which is what lets a generation ending decide the
    /// fate of both in a single pass. See [`queue`].
    queue: Arc<queue::ExecutionQueue>,
    /// The runtime this fleet was built on, which outlives any one caller.
    ///
    /// A coalesced leader answers for everyone who joined its execution, so it
    /// cannot run on the runtime of whichever caller happened to open it: that
    /// runtime may end first, and ending drops every task on it, a leader
    /// included, mid-execution. Synchronous code driving async work through a
    /// runtime built for the one call (`run_db_blocking` and its siblings) is
    /// on exactly such a runtime. Absent on a fleet built off any runtime,
    /// which `Fleet::default()` is; its leaders run on the caller's runtime.
    host_runtime: Option<tokio::runtime::Handle>,
    /// Executions carried across a link gap that the reattached connection has
    /// not yet accounted for, with the generation that owes the accounting.
    ///
    /// Emptied by the first snapshot of that generation, which is the machine's
    /// own statement of what it still has.
    unreconciled_executions: Arc<Mutex<HashMap<String, HeldExecutions>>>,
    pending_residency: Arc<Mutex<PendingResidencyResults>>,
    pending_materialization_reads: Arc<Mutex<PendingMaterializationReads>>,
    pending_policy: Arc<Mutex<HashMap<String, PendingPolicyResult>>>,
    pending_memory_diagnostic: Arc<Mutex<HashMap<String, PendingMemoryDiagnosticResult>>>,
    pending_tree_adoption: Arc<Mutex<HashMap<String, PendingTreeAdoptionResult>>>,
    desired_runtime_policies:
        Arc<Mutex<HashMap<String, cairn_common::executor_protocol::ExecutorRuntimePolicy>>>,
    pending_drain: Arc<Mutex<HashMap<String, PendingDrainResult>>>,
    /// Publication, attach and fence changes share this ordering boundary.
    drain_projection: Arc<Mutex<drain_projection::DrainProjection>>,
    /// What each machine is for, by executor identity; absent means general.
    ///
    /// A cache of [`FleetConfig::machine_roles`], which is the one authority:
    /// it is only ever replaced whole by [`Fleet::hydrate_machine_roles`] from
    /// a config just loaded or just persisted, so it cannot hold a role the
    /// settings file does not. Attach stamps from it; the machine is never
    /// told.
    machine_roles: Arc<Mutex<HashMap<String, MachineRole>>>,
    /// Placement's local models: the residents on each machine, the room
    /// refusals that may evict them, and the pass that decides them.
    models: Arc<models::ModelPlacement>,
    /// Where deployment reports go; see [`Fleet::subscribe_deployment_reports`].
    deployment_reports:
        Arc<Mutex<Option<mpsc::UnboundedSender<inference::DeploymentReportReceived>>>>,
    /// Every recorded preparation refusal, by executor id then project id.
    ///
    /// The in-memory face of `executor_preparation_refusals`, hydrated when the
    /// store attaches and written through by every recording, so attach and
    /// placement read it without touching the database.
    ///
    /// One lock over the held refusals and the ordered writer that persists
    /// them; absent a store (a bare `Fleet::default()`), memory only.
    preparation_refusals: Arc<Mutex<preparation::PreparationRefusals>>,
    /// Capabilities each project's work was learned to need from a run that
    /// failed for want of one; stamped onto every request beside what the
    /// project declares. See [`learned_requirements`].
    learned_requirements: learned_requirements::LearnedRequirements,
    /// Each project's placement requirements as its most recent preparation on
    /// this runner resolved them, for the synchronous decisions that have no
    /// request to read them from: whether a job's home can still prepare its
    /// project, and what the executors page shows.
    known_project_requirements: Arc<Mutex<HashMap<String, ProjectPlacementRequirements>>>,
    /// Projects whose most recent preparation populates cells from the
    /// runner's live checkout, which binds their cells to the colocated
    /// executor. Learned where `known_project_requirements` is, for the same
    /// synchronous reason: route resolution has to know whether an environment
    /// could be acquired anywhere else before it gives one up.
    colocated_population_projects: Arc<Mutex<HashSet<String>>>,
    residency_routes: Arc<Mutex<HashMap<(String, String), ResidencyRoute>>>,
    residency_route_path: Arc<Option<PathBuf>>,
    residency_route_store_error: Arc<Mutex<Option<String>>>,
    /// The cell each residency result returned, by executor and holder, held
    /// until that executor's own report carries it. An executor answers a
    /// grant before it publishes the snapshot that shows the grant, and the
    /// two travel separately, so the snapshot can reach the runner well after
    /// the answer; and a snapshot taken before the grant can arrive after it.
    /// Between the two, the result is the only account of the cell.
    ///
    /// Scoped to the link generation the result arrived on and kept in memory:
    /// a new link's opening snapshot, and after a runner restart the reconnect
    /// snapshot, is the authority.
    ///
    /// Lock order: `residency_routes`, then this; never the reverse.
    granted_cells: Arc<Mutex<HashMap<(String, String), GrantedCell>>>,
    /// Environments this runner rebuilt because the executor session holding the
    /// previous one ended, keyed by holder and drained by the surface that
    /// reports it. Held in memory on purpose: it is a fact about this runner's
    /// current session, and an unreported one is worth less than a stale one
    /// resurfacing days later.
    environment_evictions: Arc<Mutex<HashMap<String, EnvironmentEviction>>>,
    /// Machines withdrawn from an owner's placement after refusing to provision
    /// its environment, keyed by holder. In memory for the same reason as
    /// `environment_evictions`: each one is a verdict about a machine session
    /// this runner is watching, and lapses with it.
    withdrawn_homes: Arc<Mutex<HashMap<String, Vec<WithdrawnHome>>>>,
    /// The machine each job's home last answered from, keyed by holder, which
    /// a thread's next fresh home is priced toward. In memory by choice: a
    /// runner restart costs one placement on the runner's platform, and a live
    /// home's route persists anyway.
    previous_homes: Arc<Mutex<HashMap<String, PreviousHome>>>,
    /// When each absent executor was first observed to be absent, which is what
    /// separates a link that flapped from a machine that left. Stamped when a
    /// connection goes away and when an acquisition first finds a route with no
    /// connection behind it; cleared when the executor attaches again. Monotonic
    /// stamps, so a link that died as the host went to sleep is not read as gone
    /// for the length of the sleep.
    link_absence_since: Arc<Mutex<HashMap<String, u64>>>,
    /// Whether the colocated executor has attached in this runner's life.
    colocated_attached_once: Arc<std::sync::atomic::AtomicBool>,
    /// When the runner last bounced a link for publishing frozen facts.
    ///
    /// Facts freezing on several machines within one window is the specimen
    /// this remediation exists for, and it is also the case where recycling all
    /// of them together is worst. The trigger is shared, so a simultaneous
    /// bounce reruns whatever caused it on the whole fleet at once, drops every
    /// in-flight attempt in one window, and hands the survivors the entire
    /// load. One machine at a time, spaced, recovers capacity just as fast in
    /// the case that matters and leaves a legible sequence behind instead of a
    /// cliff.
    ///
    /// Silence bounces are deliberately not paced through here: a link nobody
    /// has heard from is already gone, and making its replacement wait behind
    /// another machine's recovery would only extend the outage.
    last_telemetry_bounce_unix_ms: Arc<Mutex<u64>>,
    resident_process_subscribers: Arc<Mutex<ResidentProcessSubscribers>>,
    node_scratch_subscribers: Arc<Mutex<Vec<node_scratch::NodeScratchSubscriber>>>,
    /// At most one live work item per question. The single chokepoint fact that
    /// makes duplicate answers unrepresentable rather than merely suppressed;
    /// see [`purpose`].
    purposes: Arc<Mutex<purpose::PurposeRegistry>>,
    /// Woken whenever a purpose debt settles, so a successor waiting on its
    /// predecessor's death, and a cadence waiting to re-arm, learn of it
    /// rather than polling.
    purpose_quiesced: Arc<tokio::sync::Notify>,
    /// Woken whenever an item is retired, so work this runner still holds
    /// before any grant (being placed, queued, offered) ends at once instead
    /// of when some machine happens to claim it.
    retirements: Arc<tokio::sync::Notify>,
    /// Purpose items whose every check joined an execution another leader
    /// opened: the question each answers and the subscriber identities it
    /// waits under. Retiring one detaches those.
    purpose_joiners: Arc<Mutex<PurposeJoiners>>,
    /// Whether the debts a previous runner process journaled have been read
    /// back; purpose admission waits on it.
    purpose_debts_loaded: PurposeDebtsLoaded,
    /// The last failure to make a purpose debt change durable.
    purpose_debt_journal_error: Arc<Mutex<Option<String>>>,
    cancelled_leaders: Arc<Mutex<HashSet<RequestIdentity>>>,
    /// Why each request Cairn sent `Cancel` for was cancelled, until its
    /// outcome returns and takes the cause with it.
    cancel_causes: Arc<Mutex<CancelCauses>>,
    coalesced_leaders: Arc<Mutex<HashSet<RequestIdentity>>>,
    preparing_leaders: Arc<Mutex<HashMap<RequestIdentity, LeaderPreparation>>>,
    in_flight: Arc<Mutex<InFlightRegistry>>,
    runner_contexts: Arc<Mutex<HashMap<String, RunnerContextEntry>>>,
    /// Why recently ended runner contexts ended, so a late call on one is
    /// refused by name. Locked after `runner_contexts`, never before it.
    closed_runner_contexts: Arc<Mutex<relay_authority::ClosedRunnerContexts>>,
    /// The callback relays of resident processes on machines other than this
    /// runner's. See [`resident_relay`].
    resident_relays: Arc<Mutex<resident_relay::ResidentRelays>>,
    /// Streaming scrubbers for resident-process output, one per live stream.
    ///
    /// Terminals, REPLs, workflows, and dev instances all reach their consumers
    /// through one fan-out, and each of them is given a per-process relay
    /// capability this runner mints (never its machine secret). On unix the
    /// executor strips it from the environment and attaches it to calls on the
    /// lineage socket; only on Windows is it carried in `CAIRN_MCP_SECRET`.
    /// Either way the executor's scrubber registers only per-batch
    /// capabilities, so this runner, which registered the resident one, is the
    /// pass that covers it.
    resident_output: output_guard::SharedRelayedOutput,
    recent_cached_completions:
        Arc<Mutex<VecDeque<cairn_common::executor_protocol::CellCompletion>>>,
    expected_executor_build_ids: Arc<Mutex<HashMap<String, String>>>,
    /// The executor build this runner serves for each artifact target, as the
    /// self-update driver last resolved it. A remote's expected build is the
    /// one here for its target. Locked last wherever it is locked with
    /// anything else.
    executor_artifact_builds: Arc<Mutex<HashMap<String, rollout::ArtifactBuild>>>,
    /// The rolling self-update of remotes whose build differs from what this
    /// runner serves (see [`rollout`]). Locked alone or after `connections`,
    /// never before it.
    self_update: Arc<Mutex<rollout::Rollout>>,
    self_update_runtime: Arc<self_update::SelfUpdateRuntime>,
    /// Machines the rollout concluded did not return from a self-update,
    /// waiting for the holder of their bootstrap (see [`unreturned`]).
    update_recoveries: Arc<unreturned::UpdateRecoveries>,
    /// The placement decisions this runner most recently took, newest last.
    recent_placements: Arc<Mutex<VecDeque<PlacementDecision>>>,
    /// Which placement rejections have been logged, so an unchanged verdict
    /// about one machine for one request is logged when it changes and in a
    /// periodic summary, not on every pass. The placement record keeps them all.
    placement_rejection_log: Arc<Mutex<PlacementRejectionLog>>,
    /// What placing queued work has cost this runner (see [`counters`]).
    counters: Arc<counters::FleetCounters>,
    /// Rate limit for the pushed telemetry frame. One per runner rather than
    /// one per connection: an N-machine fleet beating independently would
    /// otherwise emit at N times the intended cadence, each connection
    /// believing it was within its own budget.
    telemetry_cadence: Arc<telemetry::TelemetryCadence>,
    colocated_substrate_state: Arc<Mutex<Option<ExecutorSubstrateEvidence>>>,
    /// The supervision nonce the runner handed its current colocated launch. A
    /// colocated Hello is admitted only when it presents this value, so an
    /// executor an earlier runner started cannot attach to this one.
    expected_colocated_nonce: Arc<Mutex<Option<String>>>,
    /// What supervising the colocated executor has looked like across
    /// attempts. Outlives the substrate declaration, which every attempt
    /// replaces and readiness clears.
    colocated_supervision: Arc<Mutex<cairn_common::executor_protocol::ColocatedSupervision>>,
    /// Every machine this runner is enrolled with, keyed by executor id and
    /// kept whether or not the machine is attached. Always locked AFTER
    /// `connections` where both are needed, so the two orders cannot deadlock.
    enrolled_remotes: Arc<Mutex<HashMap<String, EnrolledRemoteRecord>>>,
    /// Wakes listener-certificate gate reconciliation whenever enrollment
    /// membership or ceremony state changes. The transport layer owns the
    /// rotation policy; Fleet only publishes that its authoritative membership
    /// projection moved.
    enrollment_gate_changed: Arc<tokio::sync::Notify>,
    /// Who manages this fleet and what enrollments are in flight. Held here
    /// because management is fleet state: the same projection that answers what
    /// machines exist has to answer what is currently becoming one.
    management: Arc<management::ExecutorManagementState>,
    /// When this runner began shutting down, once it has. Every in-flight
    /// request races its own lifecycle against it.
    runner_stop: Arc<RunnerStop>,
    /// How long a claimed attempt may go without its machine accounting for
    /// it; see [`CLAIMED_ATTEMPT_SILENCE_BOUND_MS`].
    claimed_attempt_silence: ClaimedAttemptSilenceBound,
    /// The one sink for this runner's lifecycle facts; see [`runner_events`].
    /// Unattached (memory only) on a bare `Fleet::default()`.
    runner_events: runner_events::RunnerEvents,
    /// This runner process's identity in every journal it writes, set once
    /// when the journals attach.
    runner_incarnation: Arc<std::sync::OnceLock<String>>,
    /// Results that arrived for no binding this process holds, for the
    /// orchestrator's adoption to decide (`run::adopted`).
    unbound_results: Arc<Mutex<UnboundResults>>,
    check_adoptions: Arc<Mutex<check_adoptions::CheckAdoptions>>,
    /// Run batches whose results this process acknowledges only once the
    /// batch's own settlement is done with them, by request id, with each
    /// attempt and machine a result for it arrived from.
    deferred_acks: Arc<Mutex<HashMap<String, DeferredAttempts>>>,
    #[cfg(test)]
    result_ack_probe: Arc<Mutex<Option<ResultAckProbe>>>,
    /// Reauthorization answers awaited, by attempt.
    reauthorizations: Arc<Mutex<HashMap<RequestIdentity, ReauthorizationWaiter>>>,
    /// Requests an unsettled durable row of an earlier runner process still
    /// claims, named in every `WaitingRequests` so a machine still preparing
    /// one does not retire it for want of a waiter.
    adopting: Arc<Mutex<HashSet<String>>>,
}

/// Where a machine's answer to `ReauthorizeTransfers` goes.
/// The `(attempt_id, executor_id)` of each result a run batch's settlement
/// acknowledges once it is done with them.
type DeferredAttempts = Vec<(String, String)>;
#[cfg(test)]
type ResultAckProbe = Arc<dyn Fn(&str, &str) + Send + Sync>;

type ReauthorizationWaiter =
    oneshot::Sender<Vec<cairn_common::executor_protocol::ReauthorizedTransfer>>;

/// A result no binding in this process accounts for.
#[derive(Clone, Debug)]
pub(crate) struct UnboundResult {
    pub(crate) executor_id: String,
    pub(crate) machine: String,
    pub(crate) request_id: String,
    pub(crate) attempt_id: String,
    pub(crate) outcome: CellOutcome,
}

#[derive(Default)]
struct UnboundResults {
    sink: Option<mpsc::UnboundedSender<UnboundResult>>,
    /// Held until a sink is installed. Never acknowledged meanwhile: until the
    /// orchestrator can read its durable rows nothing here can decide, and a
    /// result acknowledged wrongly is lost for good.
    pending: Vec<UnboundResult>,
}

/// Acknowledges a run batch's results when the batch's own settlement drops
/// it: after the answer has reached whatever durable row or call it was for.
/// Never dropped by a runner process that died first, which is the point.
/// Dropped by a panic, it acknowledges nothing: a settlement that unwound is
/// not one that finished.
pub(crate) struct DeferredResultAck {
    fleet: Fleet,
    request_id: String,
    retain: bool,
}

impl DeferredResultAck {
    /// A failed recorder has not earned an acknowledgement.
    pub(crate) fn retain(mut self) {
        self.retain = true;
    }
}

impl Drop for DeferredResultAck {
    fn drop(&mut self) {
        if self.retain || std::thread::panicking() {
            return;
        }
        // A check grant owns its acknowledgement until every observation and
        // consumer delivery is durable, even if one caller's guard ends early.
        if self
            .fleet
            .check_adoptions
            .lock()
            .unwrap()
            .claims
            .keys()
            .any(|identity| identity.0 == self.request_id)
        {
            return;
        }
        let delivered = self
            .fleet
            .deferred_acks
            .lock()
            .unwrap()
            .remove(&self.request_id)
            .unwrap_or_default();
        for (attempt_id, executor_id) in delivered {
            self.fleet
                .send_result_ack(&executor_id, &self.request_id, &attempt_id);
        }
    }
}

/// Grace after a residency response's link stops reporting progress.
/// This bounds response silence, never admission or provisioning duration.
const RESIDENCY_RESPONSE_FLOOR_MS: u64 = 30_000;

/// How the machine an entry was offered to answered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OfferAnswer {
    /// It took responsibility. The work binds here.
    Claimed,
    /// It refused, its link ended, or it never answered. The entry is back in
    /// the queue and the next candidate can have it.
    Requeued,
    /// The requester's horizon arrived while this offer was outstanding.
    Expired,
    /// The entry left the queue entirely: cancelled, or settled elsewhere.
    Gone,
}

/// What a wait bounded by silence rather than by elapsed time ended up doing.
enum LinkBoundResponse<T> {
    /// The other side answered.
    Answered(T),
    /// The response channel closed without an answer.
    Dropped,
    /// Nothing reported progress for a whole silence budget.
    LinkSilent,
}

impl Fleet {
    /// Give this fleet's queue a durable journal, and close whatever a previous
    /// runner incarnation left open.
    ///
    /// Optional by construction, the way the residency-route store is. The queue
    /// is always authoritative and always present; the journal is a sink on top
    /// of it rather than a dependency underneath it, so `Fleet::default()` --
    /// which is what every test builds -- journals nothing and needs no database.
    pub(crate) fn attach_queue_journal(
        &self,
        db: Arc<crate::storage::LocalDb>,
        runner_incarnation: String,
    ) {
        queue::spawn_queue_journal(db, runner_incarnation, self.queue.clone());
    }

    /// What placing queued work has cost this runner so far.
    pub(crate) fn placement_counters(&self) -> &counters::FleetCounters {
        &self.counters
    }

    /// Log [`Self::placement_counters`] once a minute for the life of this fleet.
    pub(crate) fn spawn_placement_counter_log(&self) {
        counters::spawn_minute_log(&self.counters);
    }

    /// Give this runner's lifecycle facts a durable journal, opening it with
    /// the `runner_started` row that marks where this process's connection
    /// generations begin. Attach it before the queue journal, so that row is
    /// the first this incarnation writes anywhere.
    pub(crate) fn attach_runner_event_journal(
        &self,
        db: Arc<crate::storage::LocalDb>,
        runner_incarnation: String,
    ) {
        if let Err(error) = self
            .runner_events
            .attach_journal(db.clone(), &runner_incarnation)
        {
            log::error!("runner event journal did not attach: {error}");
        }
        let _ = self.runner_incarnation.set(runner_incarnation);
    }

    fn record_memory_lookup_failures(
        &self,
        resolved: &mut resource_profiles::ResolvedResourceProfile,
        executor_id: &str,
    ) {
        for failure in &resolved.process_evidence.unreadable_memory {
            if !resolved.rationale.unreadable_memory.contains(failure) {
                resolved.rationale.unreadable_memory.push(failure.clone());
            }
        }
        for failure in &resolved.rationale.unreadable_memory {
            self.runner_events
                .memory_evidence_unreadable(failure, executor_id);
        }
    }

    /// The sink every runner lifecycle fact is recorded through.
    pub fn runner_events(&self) -> &runner_events::RunnerEvents {
        &self.runner_events
    }

    /// This runner process's journal identity, once its journals attached.
    pub fn runner_incarnation(&self) -> Option<&str> {
        self.runner_incarnation.get().map(String::as_str)
    }

    /// What went wrong the last time the queue tried to make itself durable.
    pub fn queue_journal_error(&self) -> Option<String> {
        self.queue.journal_error()
    }
}

/// Past this, waiting for or holding the connection table, or persisting the
/// residency routes, is logged.
const SLOW_CONNECTION_LOCK: Duration = Duration::from_millis(100);

/// The connection table, held. Logs on release if it was held past
/// [`SLOW_CONNECTION_LOCK`].
struct ConnectionsGuard<'a> {
    guard: MutexGuard<'a, HashMap<String, ExecutorConnectionState>>,
    acquired: std::time::Instant,
    caller: &'static std::panic::Location<'static>,
}

impl std::ops::Deref for ConnectionsGuard<'_> {
    type Target = HashMap<String, ExecutorConnectionState>;
    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl std::ops::DerefMut for ConnectionsGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}

impl Drop for ConnectionsGuard<'_> {
    fn drop(&mut self) {
        note_slow_section(
            "lock_connections hold",
            self.caller,
            self.acquired.elapsed(),
        );
    }
}

/// Log a section that took longer than [`SLOW_CONNECTION_LOCK`], rate-limited
/// per section and caller.
fn note_slow_section(section: &str, caller: &std::panic::Location<'_>, elapsed: Duration) {
    if elapsed < SLOW_CONNECTION_LOCK {
        return;
    }
    static SLOW_SECTIONS: std::sync::LazyLock<
        Mutex<crate::repeat_report::KeyedRepeatReporter<String>>,
    > = std::sync::LazyLock::new(|| {
        Mutex::new(crate::repeat_report::KeyedRepeatReporter::new(
            Duration::from_secs(60),
        ))
    });
    let key = format!("{section} at {}:{}", caller.file(), caller.line());
    let line = format!("slow fleet section: {key} took {}ms", elapsed.as_millis());
    if let Some(line) = SLOW_SECTIONS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .record(std::time::Instant::now(), key.clone(), "slow", &line)
    {
        log::warn!("{line}");
    }
}

#[derive(Clone)]
struct ExecutorConnectionState {
    identity: ExecutorIdentity,
    /// Shared for the reason `snapshot` is: every placement pass copies the
    /// connection table, and each copy would otherwise clone every machine's
    /// capabilities and scratch replicas.
    advertisement: Arc<ExecutorAdvertisement>,
    /// The warm roots this link's Hello carried, moved by each change frame
    /// since. Shared for the reason `advertisement` is.
    warm_roots: Arc<warm_roots::WarmRootSet>,
    /// Whether this runner has asked the executor for its whole warm-root
    /// inventory and not yet had it, so a run of deltas against a base it
    /// does not hold asks once rather than once per delta.
    warm_roots_resync_requested: bool,
    generation: u64,
    sender: mpsc::UnboundedSender<ExecutorMessage>,
    /// Shared rather than owned so a placement pass can copy the connection
    /// table under its lock by bumping refcounts. A snapshot runs to hundreds
    /// of KB, and the transport's ingest path waits on that same lock.
    snapshot: Arc<FleetSnapshot>,
    /// Snapshots this link has delivered. An executor holds at most one
    /// outbound snapshot, superseded rather than queued, so the second one
    /// received after any moment was built after that moment: this count is
    /// how a reader proves an advertisement is causally fresh.
    snapshots_received: u64,
    /// Grants observed on this connection's ordered stream, kept through
    /// completion-only accounts until the attempt is fully absent.
    witnessed_grants: HashSet<RequestIdentity>,
    last_progress_unix_ms: u64,
    health: Arc<ExecutorSubstrateReport>,
    /// Whether this executor publishes [`MachineSample`]s. Once it has, no
    /// write of `health` moves a machine reading backwards: a snapshot or beat
    /// built by a refresh pass that waited on pool state carries readings older
    /// than the samples that arrived while it waited. An executor that has
    /// never sent one has its health replaced exactly as it posts it.
    publishes_machine_samples: bool,
    /// What the executor's latest sample said its running work is using,
    /// joined onto the rows of `snapshot` whenever the snapshot is read rather
    /// than written into it, which would clone the whole snapshot every sample.
    live_usage: load::LiveUsageIndex,
    executor_build_id: Option<String>,
    last_update: Option<UpdateReport>,
    colocated: bool,
    /// Whether the runner is holding this machine drained. Stamped from the
    /// durable set when the connection is made and whenever a drain is written,
    /// so the placement gate reads one field on the candidate rather than
    /// reaching for fleet state it would have to lock separately.
    drained: bool,
    drain_blockers: Vec<cairn_common::executor_protocol::DrainBlocker>,
    drain_state: cairn_common::executor_protocol::ExecutorDrainState,
    /// Whether the self-update rollout is emptying this machine so it can be
    /// replaced. Stamped by every rollout pass and on attach, for the reason
    /// `drained` is, and cleared by the pass that ends its turn.
    update_hold: bool,
    /// What an operator holds this machine to be for. Stamped from
    /// [`Fleet::machine_roles`] on attach and whenever the roles are
    /// re-adopted, for the same reason `drained` is.
    machine_role: MachineRole,
    /// The models placement holds resident on this machine, stamped from
    /// [`Fleet::model_residents`] on attach and by every model pass that
    /// changes them, so placement weighs their room under the table lock.
    model_residents: Vec<MachineModelResident>,
    /// Projects this machine recorded as unpreparable (a setup command not
    /// found), by project id. Stamped from [`Fleet::preparation_refusals`] on
    /// attach, keeping only refusals recorded against the environment this
    /// connection advertises, so placement reads one field on the candidate.
    preparation_refusals: HashMap<String, preparation::PreparationRefusal>,
    /// This link's reader and ingest stamps, written by the transport and
    /// judged here. See [`link_clock`] for why the judge's own presence is
    /// part of the evidence.
    link_clock: Arc<link_clock::LinkClock>,
    /// The network path this connection took, when the runner's executor
    /// listener accepted it.
    link_path: Option<cairn_common::executor_protocol::ExecutorLinkPath>,
    /// The room this link last announced (cairn/7303), as shown on its page.
    /// The dispatcher holds its own copy, less what it has assigned since.
    /// `None` until the link announces.
    room: Option<Arc<cairn_common::executor_protocol::RoomAnnouncement>>,
}

impl ExecutorConnectionState {
    /// The work this connection posted, with what each running command and
    /// resident process is using joined on from its latest sample.
    fn occupancy(&self) -> FleetSnapshot {
        let mut snapshot = (*self.snapshot).clone();
        self.live_usage.join_onto(&mut snapshot);
        snapshot
    }

    /// `health` as this connection should hold it in place of what it holds
    /// now: carrying forward every machine reading a sample already brought
    /// that is newer than the one `health` measured.
    fn carry_sampled_readings(
        &self,
        mut health: ExecutorSubstrateReport,
    ) -> ExecutorSubstrateReport {
        if self.publishes_machine_samples {
            health.adopt_newer_readings(&MachineSample::of_report(&self.health));
        }
        health
    }
}

impl Fleet {
    /// The executor connection table, recovered if an earlier holder panicked.
    ///
    /// Every write to it is a single step: an entry is inserted or removed
    /// whole, or its fields are overwritten together from one executor report
    /// with nothing that can fail between the first write and the last. A
    /// panic while the table is held therefore leaves no half-applied state,
    /// and the next report from each executor overwrites whatever it
    /// observed. Propagating the poison instead turns one request's panic into
    /// a panic in every later placement, every fleet read, and every
    /// executor's snapshot apply, which closes its link; and a destructor that
    /// reaches the table while a poisoned acquisition is unwinding aborts the
    /// process outright. Keep new critical sections single-step, or this
    /// recovery stops being sound.
    /// The connection table, timed.
    ///
    /// The transport's ingest path takes this lock for every beat and snapshot,
    /// so a caller that waits long for it, or holds it long, is a candidate for
    /// why a link's ingest fell behind. Both are logged past
    /// [`SLOW_CONNECTION_LOCK`] with the caller's location, at most once a
    /// minute per location with a count of the rest.
    #[track_caller]
    fn lock_connections(&self) -> ConnectionsGuard<'_> {
        let caller = std::panic::Location::caller();
        let waiting = std::time::Instant::now();
        let guard = self.connections.lock().unwrap_or_else(|poisoned| {
            log::error!(
                "executor connection table was held by a task that panicked; recovering it, the panic above is the defect"
            );
            self.connections.clear_poison();
            poisoned.into_inner()
        });
        let acquired = std::time::Instant::now();
        note_slow_section("lock_connections wait", caller, acquired - waiting);
        ConnectionsGuard {
            guard,
            acquired,
            caller,
        }
    }

    fn occupancy_for_at(
        &self,
        scope: PlacementScope<'_>,
        now_unix_ms: u64,
    ) -> occupancy::MachineOccupancy {
        let connections = self.lock_connections();
        connections
            .values()
            .filter(|connection| candidate_rejection(connection, scope, now_unix_ms).is_none())
            .map(|connection| {
                // The relief estimate is the machine's own posted answer rather
                // than a forecast computed here over what it said it was
                // running. It knows when its work started; this process knew
                // only when it dispatched.
                occupancy::MachineOccupancy::read(
                    &connection.snapshot.executing_requests,
                    connection.health.predicted_relief_ms,
                )
            })
            .reduce(occupancy::MachineOccupancy::or_earlier)
            .unwrap_or(occupancy::MachineOccupancy::Unforecastable)
    }

    pub fn set_expected_executor_build_id(&self, executor_id: impl Into<String>, build_id: String) {
        self.expected_executor_build_ids
            .lock()
            .unwrap()
            .insert(executor_id.into(), build_id);
    }

    /// Record the executor build this runner serves for `target`, or why it
    /// has none. Written by the self-update driver, which resolves and hashes
    /// the artifact off the fleet's locks.
    pub fn record_executor_artifact_build(
        &self,
        target: &str,
        build: Result<String, ArtifactUnavailable>,
    ) {
        self.executor_artifact_builds.lock().unwrap().insert(
            target.to_string(),
            match build {
                Ok(build_id) => rollout::ArtifactBuild::Resolved(build_id),
                Err(unavailable) => rollout::ArtifactBuild::Unavailable(unavailable),
            },
        );
    }

    /// The artifact targets the self-update driver has to resolve: those the
    /// attached remotes run, and those every enrolled machine runs whether or
    /// not it is attached. The second half is what a machine refused at hello
    /// needs, because a refused machine never attaches, and a runner that
    /// only resolved attached targets could not say it had nothing to repair
    /// that machine with.
    pub fn remote_executor_targets(&self) -> std::collections::BTreeSet<&'static str> {
        let mut targets: std::collections::BTreeSet<&'static str> = self
            .lock_connections()
            .values()
            .filter(|entry| !entry.colocated)
            .filter_map(executor_artifact_target)
            .collect();
        targets.extend(
            self.enrolled_remotes
                .lock()
                .unwrap()
                .values()
                .filter_map(|record| enrolled_artifact_target(&record.os, &record.arch)),
        );
        targets
    }

    /// Record that an enrolled machine dialled in speaking `received` and was
    /// refused for not speaking `expected`, and answer whether that leaves it
    /// stranded: refused, and with no artifact this runner could repair it
    /// with. `None` for a machine this runner has not enrolled, and for one a
    /// self-update can still repair (or whose target is not yet resolved).
    pub fn record_protocol_refusal(
        &self,
        executor_id: &str,
        expected: u32,
        received: u32,
    ) -> Option<cairn_common::executor_protocol::StrandedExecutor> {
        let (run, target) = {
            let mut enrolled = self.enrolled_remotes.lock().unwrap();
            let record = enrolled.get_mut(executor_id)?;
            let run = ProtocolRefusalRun::extended(
                record.protocol_refusal,
                expected,
                received,
                unix_time_ms(),
            );
            record.protocol_refusal = Some(run);
            (run, enrolled_artifact_target(&record.os, &record.arch))
        };
        let artifacts = self.executor_artifact_builds.lock().unwrap();
        run.stranding(target?, &artifacts)
    }

    /// One pass of the rolling self-update (see [`rollout`]): stamp the holds
    /// placement reads (the machine updating and the one draining), send the
    /// directive once the machine whose turn it is has emptied, and journal
    /// what changed.
    ///
    /// Each lock is taken alone. The queue and the pending-residency map are
    /// read between two acquisitions of the connection table rather than
    /// under it, and the rollout decides between them, so no lock here is ever
    /// held across another.
    ///
    /// Returns the notices residents' owners are owed, which the caller
    /// delivers: telling an owner reaches the database and the messaging
    /// path, neither of which this pass may hold a lock across.
    pub(crate) fn advance_self_update_rollout(
        &self,
        now_unix_ms: u64,
        persist: impl FnOnce(&[rollout::SelfUpdateDrain]) -> Result<(), String>,
    ) -> Result<Vec<rollout::OwnerNotice>, String> {
        let _pass = self.self_update_runtime.pass.lock().unwrap();
        let artifacts = self.executor_artifact_builds.lock().unwrap().clone();
        let mut candidates: Vec<rollout::Candidate> = self
            .lock_connections()
            .values()
            .filter(|entry| !entry.colocated)
            .map(|entry| {
                let mut candidate = rollout_candidate(entry, now_unix_ms);
                (candidate.work, candidate.services, candidate.residents) = snapshot_work(entry);
                candidate
            })
            .collect();
        let mut pending_residency: HashMap<String, Vec<String>> = HashMap::new();
        for (correlation_id, pending) in self.pending_residency.lock().unwrap().iter() {
            pending_residency
                .entry(pending.executor_id.clone())
                .or_default()
                .push(correlation_id.clone());
        }
        for candidate in &mut candidates {
            let bound = self.queue.identities_bound_to(&candidate.executor_id);
            // A terminal result remains delivery debt until its durable
            // disposition authorizes ACK, even after the execution queue ends.
            let mut bound = bound;
            bound.extend(
                self.check_claims_for_machine(&candidate.executor_id)
                    .into_iter()
                    .map(|(identity, _)| identity),
            );
            join_runner_work(
                &mut candidate.work,
                &bound,
                pending_residency
                    .get(&candidate.executor_id)
                    .map(Vec::as_slice)
                    .unwrap_or_default(),
            );
        }
        let mut proposed = self.self_update.lock().unwrap().clone();
        let step = proposed.advance(&candidates, &artifacts, now_unix_ms);
        // No connection or rollout lock crosses persistence. A failed commit
        // leaves both the applied schedule and its existing holds untouched.
        persist(&proposed.required_drains())?;
        let refusal_events = proposed.record_committed_drains(unix_time_ms().max(now_unix_ms));
        *self.self_update.lock().unwrap() = proposed;
        for (executor_id, detail) in refusal_events {
            self.runner_events.record(
                runner_events::RunnerEvent::new(runner_events::RunnerEventKind::ExecutorSelfUpdate)
                    .executor(executor_id)
                    .cause("committed self-update admission refusal transition")
                    .detail(detail),
            );
        }
        let mut released = false;
        let delivered = {
            let mut connections = self.lock_connections();
            for (executor_id, entry) in connections.iter_mut() {
                let held = step.holds.contains(executor_id);
                released |= entry.update_hold && !held;
                entry.update_hold = held;
            }
            step.direct.as_ref().map(|directive| {
                connections
                    .get(&directive.executor_id)
                    .filter(|entry| entry.generation == directive.generation)
                    .is_some_and(|entry| {
                        entry
                            .sender
                            .send(ExecutorMessage::UpdateDirected {
                                manifest_url:
                                    cairn_common::executor_protocol::EXECUTOR_ARTIFACT_MANIFEST_PATH
                                        .into(),
                                build_id: directive.build_id.clone(),
                            })
                            .is_ok()
                    })
            })
        };
        // Placement waits on a machine being emptied for its update, so the
        // hold's end is what offers it the waiting work.
        if released {
            self.connection_ready.notify_waiters();
            self.machine_facts_posted_all();
        }
        if let (Some(directive), Some(false)) = (&step.direct, delivered) {
            log::info!(
                "self-update directive for executor_id={} did not reach generation {}; draining it again",
                directive.executor_id,
                directive.generation
            );
            self.self_update
                .lock()
                .unwrap()
                .directive_undelivered(&directive.executor_id, now_unix_ms);
        }
        for transition in step.transitions {
            log::info!(
                "executor self-update {} executor_id={} build={}: {}",
                transition.kind.as_str(),
                transition.executor_id,
                transition.build_id,
                transition.cause
            );
            let mut detail = serde_json::json!({
                "phase": transition.kind.as_str(),
                "build": transition.build_id,
            });
            if let Some(deferrals) = transition.deferrals {
                detail["deferrals"] = serde_json::json!(deferrals);
            }
            self.runner_events.record(
                runner_events::RunnerEvent::new(runner_events::RunnerEventKind::ExecutorSelfUpdate)
                    .executor(transition.executor_id)
                    .cause(transition.cause)
                    .detail(detail),
            );
        }
        self.note_unreturned(step.unreturned, now_unix_ms);
        Ok(step.notices)
    }

    /// Record what delivering one resident's notice came to, so the machine's
    /// page says whether its owner was told.
    pub(crate) fn note_resident_notice(
        &self,
        notice: &rollout::OwnerNotice,
        outcome: cairn_common::executor_protocol::ResidentNotice,
    ) {
        self.self_update.lock().unwrap().note_resident_notice(
            &notice.executor_id,
            &notice.operation_id,
            &notice.resident.key,
            notice.kind,
            outcome,
        );
    }

    /// Fill each self-update refusal with when its machine is expected back,
    /// so a parked request can say when its candidate returns.
    fn stamp_update_bounds(&self, rejected: &mut [PlacementRejection]) {
        if !rejected.iter().any(|rejection| {
            matches!(
                rejection.reason,
                PlacementRejectionReason::UpdatePending { .. }
            )
        }) {
            return;
        }
        let rollout = self.self_update.lock().unwrap();
        for rejection in rejected {
            if let PlacementRejectionReason::UpdatePending { returns_by_unix_ms } =
                &mut rejection.reason
            {
                *returns_by_unix_ms = rollout
                    .returns_by(&rejection.executor_id)
                    .or(*returns_by_unix_ms);
            }
        }
    }

    fn note_unreturned(&self, unreturned: Vec<rollout::Unreturned>, now_unix_ms: u64) {
        for conclusion in unreturned {
            self.conclude_unreturned_update(
                &conclusion.executor_id,
                &conclusion.build_id,
                conclusion.directed_at_unix_ms,
                now_unix_ms,
            );
        }
    }

    /// Record that this machine, directed to install `build_id`, did not
    /// return: on its enrolment record, where the fleet page reads it, and in
    /// the queue its bootstrap holder restarts it from. `false` for a machine
    /// this runner has not enrolled, which has no record or bootstrap.
    pub fn conclude_unreturned_update(
        &self,
        executor_id: &str,
        build_id: &str,
        directed_at_unix_ms: u64,
        now_unix_ms: u64,
    ) -> bool {
        let recovery = {
            let mut enrolled = self.enrolled_remotes.lock().unwrap();
            let Some(record) = enrolled.get_mut(executor_id) else {
                return false;
            };
            let update = cairn_common::executor_protocol::UnreturnedUpdate {
                build_id: build_id.to_string(),
                directed_at_unix_ms,
                concluded_at_unix_ms: now_unix_ms,
                restart: None,
            };
            record.unreturned_update = Some(update.clone());
            unreturned::UpdateRecovery {
                executor_id: executor_id.to_string(),
                name: record.name.clone(),
                last_seen_unix_ms: record.last_seen_unix_ms,
                update,
            }
        };
        self.update_recoveries.push(recovery);
        true
    }

    /// The strandings queued since the last call, for the holder of the
    /// machines' bootstrap.
    pub fn take_update_recoveries(&self) -> Vec<unreturned::UpdateRecovery> {
        self.update_recoveries.take()
    }

    /// Signalled whenever a stranding is queued.
    pub fn update_recovery_arrived(&self) -> Arc<tokio::sync::Notify> {
        self.update_recoveries.arrived()
    }

    /// Record the restart made for a machine that did not return from the
    /// update to `build_id`, on its page and in the runner journal. `false`
    /// when the machine no longer stands unreturned from that build: it
    /// attached meanwhile, or it was removed.
    pub fn record_update_restart(
        &self,
        executor_id: &str,
        build_id: &str,
        restart: cairn_common::executor_protocol::UpdateRestart,
    ) -> bool {
        let recorded = self
            .enrolled_remotes
            .lock()
            .unwrap()
            .get_mut(executor_id)
            .and_then(|record| record.unreturned_update.as_mut())
            .filter(|update| update.build_id == build_id)
            .map(|update| update.restart = Some(restart.clone()))
            .is_some();
        if let Some(outcome) = &restart.outcome {
            log::info!(
                "executor self-update restart executor_id={executor_id} build={build_id}: {outcome}"
            );
            self.runner_events.record(
                runner_events::RunnerEvent::new(runner_events::RunnerEventKind::ExecutorSelfUpdate)
                    .executor(executor_id)
                    .cause(unreturned::restart_text(&restart))
                    .detail(serde_json::json!({
                        "phase": "restarted",
                        "build": build_id,
                    })),
            );
        }
        recorded
    }

    /// The build each attached executor should be running: the one the
    /// supervisor launched for the colocated executor, and the one this runner
    /// serves for each remote's target.
    fn expected_build_ids(
        &self,
        connections: &HashMap<String, ExecutorConnectionState>,
    ) -> HashMap<String, String> {
        let mut expected = self.expected_executor_build_ids.lock().unwrap().clone();
        let artifacts = self.executor_artifact_builds.lock().unwrap();
        for (executor_id, entry) in connections.iter().filter(|(_, entry)| !entry.colocated) {
            if let Some(rollout::ArtifactBuild::Resolved(build_id)) =
                executor_artifact_target(entry).and_then(|target| artifacts.get(target))
            {
                expected.insert(executor_id.clone(), build_id.clone());
            }
        }
        expected
    }

    /// The OS-confinement regime for a project-check batch, at either cadence.
    ///
    /// Declared checks run **unconfined, with host permissions**. A check command
    /// is not agent input that has to earn trust: it comes from the `checks`
    /// contract in the project's live main checkout
    /// (`execution::checks::load_live_project_checks`), which a branch cannot edit
    /// for its own run. That is the same trust decision
    /// [`crate::config::check_exemption`] makes when an agent types a declared
    /// check into `run`; there the command must be matched back to the contract,
    /// while a cadence holds the declaration itself.
    ///
    /// Confining these batches does not contain them, it breaks them. macOS
    /// sandboxes do not nest, so a confined suite exits 71 the moment a test
    /// spawns its own `sandbox-exec`, and the `CAIRN_SANDBOXED=1` that rides
    /// along with a policy makes fence-sensitive tests self-skip — a lane that is
    /// structurally red on every branch and says nothing about the tree
    /// (CAIRN-3124). Containment of what a batch may *publish* is a separate
    /// knob: [`MutationPolicy`] owns it, and review stays `PureVerdict`.
    ///
    /// What a check runs is still the branch's code, so on a machine holding an
    /// operator credential the executor applies that boundary alone
    /// (`BranchCode`); a suite there cannot exercise its own nested sandbox,
    /// which is what the skip manifest's live tests record.
    const CHECK_CADENCE_SANDBOX_MODE: ProcessSandboxMode = ProcessSandboxMode::BranchCode;

    /// How long a write-cadence check's process tree may show no output and no
    /// CPU progress before its executor stops it as stalled.
    ///
    /// A bound on evidence, not on duration: a check that keeps printing or
    /// keeps computing never meets it however long it runs, and it is never
    /// derived from a duration estimate or a default timeout. It is generous
    /// because legitimate quiet work exists — a long link step, a build that
    /// prints nothing until it finishes — and short enough that a wedged test
    /// stops holding a cell within the working session that started it.
    pub(crate) const WRITE_CHECK_SILENCE_BOUND_MS: u64 = 15 * 60 * 1_000;
    /// The same bound for the review cadence, whose fuller suites include the
    /// longest quiet phases.
    pub(crate) const REVIEW_CHECK_SILENCE_BOUND_MS: u64 = 30 * 60 * 1_000;
}

// Every test module stays declared here under its pre-split name, whatever
// file holds it: the per-test outcome ledger and open incidents key tests by
// their full module path, so moving a declaration renames its tests.
#[cfg(test)]
mod process_fit_tests;

/// The published artifact target a machine runs, from what it advertises.
fn executor_artifact_target(entry: &ExecutorConnectionState) -> Option<&'static str> {
    let capabilities = &entry.advertisement.capabilities;
    cairn_common::executor_protocol::RemotePlatform::from_os_arch(
        &capabilities.os.to_ascii_lowercase(),
        &capabilities.arch.to_ascii_lowercase(),
    )
    .map(cairn_common::executor_protocol::RemotePlatform::target)
}

/// A remote as the self-update rollout reads it. `work` and `services` are
/// filled in by the rollout pass, which alone can read the runner's queue for
/// it; a status read needs neither.
fn rollout_candidate(entry: &ExecutorConnectionState, now_unix_ms: u64) -> rollout::Candidate {
    rollout::Candidate {
        executor_id: entry.identity.executor_id.clone(),
        name: executor_public_name(entry),
        generation: entry.generation,
        target: executor_artifact_target(entry),
        running_build: entry.executor_build_id.clone(),
        last_update: entry.last_update.clone(),
        work: Vec::new(),
        services: Vec::new(),
        residents: Vec::new(),
        snapshots_received: entry.snapshots_received,
        reachable: !entry.sender.is_closed()
            && matches!(
                judge_link(entry, now_unix_ms, EXECUTOR_PROGRESS_FRESHNESS_MS),
                link_clock::LinkJudgement::Fresh
            ),
    }
}

/// The public address of a connected executor.
///
/// Derived rather than stored on the connection so one rule decides it
/// everywhere: the runner's own executor answers to the reserved name, and every
/// enrolled machine answers to the normalization of the label it advertises. An
/// advertisement whose label normalizes to nothing falls back to its identity,
/// so a machine always has an address rather than vanishing from the fleet.
fn executor_public_name(entry: &ExecutorConnectionState) -> String {
    if entry.colocated {
        return LOCAL_EXECUTOR_NAME.to_string();
    }
    normalize_executor_name(&entry.identity.display_name)
        .or_else(|| normalize_executor_name(&entry.identity.executor_id))
        .unwrap_or_else(|| entry.identity.executor_id.clone())
}

pub(crate) fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// The instant every fleet fixture is stamped at, so a test's machines, their
/// telemetry, and the placement clock reading them all agree.
#[cfg(test)]
const NOW: u64 = 1_000_000_000;

#[cfg(test)]
mod tests;

#[cfg(all(test, unix))]
mod bench;

#[cfg(test)]
mod loop_regressions;

#[cfg(test)]
mod dispatcher_properties;

/// A fence wait relayed from an executor batch, end to end through
/// [`Fleet::handle_runner_callback`]: what happens to the permission and the
/// turn when the batch context closes before the inline budget, and when it
/// does not (cairn/4937).
#[cfg(test)]
mod handoff_tests;
