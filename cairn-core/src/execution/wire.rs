//! Runner-to-executor payload construction for check execution.
//!
//! This module is the physical trust boundary for executor wire behavior. Check
//! planning and cache policy stay in checks.rs; translating those plans into
//! CellRequest and ProcessBatchItem values happens only here.

use crate::config::project_settings::{CheckCommand, CheckResourceClass};
use crate::execution::checks::{PlannedCheckBatchItem, PlannedCheckBatchRequest};
use crate::fleet::{
    CellPriority, CellRequest, CommandResourceIdentity, MutationPolicy, PureVerdictBatchItem,
};
use sha2::{Digest, Sha256};

#[derive(Clone)]
pub(crate) enum CheckResourceIdentityInput {
    Configured { name: String, check: CheckCommand },
    Checkpoint { name: String, command: String },
}

fn resource_identity(input: &CheckResourceIdentityInput) -> CommandResourceIdentity {
    let key = match input {
        CheckResourceIdentityInput::Configured { name, check } => {
            configured_check_resource_key(name, check)
        }
        CheckResourceIdentityInput::Checkpoint { name, command } => {
            let mut hasher = Sha256::new();
            hasher.update(name.as_bytes());
            hasher.update([0]);
            hasher.update(command.as_bytes());
            format!("{:x}", hasher.finalize())
        }
    };
    CommandResourceIdentity {
        version: cairn_common::executor_protocol::COMMAND_RESOURCE_IDENTITY_VERSION,
        key,
    }
}

#[cfg(test)]
pub(crate) fn check_resource_identity(name: &str, check: &CheckCommand) -> CommandResourceIdentity {
    resource_identity(&CheckResourceIdentityInput::Configured {
        name: name.to_string(),
        check: check.clone(),
    })
}

fn configured_check_resource_key(name: &str, check: &CheckCommand) -> String {
    fn field(hasher: &mut Sha256, value: &str) {
        hasher.update(value.len().to_be_bytes());
        hasher.update(value.as_bytes());
    }
    fn option(hasher: &mut Sha256, value: Option<&str>) {
        field(hasher, value.unwrap_or("<none>"));
    }
    fn strings(hasher: &mut Sha256, values: Option<&[String]>) {
        let mut values = values.unwrap_or_default().to_vec();
        values.sort();
        field(hasher, &values.len().to_string());
        for value in values {
            field(hasher, &value);
        }
    }

    let mut hasher = Sha256::new();
    field(&mut hasher, "check-resource-v1");
    field(&mut hasher, name);
    field(&mut hasher, &check.command);
    strings(&mut hasher, check.impact.as_deref());
    field(&mut hasher, check.policy.as_str());
    field(&mut hasher, check.when.as_str());
    field(&mut hasher, check.resource_class.as_str());
    option(
        &mut hasher,
        check.timeout.map(|value| value.to_string()).as_deref(),
    );
    if let Some(selector) = check.executor.as_ref() {
        field(&mut hasher, "executor");
        option(&mut hasher, selector.name.as_deref());
        option(&mut hasher, selector.os.as_deref());
        strings(&mut hasher, Some(&selector.required_toolchains));
    } else {
        field(&mut hasher, "no-executor");
    }
    format!("{:x}", hasher.finalize())
}
use cairn_common::executor_protocol::{
    CellCommandClass, CellOwnerRef, ExecutorSelector, PlacementMobility, PlacementWorkClass,
    ProcessBatchExecution, ProcessBatchItem, RepositoryLocator, ResourceReservation,
    ResourceReservationSource,
};

/// Elapsed time cannot evict a parked check. Requester liveness and explicit
/// cancellation still remove abandoned or no-longer-wanted entries.
const PARKED_CHECK_WAIT_HORIZON_UNIX_MS: u64 = u64::MAX;

pub(crate) fn durable_check_request(build: impl FnOnce(u64) -> CellRequest) -> CellRequest {
    build(PARKED_CHECK_WAIT_HORIZON_UNIX_MS)
}

/// Whether a planned check batch is free for placement policy to move.
///
/// A pure-verdict batch is disposable and publishes a verdict from managed
/// objects wherever it lands. A write-cadence batch mutates one shared working
/// tree, so it and its returned delta remain tied to the colocated executor.
/// Mobility is stated from mutation policy, never inferred from the absence of
/// a selector; platform eligibility and mobility are independent facts.
pub(crate) fn batch_placement_mobility(policy: &MutationPolicy) -> PlacementMobility {
    match policy {
        MutationPolicy::PureVerdict => PlacementMobility::SpillEligible,
        MutationPolicy::AllowDelta => PlacementMobility::PinnedOrColocated,
    }
}

pub(crate) struct CheckBatchWire {
    pub(crate) request: CellRequest,
    pub(crate) items: Vec<PureVerdictBatchItem>,
}

pub(crate) fn build_check_batch_wire(
    batch: &PlannedCheckBatchRequest,
    request_id: String,
    attempt_id: String,
    executor: Option<ExecutorSelector>,
    verdict_platforms: Vec<String>,
) -> CheckBatchWire {
    let timeout_ms = batch
        .items
        .iter()
        .fold(0_u64, |sum, item| sum.saturating_add(item.timeout_ms));
    let command = batch
        .items
        .iter()
        .map(|item| item.name.as_str())
        .collect::<Vec<_>>()
        .join(" · ");
    let request = durable_check_request(|wait_horizon_unix_ms| CellRequest {
        request_id,
        attempt_id,
        project_id: batch.project_id.clone(),
        repository: RepositoryLocator::ColocatedPath {
            project_id: batch.project_id.clone(),
            repository_id: batch.project_id.clone(),
            absolute_path: batch.repository.clone(),
        },
        base_commit: batch.sealed_commit.clone(),
        command_class: batch_command_class(&batch.items),
        placement_work_class: match batch.priority {
            CellPriority::ReviewCheck => PlacementWorkClass::ReviewChecks,
            CellPriority::WriteCheck | CellPriority::AgentInteractive => {
                PlacementWorkClass::WriteChecks
            }
        },
        command,
        owner: Some(batch.owner.clone()),
        cwd: String::new(),
        env: batch.env.clone(),
        priority: batch.priority,
        wait_horizon_unix_ms,
        waiting_since_unix_ms: unix_time_ms(),
        timeout_ms,
        mutation_policy: batch.mutation_policy.clone(),
        requesting_job_id: Some(batch.requesting_job_id.clone()),
        affinity_key: batch.affinity_key.clone(),
        executor,
        pinned_executor_id: None,
        placement_mobility: batch_placement_mobility(&batch.mutation_policy),
        verdict_platforms,
        command_resource_identity: None,
        resource_reservation: declared_batch_reservation(&batch.items),
        learned_estimate: None,
    });
    let items = batch
        .items
        .iter()
        .map(|item| PureVerdictBatchItem {
            result_identity: crate::execution::cache::CheckResultIdentity::new(
                &batch.project_id,
                &item.name,
                &item.input_hash,
            ),
            process: ProcessBatchItem {
                header: item.name.clone(),
                stream_id: item.stream_id.clone(),
                execution: ProcessBatchExecution::Direct,
                program: "bash".into(),
                args: vec!["-c".into(), item.command.clone()],
                env: item.env.clone(),
                stdin: None,
                timeout_ms: item.timeout_ms,
                command_resource_identity: Some(resource_identity(&item.resource_identity)),
                verdict_environment_names: item.verdict_environment_names.clone(),
            },
        })
        .collect();
    CheckBatchWire { request, items }
}

pub(crate) struct ReviewCheckWireInput {
    pub(crate) request_id: String,
    pub(crate) attempt_id: String,
    pub(crate) project_id: String,
    pub(crate) repository: String,
    pub(crate) base_commit: String,
    pub(crate) command: String,
    pub(crate) owner: Option<CellOwnerRef>,
    pub(crate) env: Vec<(String, String)>,
    pub(crate) priority: CellPriority,
    pub(crate) timeout_ms: u64,
    pub(crate) requesting_job_id: String,
    pub(crate) executor: Option<ExecutorSelector>,
    pub(crate) verdict_platforms: Vec<String>,
    pub(crate) resource_identity: CheckResourceIdentityInput,
    pub(crate) resource_class: CheckResourceClass,
}

pub(crate) fn build_review_check_request(input: ReviewCheckWireInput) -> CellRequest {
    durable_check_request(|wait_horizon_unix_ms| CellRequest {
        request_id: input.request_id,
        attempt_id: input.attempt_id,
        project_id: input.project_id.clone(),
        repository: RepositoryLocator::ColocatedPath {
            project_id: input.project_id.clone(),
            repository_id: input.project_id.clone(),
            absolute_path: input.repository,
        },
        base_commit: input.base_commit,
        command_class: CellCommandClass::classify(&input.command),
        placement_work_class: PlacementWorkClass::ReviewChecks,
        command: input.command,
        owner: input.owner,
        cwd: String::new(),
        env: input.env,
        priority: input.priority,
        wait_horizon_unix_ms,
        waiting_since_unix_ms: unix_time_ms(),
        timeout_ms: input.timeout_ms,
        mutation_policy: MutationPolicy::PureVerdict,
        requesting_job_id: Some(input.requesting_job_id),
        affinity_key: None,
        executor: input.executor,
        pinned_executor_id: None,
        placement_mobility: batch_placement_mobility(&MutationPolicy::PureVerdict),
        verdict_platforms: input.verdict_platforms,
        command_resource_identity: Some(resource_identity(&input.resource_identity)),
        resource_reservation: declared_check_reservation(input.resource_class),
        learned_estimate: None,
    })
}

/// Compatibility reservation for a configured check.
///
/// Memory and disk are resolved from measured command profiles. CPU is a
/// compressible resource and therefore carries no admission charge, regardless
/// of the legacy resource class value.
pub(crate) fn declared_check_reservation(
    _resource_class: CheckResourceClass,
) -> ResourceReservation {
    ResourceReservation {
        memory_bytes: 0,
        disk_growth_bytes: 0,
        concurrency_units: 0,
        source: ResourceReservationSource::Declared,
    }
}

/// Compatibility reservation for a batch. Resource classes no longer affect
/// admission, so every composition has the same zero CPU charge.
pub(crate) fn declared_batch_reservation(items: &[PlannedCheckBatchItem]) -> ResourceReservation {
    let compatibility_class = if items
        .iter()
        .any(|item| item.resource_class == CheckResourceClass::Exclusive)
    {
        CheckResourceClass::Exclusive
    } else {
        CheckResourceClass::Shared
    };
    declared_check_reservation(compatibility_class)
}

/// The command class of a batch is its heaviest item.
///
/// Classification must inspect commands rather than the display-only joined
/// check names, which do not match the learned resource-profile patterns.
pub(crate) fn batch_command_class(items: &[PlannedCheckBatchItem]) -> CellCommandClass {
    fn weight(class: CellCommandClass) -> u8 {
        match class {
            CellCommandClass::Other => 0,
            CellCommandClass::Typecheck => 1,
            CellCommandClass::Vitest => 2,
            CellCommandClass::Build => 3,
            CellCommandClass::CargoCheck => 4,
            CellCommandClass::CargoClippy => 5,
            CellCommandClass::CargoTest => 6,
        }
    }
    items
        .iter()
        .map(|item| CellCommandClass::classify(&item.command))
        .max_by_key(|class| weight(*class))
        .unwrap_or_default()
}

fn unix_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
