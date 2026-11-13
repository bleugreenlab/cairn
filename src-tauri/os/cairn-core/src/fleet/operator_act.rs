//! The fleet's operator-act path: how an accepted `fenced_command` reaches one
//! executor. It is an ordinary counted command under the same admission,
//! brakes and placement journal as any batch, differing only in what an
//! agent's batch can never ask for: a pin to the accepted machine, no path
//! fence, and a value only the request consumer can build.

use cairn_common::executor_protocol::{
    CellCommandClass, CellOutcome, CellPriority, CellRequest, ExecutorSelector, MutationPolicy,
    PlacementMobility, PlacementWorkClass, ProcessBatch, ProcessBatchExecution, ProcessBatchItem,
    ProcessSandboxMode, RepositoryLocator,
};
use cairn_common::run_contract::RUN_BATCH_CEILING_MS;

use super::{output_guard, unix_time_ms, Fleet, RunnerCallbackContext, RunnerContextEntry};
use crate::operator_requests::fenced_command::{credential_floor_envelope, OperatorAct};
use crate::orchestrator::Orchestrator;

/// The cell request an act becomes. Pinned to the accepted executor id, never
/// selected by name, so a rename cannot retarget it; a machine that cannot
/// serve refuses it by name rather than spilling it elsewhere.
pub(super) fn operator_act_request(act: &OperatorAct) -> CellRequest {
    let frozen = act.act();
    let now = unix_time_ms();
    CellRequest {
        request_id: act.cell_request_id().to_string(),
        attempt_id: act.cell_attempt_id().to_string(),
        project_id: frozen.project_id.clone(),
        repository: RepositoryLocator::ColocatedPath {
            project_id: frozen.project_id.clone(),
            repository_id: frozen.project_id.clone(),
            absolute_path: act.repository().to_string(),
        },
        base_commit: frozen.commit.clone(),
        publication: None,
        command_class: CellCommandClass::classify(&frozen.command),
        placement_work_class: PlacementWorkClass::AgentSessions,
        command: frozen.command.clone(),
        owner: None,
        cwd: frozen.cwd.clone(),
        env: vec![("CAIRN_WORKTREE_BRANCH".into(), frozen.branch.clone())],
        priority: CellPriority::AgentInteractive,
        wait_horizon_unix_ms: now.saturating_add(u64::from(RUN_BATCH_CEILING_MS)),
        waiting_since_unix_ms: now,
        timeout_ms: frozen.timeout_ms,
        // Tracked changes are discarded with the cell; what the act is for is
        // what it writes outside the checkout.
        mutation_policy: MutationPolicy::PureVerdict,
        requesting_job_id: None,
        affinity_key: None,
        // Placement may only choose a machine whose credential floor is
        // measured refused, and the executor re-reads that evidence at spawn.
        executor: Some(ExecutorSelector {
            containment: Some(credential_floor_envelope()),
            ..ExecutorSelector::default()
        }),
        pinned_executor_id: Some(frozen.executor_id.clone()),
        placement_mobility: PlacementMobility::PinnedOrColocated,
        verdict_platforms: Vec::new(),
        avoided_executor_ids: Vec::new(),
        project_requirements: Default::default(),
        command_resource_identity: None,
        resource_reservation: Default::default(),
        learned_estimate: None,
        scratch_sync: None,
        home_affinity: None,
    }
}

/// One `/bin/sh -c` item carrying the exact accepted bytes, outside the path
/// fence but under the operator-credential floor.
pub(super) fn operator_act_batch(act: &OperatorAct, runner_context_id: String) -> ProcessBatch {
    let frozen = act.act();
    ProcessBatch {
        files: Vec::new(),
        sequential: true,
        stop_on_error: true,
        sandbox_mode: ProcessSandboxMode::CredentialFloor,
        containment: Some(credential_floor_envelope()),
        timeout_bases: Vec::new(),
        items: vec![ProcessBatchItem {
            header: "fenced_command".into(),
            stream_id: format!("{}:item-0", act.cell_request_id()),
            execution: ProcessBatchExecution::Direct,
            program: "/bin/sh".into(),
            args: vec!["-c".into(), frozen.command.clone()],
            env: Vec::new(),
            stdin: None,
            // A declared bound, honoured as one; no stall stop is derived.
            timeout_ms: frozen.timeout_ms,
            silence_bound_ms: 0,
            command_resource_identity: None,
            verdict_environment_names: Vec::new(),
        }],
        runtime_packages: Vec::new(),
        runner_context_id: Some(runner_context_id),
        execution_residency: None,
        node_scratch: act.node_scratch().cloned(),
    }
}

impl Fleet {
    pub(crate) async fn submit_operator_act(
        &self,
        orch: &Orchestrator,
        act: &OperatorAct,
    ) -> CellOutcome {
        let runner_context_id = uuid::Uuid::new_v4().to_string();
        let output = output_guard::RelayedOutput::shared();
        self.runner_contexts.lock().unwrap().insert(
            runner_context_id.clone(),
            RunnerContextEntry::new(
                &runner_context_id,
                &format!("{}:{}", act.cell_request_id(), act.cell_attempt_id()),
                RunnerCallbackContext {
                    request: None,
                    run_context: None,
                    check_status_board: None,
                    run_batch_request_id: None,
                    live_checkout: false,
                    executor_binding: None,
                    output: output.clone(),
                },
            ),
        );
        let mut request = operator_act_request(act);
        let batch = operator_act_batch(act, runner_context_id.clone());
        let outcome = match self.prepare_execution(orch, &mut request).await {
            Ok(prepared) => self.execute_prepared(request, Some(batch), prepared).await,
            Err(outcome) => outcome,
        };
        output_guard::flush_batch_output(orch, &output, None);
        self.settle_runner_context(&runner_context_id);
        outcome
    }
}
