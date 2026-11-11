use super::*;
use crate::models::{ConfirmPolicy, OutputSchemaInfo};
use cairn_config::library::LibraryRoots;

// ============================================================================
// create_call_run
// ============================================================================

/// Create an ephemeral agent-call run: a node-less job+session+run started
/// directly (never via DAG advancement), carrying a per-run output contract and
/// carrying either the caller's inherited branch coordinate or no branch.
///
/// This is the call-primitive sibling of [`create_child_task`] (CAIRN-2481): the
/// same real-run shape (job, session, run, event stream, backend session), but
/// with a per-call branch policy, an enforced output schema, and durable
/// workflow tags. The spawn pipeline persists a pre-materialized `CallTool`
/// packet around it so the delegated-task wait/suspend/resume machinery is
/// reused unchanged.
///
/// Splitting `prepare` (rows + transcript seed) from [`start_call_run`] (the
/// backend session) lets the caller persist the call packet BETWEEN them, so a
/// packet always exists before the run can finalize — closing the fast-finish
/// resume race.
pub(crate) fn prepare_call_run(
    orch: &Orchestrator,
    input: CreateCallRunInput,
) -> Result<PreparedCallRun, String> {
    // Resolve the parent job's owning database (a team run lives in its replica).
    let db = run_db({
        let dbs = orch.db.clone();
        let parent_job_id = input.parent_job_id.clone();
        async move {
            crate::execution::routing::owning_db_for_job(&dbs, &parent_job_id)
                .await
                .map_err(|e| e.to_string())
        }
    })?;

    let parent_job = run_db(load_job(
        db.clone(),
        input.parent_job_id.clone(),
        "Parent job not found",
    ))?;
    let project_id = parent_job.project_id.clone();
    let issue_id = parent_job.issue_id.clone();
    let thread_id = parent_job.thread_id.clone();
    // The call job hangs off the parent's task-execution (created/ensured by the
    // spawn pipeline) so its snapshot carries the call packet.
    let execution_id = input
        .execution_id
        .clone()
        .or_else(|| parent_job.execution_id.clone());

    let project_path = run_db(load_project_path(orch.db.clone(), project_id.clone()))?;

    // ---- Load agent config from files (mirrors create_child_task) --------
    // The same root the call's snapshot entry is resolved from below.
    let config_dir = orch.config_dir.clone();
    let mut agent_config: AgentConfig = {
        let file_agent = match config_agents::get_agent(
            &LibraryRoots::at(&config_dir, project_path.as_deref()),
            &input.subagent_type,
        ) {
            Ok(Some(agent)) => agent,
            Ok(None) => {
                let agents = config_agents::list_agents(&LibraryRoots::at(
                    &config_dir,
                    project_path.as_deref(),
                ))
                .unwrap_or_default();
                let mut found = None;
                for result in agents {
                    if let ConfigResult::Ok(agent) = result {
                        if agent.name == input.subagent_type {
                            found = Some(agent);
                            break;
                        }
                    }
                }
                found.ok_or_else(|| format!("Agent config not found: {}", input.subagent_type))?
            }
            Err(e) => return Err(format!("Failed to load agent config: {}", e)),
        };
        file_agent.agent_config(&project_id)
    };

    // ---- Mint ids + select the model ------------------------------------
    let job_id = ids::mint_child(&project_id);
    let run_id = ids::mint_child(&job_id);
    let session_id = ids::mint_session_id().into_string();
    let now = chrono::Utc::now().timestamp() as i32;

    let subject = crate::execution::model_selection::SelectionSubject {
        issue_id: issue_id.clone(),
        execution_id: execution_id.clone(),
        node_id: None,
        job_id: Some(job_id.clone()),
    };
    let record = if orch.get_settings().route_calls_via_openrouter {
        select_call_via_openrouter(
            orch,
            &project_id,
            input.models.as_deref(),
            &agent_config,
            subject,
        )?
    } else {
        orch.select_for_agent(
            &project_id,
            input.models.as_deref(),
            &agent_config.models,
            subject,
        )?
    };
    let selected_model = Some(record.chosen.model.clone());
    agent_config.selection = Some(record.chosen.clone());
    agent_config.extras = Some(record.extras.clone());

    // A call never owns a checkout. Inherit carries the parent's durable branch;
    // None remains branchless. The session layer creates the job scratch
    // residence independently of this coordinate.
    //
    // The parent's recorded base rides along as bookkeeping under either policy.
    // It is not a coordinate anything resolves against — a call owns no checkout
    // to place — so an unrecorded one is carried as `None` rather than refusing
    // the call. The branch, which the caller genuinely asked to inherit, is still
    // required.
    let (branch, base_commit) = match input.branch_policy {
        CallBranchPolicy::Inherit => (
            Some(parent_job.branch.clone().ok_or_else(|| {
                format!(
                    "Cannot inherit the caller's branch: parent job {} has no branch",
                    input.parent_job_id
                )
            })?),
            parent_job.base_commit.clone(),
        ),
        CallBranchPolicy::None => (None, parent_job.base_commit.clone()),
    };
    // ---- Persist job + session + run ------------------------------------
    let output_contract_json = serde_json::to_string(&input.output_contract)
        .map_err(|e| format!("Failed to serialize call output contract: {e}"))?;

    let execution_id = run_db_borrowed(insert_child_with_agent_snapshot(
        orch,
        db.clone(),
        ChildInsert {
            job_id: job_id.clone(),
            run_id: run_id.clone(),
            session_id: session_id.clone(),
            parent_job_id: Some(input.parent_job_id.clone()),
            branch,
            agent_config_id: agent_config.id.clone(),
            project_id: project_id.clone(),
            issue_id: issue_id.clone(),
            execution_id: execution_id.clone(),
            description: input.description.clone(),
            model: selected_model.as_ref().map(|m| m.to_string()),
            base_commit,
            output_contract: Some(output_contract_json),
            label: input.label.clone(),
            phase: input.phase.clone(),
            parent_tool_use_id: input.parent_tool_use_id.clone(),
            task_index: input.task_index,
            now,
        },
        project_path.as_deref(),
    ))?;

    orch.store_job_selection(&job_id, &record)?;

    let _ = orch.services.emitter.emit(
        "db-change",
        crate::notify::job_db_change_ids(
            "insert",
            &job_id,
            issue_id.as_deref(),
            thread_id.as_deref(),
            execution_id.as_deref(),
            Some(&input.parent_job_id),
            None,
            &project_id,
        ),
    );
    let _ = orch.services.emitter.emit(
        "db-change",
        crate::notify::run_db_change_ids(
            "insert",
            &run_id,
            Some(&job_id),
            issue_id.as_deref(),
            Some(&project_id),
        ),
    );

    // Link this call's run to its workflow journal key (private, runner-
    // transient) so the completion path journals the result under
    // (workflow_run_id, ordinal). Best-effort: journaling never blocks a call.
    if let Some(link) = input.workflow_journal_link.as_ref() {
        if let Err(e) = run_db({
            let db = orch.db.local.clone();
            let run_id = run_id.clone();
            let link = link.clone();
            async move { crate::workflow_journal::store_call_link(&db, &run_id, &link).await }
        }) {
            log::warn!("Failed to record workflow_call link for run {run_id}: {e}");
        }
    }

    // ---- Seed the transcript --------------------------------------------
    // The call's prompt was written by whoever invoked the call, not typed at
    // this job by an operator (CAIRN-3408).
    store_launch_event(orch, &run_id, &session_id, &input.prompt, now)?;

    // The prompt's output-artifact instruction is built from the SAME contract
    // that resolve_artifact_contract validates against, so they cannot drift.
    let output_schema = OutputSchemaInfo {
        schema: input.output_contract.schema_type.clone(),
        artifact_name: Some(input.output_contract.artifact_name()),
        confirm_policy: ConfirmPolicy::default(),
        tool_name: input.output_contract.tool_name.clone(),
        description: input.output_contract.description.clone(),
    };

    Ok(PreparedCallRun {
        job_id,
        run_id,
        session_id,
        agent_config,
        selected_model,
        prompt: input.prompt,
        output_schema,
        execution_id,
    })
}

/// Start (or queue) a prepared call run. Split from [`prepare_call_run`] so the
/// call packet is persisted first.
///
/// This is the ONLY consumer of the legacy calls-only admission ledger. Runtime
/// cost and lane identity come from the backend's shared launch capability; the
/// old batch descriptor is consulted only for its temporary local ceiling until
/// promotion moves entirely to the durable runtime-admission service.
pub(crate) fn start_call_run(
    orch: &Orchestrator,
    prepared: &PreparedCallRun,
) -> Result<(), String> {
    // Resolve the backend the SAME way `start_agent_session` does, then key
    // admission on that backend's canonical name so it can never charge a
    // different backend than the session actually launches.
    let backend = crate::backends::backend_for_name(admission_backend_name(prepared).as_deref());
    let capability = backend.runtime_launch_capability(&crate::backends::RuntimeLaunch {
        is_ephemeral_call: true,
        server_instance_key: None,
    })?;
    // Stateless HTTP has no resident runtime to reconstruct and must not acquire
    // a durable-DB failure mode merely because its independent API stream claim
    // is non-zero.
    //
    // Codex classification is provisional until authentication resolves the
    // exact app-server pool key. Persisting the provisional `None` key would
    // charge every logical stream as a separate resident process.
    //
    // Both start without a durable row, so the in-memory launch claim is what
    // keeps the stale-run sweep off the run until its process registers.
    if capability.class == crate::backends::RuntimeClass::StatelessHttp
        || (capability.class == crate::backends::RuntimeClass::MultiplexedServer
            && capability.claim.server_instance_key.is_none())
    {
        let launch = orch.claim_job_launch(&prepared.job_id, &prepared.run_id);
        if orch.startup_readiness.is_ready() {
            return start_call_run_now(orch, prepared);
        }
        // No durable row can hold this class, so it waits on the readiness
        // latch in the background, keeping its launch claim until it starts.
        let task_orch = orch.clone();
        let prepared = prepared.clone();
        orch.spawn_blocking_detached(move || {
            let orch = task_orch;
            orch.startup_readiness.wait_ready_blocking();
            if super::private_lane::run_settled(&orch, &prepared.run_id) != Some(true) {
                if let Err(e) = start_call_run_now(&orch, &prepared) {
                    log::error!("deferred call {} failed to start: {e}", prepared.run_id);
                    crate::orchestrator::lifecycle::fail_run(
                        &orch,
                        &prepared.run_id,
                        &format!("call start failed: {e}"),
                    );
                }
            }
            drop(launch);
        });
        return Ok(());
    }

    let concurrency = backend.call_batch_capability().max_concurrency;
    if concurrency.is_some_and(|bounds| bounds.cap == 0) {
        return Err(format!(
            "runtime admission capacity for {} is zero",
            capability.lane.0
        ));
    }
    let now = chrono::Utc::now().timestamp();
    let request_id = format!("call-admission:{}", prepared.run_id);
    let prepared_json = serde_json::to_string(prepared)
        .map_err(|e| format!("failed to persist prepared call identity: {e}"))?;
    run_db({
        let db = orch.db.local.clone();
        let request_id = request_id.clone();
        let run_id = prepared.run_id.clone();
        let lane = capability.lane.0.clone();
        let claim = capability.claim.clone();
        async move {
            let request = crate::storage::enqueue_admission_request(
                &db,
                crate::storage::NewAdmissionRequest {
                    request_id,
                    workspace_key: crate::storage::CALL_ADMISSION_WORKSPACE.into(),
                    resource_lane: lane,
                    launch_key: format!("call:{run_id}"),
                    resource_claim: crate::storage::RuntimeResourceClaim {
                        resident_process_units: claim.resident_process_units,
                        server_instance_key: claim.server_instance_key,
                        logical_stream_units: claim.logical_stream_units,
                        estimated_memory_bytes: claim.estimated_memory_bytes,
                    },
                    urgent: false,
                    now,
                },
            )
            .await
            .map_err(|e| e.to_string())?;
            crate::storage::persist_prepared_runtime_call(
                &db,
                crate::storage::PreparedRuntimeCall {
                    request_id: request.request_id.clone(),
                    run_id,
                    prepared_json,
                },
                now,
            )
            .await
            .map_err(|e| e.to_string())?;
            Ok::<_, String>(request)
        }
    })?;
    // Promotion consumes whichever request durable fairness selects. It may be
    // an older request, so tying handoff to this enqueue's request id would drop
    // a valid lease and leave an otherwise idle lane stuck until expiry.
    promote_call_lane(orch, &capability.lane.0, concurrency);
    Ok(())
}

const CALL_ADMISSION_OWNER: &str = "local-call-promoter";
const CALL_LEASE_SECONDS: i64 = 300;

/// This boot's call lease owner. Boot-scoped like ordinary admission's, so
/// startup recovery can requeue an earlier boot's leases without requeueing a
/// call this boot already started (which would launch it a second time).
fn call_lease_owner(orch: &Orchestrator) -> String {
    format!("{CALL_ADMISSION_OWNER}@{}", orch.boot_at)
}

/// Resolve exactly the backend reconstruction would launch, without exposing
/// any of its prompt, agent configuration, or credentials to the inspector.
pub(crate) fn prepared_call_provider_metadata(
    prepared_json: &str,
    server_instance_key: Option<String>,
) -> Option<(
    crate::storage::PreparedRuntimeCapability,
    crate::storage::RuntimeResourceCapacity,
)> {
    let prepared = decode_prepared_call(prepared_json)?;
    let backend_name = admission_backend_name(&prepared);
    let backend = crate::backends::backend_for_name(backend_name.as_deref());
    let capability = backend
        .runtime_launch_capability(&crate::backends::RuntimeLaunch {
            is_ephemeral_call: true,
            server_instance_key,
        })
        .ok()?;
    let mut capability: crate::storage::PreparedRuntimeCapability =
        serde_json::from_value(serde_json::to_value(capability).ok()?).ok()?;
    capability.provider =
        Some(backend_name.unwrap_or_else(|| crate::backends::CLAUDE_FAMILY_BACKEND.to_string()));
    let cap = call_lane_concurrency(Some(&prepared)).map_or(usize::MAX, |bounds| bounds.cap);
    Some((
        capability,
        crate::storage::RuntimeResourceCapacity {
            resident_process_units: u32::try_from(cap).unwrap_or(u32::MAX),
            logical_stream_units: u32::MAX,
        },
    ))
}

fn decode_prepared_call(prepared_json: &str) -> Option<PreparedCallRun> {
    serde_json::from_str(prepared_json).ok()
}

/// The count bounds of the lane a prepared call admits on. A prepared call
/// that cannot be read collapses to Claude, matching `backend_for_name`.
fn call_lane_concurrency(
    prepared: Option<&PreparedCallRun>,
) -> Option<crate::backends::CallConcurrency> {
    let backend_name = prepared.and_then(admission_backend_name);
    crate::backends::backend_for_name(backend_name.as_deref())
        .call_batch_capability()
        .max_concurrency
}

/// Close `lease` into `terminal`, fenced on the owner and generation it was
/// read with so a replacement lease is never closed by mistake. Returns
/// whether this write closed it.
fn finish_call_lease(
    orch: &Orchestrator,
    lease: &crate::storage::AdmissionRequest,
    terminal: &'static str,
    detail: Option<&'static str>,
) -> bool {
    let db = orch.db.local.clone();
    let launch_key = lease.launch_key.clone();
    let owner = lease.lease_owner.clone().unwrap_or_default();
    let generation = lease.lease_generation;
    run_db(async move {
        crate::storage::finish_admission_by_launch_key(
            &db,
            crate::storage::CALL_ADMISSION_WORKSPACE,
            &launch_key,
            &owner,
            generation,
            terminal,
            detail,
            chrono::Utc::now().timestamp(),
        )
        .await
        .map_err(|e| e.to_string())
    })
    .unwrap_or_else(|error| {
        log::warn!(
            "call admission {} could not be closed as {terminal}: {error}",
            lease.request_id
        );
        false
    })
}

/// Lease the next call `lane` admits, measured against the same host memory
/// envelope ordinary sessions lease against. Calls are batch work: they yield
/// to the private ordinary-session lanes, leaving room above the reserve for
/// every session queued there, and a call lane with nothing in flight gets no
/// exemption from that. The backend's count bounds apply only as the fan-out
/// cap on a measured host and as the whole bound on an unmeasurable one.
///
/// A lease or a hold is recorded in the host's lane ledger for
/// `cairn://executors`; an empty queue decides nothing about a call, so it
/// leaves the last decision standing.
fn lease_next_call(
    orch: &Orchestrator,
    lane: &str,
    concurrency: Option<crate::backends::CallConcurrency>,
) -> Result<Option<crate::storage::AdmissionRequest>, String> {
    let db = orch.db.local.clone();
    let lane_owned = lane.to_string();
    let owner = call_lease_owner(orch);
    let budget = call_lane_budget(orch, concurrency);
    let floor = orch.process_floor_refusal();
    let now = budget.measured_at;
    let decision = run_db(async move {
        crate::storage::lease_next_admission_within(
            &db,
            crate::storage::CALL_ADMISSION_WORKSPACE,
            &lane_owned,
            &owner,
            now,
            now + CALL_LEASE_SECONDS,
            budget.capacity(),
            budget.memory,
            None,
            crate::storage::MemoryPriority::YieldsTo(super::private_lane::WORKSPACE),
            floor.as_ref(),
        )
        .await
        .map_err(|e| e.to_string())
    })?;
    let record = |shortfall| {
        orch.private_lanes.record(
            crate::storage::CALL_ADMISSION_WORKSPACE,
            lane,
            budget,
            shortfall,
        )
    };
    Ok(match decision {
        crate::storage::LeaseDecision::Leased(request) => {
            record(None);
            Some(request)
        }
        crate::storage::LeaseDecision::Blocked { shortfall, .. } => {
            record(Some(shortfall));
            None
        }
        crate::storage::LeaseDecision::Empty => None,
    })
}

/// What one call lease decides against, measured now: the shared host memory
/// envelope, and as its ceiling the count bound the backend applies on such a
/// host (the fan-out cap when memory is measured, the unmeasured-host bound
/// otherwise; none when the backend declares no count).
fn call_lane_budget(
    orch: &Orchestrator,
    concurrency: Option<crate::backends::CallConcurrency>,
) -> super::private_lane::PrivateLaneBudget {
    let memory = super::private_lane::host_memory_envelope(orch);
    super::private_lane::PrivateLaneBudget {
        ceiling: concurrency
            .map(|bounds| u32::try_from(bounds.units(memory.is_some())).unwrap_or(u32::MAX)),
        memory,
        measured_at: chrono::Utc::now().timestamp(),
    }
}

fn start_leased_call(
    orch: &Orchestrator,
    lease: crate::storage::AdmissionRequest,
    prepared: PreparedCallRun,
) -> Result<(), String> {
    let owner = lease.lease_owner.clone().unwrap_or_default();
    let transition = |from: &'static str, to: &'static str| {
        let db = orch.db.local.clone();
        let id = lease.request_id.clone();
        let owner = owner.clone();
        let generation = lease.lease_generation;
        run_db(async move {
            crate::storage::transition_admission(
                &db,
                &id,
                &owner,
                generation,
                from,
                to,
                chrono::Utc::now().timestamp(),
            )
            .await
            .map_err(|e| e.to_string())
        })
    };
    // As for an ordinary launch: a lease that lost the race with the
    // fresh-pipe floor goes back to the queue, never to a failed run.
    if lease.resource_claim.spawns_dedicated_process() {
        if let Some(floor) = orch.process_floor_refusal() {
            if !transition("leased", "queued")? {
                return Err("runtime admission lease was replaced before call start".into());
            }
            orch.private_lanes.record(
                crate::storage::CALL_ADMISSION_WORKSPACE,
                &lease.resource_lane,
                call_lane_budget(orch, None),
                Some(crate::storage::LaneShortfall::PipeHealth { assessment: floor }),
            );
            return Ok(());
        }
    }
    if !transition("leased", "starting")? {
        return Err("runtime admission lease was replaced before call start".into());
    }
    if let Err(error) = start_call_run_now(orch, &prepared) {
        let _ = transition("starting", "failed");
        crate::orchestrator::lifecycle::fail_run(
            orch,
            &prepared.run_id,
            &format!("call start failed: {error}"),
        );
        return Err(error);
    }
    if !transition("starting", "active")? {
        crate::orchestrator::lifecycle::release_and_stop_process(orch, &prepared.run_id);
        return Err("runtime admission lease was replaced during call start".into());
    }
    // A terminal release or generation replacement ends ownership, and the
    // renewer drops the lease the first time its fenced renewal misses.
    orch.admission_renewal
        .register(super::admission_renewal::RenewedLease {
            request_id: lease.request_id.clone(),
            owner,
            generation: lease.lease_generation,
            lease_seconds: CALL_LEASE_SECONDS,
            still_current: None,
        });
    Ok(())
}

/// Resolve the backend name for admission through the SAME resolution
/// `start_agent_session` (`session.rs`) spawns with
/// ([`crate::backends::effective_backend_name`]). Keying admission on a
/// divergent resolution would charge one backend while launching another once
/// real ceilings differ. `None` collapses to Claude in `backend_for_name`,
/// matching session start.
fn admission_backend_name(prepared: &PreparedCallRun) -> Option<String> {
    crate::backends::effective_backend_name(
        prepared.agent_config.selection.as_ref(),
        prepared.selected_model.as_ref().map(Model::as_str),
    )
}

/// Opt-in (CAIRN-2550): when `route_calls_via_openrouter` is enabled, a call
/// selects among the OpenRouter models its allotment holds, so it runs in the
/// zero-process in-process HTTP loop instead of a native CLI backend.
///
/// A call that names one model outright is never moved: a named model is
/// never substituted. Fail-loud, never a silent native fallback: an allotment
/// holding no OpenRouter model is refused, because quietly running natively
/// would reintroduce the ~450 MB CLI process the user opted out of.
fn select_call_via_openrouter(
    orch: &Orchestrator,
    project_id: &str,
    own: Option<&[cairn_config::allotment::AllotmentEntry]>,
    agent: &AgentConfig,
    subject: crate::execution::model_selection::SelectionSubject,
) -> Result<crate::execution::model_selection::SelectionRecord, String> {
    use cairn_config::allotment::{model_ref, AllotmentEntry};
    let models = orch.effective_models(Some(project_id))?;
    let allotment = crate::execution::model_selection::effective_allotment(
        own,
        &agent.models,
        &models.default_models,
    );
    if let [AllotmentEntry::Model(_)] = allotment.as_slice() {
        return orch.select_for_agent(project_id, Some(&allotment), &agent.models, subject);
    }
    let routed: Vec<AllotmentEntry> = models
        .expand(&allotment, &cairn_config::allotment::unplaced)?
        .candidates
        .into_iter()
        .filter(|candidate| candidate.model.backend == "openrouter")
        .map(|candidate| AllotmentEntry::Model(candidate.model))
        .collect();
    if routed.is_empty() {
        let names: Vec<String> = allotment.iter().map(ToString::to_string).collect();
        return Err(format!(
            "route_calls_via_openrouter is enabled but [{}] holds no OpenRouter model. Put one on the ladder in those pools, or disable the opt-in; refusing to silently run the call natively.",
            names.join(", ")
        ));
    }
    let record = orch.select_for_agent(project_id, Some(&routed), &agent.models, subject)?;
    log::info!(
        "[select] call routed through OpenRouter: {} ({})",
        model_ref(&record.chosen),
        record.provenance
    );
    Ok(record)
}

/// Start the backend session for an admitted call run. The single caller of
/// `start_agent_session` for calls, so native output-schema wiring is identical
/// on both the start-now and admitted-later (dequeued) paths.
fn start_call_run_now(orch: &Orchestrator, prepared: &PreparedCallRun) -> Result<(), String> {
    crate::orchestrator::session::start_agent_session(
        orch,
        &prepared.run_id,
        &prepared.prompt,
        crate::backends::SessionStart::New {
            session_id: prepared.session_id.clone(),
        },
        prepared.selected_model.clone(),
        None,
        Some(&prepared.agent_config),
        Some(&prepared.output_schema),
        // A call is a node-less, one-shot prompt->JSON worker: constrain its
        // output to the contract natively so cheap tiers return schema-valid
        // results reliably, captured server-side (CAIRN-2505).
        true,
        false,
        prepared.execution_id.as_deref(),
        None,
    )?;

    // Mark the call's process ACTIVE (CAIRN-2526). A call establishes no DB turn
    // (it is node-less), so unlike an ordinary run it never reaches the
    // `set_current_turn_id -> begin_turn` that flips a normal run's occupancy off
    // `Idle`. An `Idle` process reads as between turns, and when the Claude
    // reader's `was_host_interrupted` check read `Idle` alone it treated every
    // `tool_result` user event as a post-interrupt transport artifact and dropped
    // it — so a completed
    // call's transcript kept its `tool_use` rows but stored zero results, and the
    // drill-in tool detail rendered forever "pending". The call IS actively
    // serving its prompt, so `Busy` is the honest occupancy; the handle exists by
    // now (start_agent_session registered it before returning, the same ordering
    // the normal path relies on for `set_current_turn_id`), and no tool_result
    // can arrive before the MCP handshake completes.
    orch.process_state.transition_to_active(&prepared.run_id);
    Ok(())
}

/// Release the call slot (if any) held by a finalized run and start the next
/// queued call. Hooked off `finalize_run` — the single terminal choke every run
/// passes through — so a crashed/killed call can never leak its slot. No-op for
/// uncapped/non-call runs; idempotent, so hooking it from more than one terminal
/// path (finalize + the stop reconcile early-return) never double-counts.
pub(crate) fn on_call_run_finalized(orch: &Orchestrator, run_id: &str) {
    let _ = settle_call_admission(orch, run_id);
}

/// What closing one run's call admission did, for the heal sweep's log line.
struct SettledCallAdmission {
    request_id: String,
    prior_state: String,
    /// Whether this pass moved the row to a terminal state.
    closed: bool,
    /// Whether a registered process handle was stopped.
    process_stopped: bool,
}

/// [`on_call_run_finalized`], reporting what it did. `None` when the run has
/// no call admission row.
fn settle_call_admission(orch: &Orchestrator, run_id: &str) -> Option<SettledCallAdmission> {
    let request = run_db({
        let db = orch.db.local.clone();
        let key = format!("call:{run_id}");
        async move {
            crate::storage::get_admission_request_by_launch_key(
                &db,
                crate::storage::CALL_ADMISSION_WORKSPACE,
                &key,
            )
            .await
            .map_err(|e| e.to_string())
        }
    })
    .ok()
    .flatten()?;
    let held_slot = matches!(request.state.as_str(), "leased" | "starting" | "active");
    let closed = if request.state == "queued" {
        let db = orch.db.local.clone();
        let id = request.request_id.clone();
        run_db(async move {
            crate::storage::cancel_queued_admission(
                &db,
                &id,
                Some("call finalized before launch"),
                chrono::Utc::now().timestamp(),
            )
            .await
            .map_err(|e| e.to_string())
        })
        .unwrap_or(false)
    } else if held_slot {
        // Fenced on the lease as read, whichever boot's promoter took it: a
        // row an earlier boot leased is released by the same write.
        finish_call_lease(orch, &request, "released", None)
    } else {
        false
    };

    let mut process_stopped = false;
    if held_slot {
        // The freed slot must correspond to a DEAD process, not a warm one still
        // holding ~450 MB of `claude` RSS — otherwise the ceiling bounds slot
        // count while real memory keeps climbing (a paper cap). A finalized
        // ephemeral call is one-shot and never resumed, so reap its OS process
        // here at the instant its slot frees, tying the concurrency bound to real
        // memory (CAIRN-2548). No-op when the kill path already removed the handle
        // (e.g. `kill_session_with_reason`), and cheap for a queued/uncapped run
        // (never reached — `held_slot` is false there).
        process_stopped = crate::orchestrator::lifecycle::release_and_stop_process(orch, run_id);
        // The memory this call held goes to ordinary sessions first; a queued
        // session also holds back the call promotion below until it leases.
        super::private_lane::promote_private_lanes(orch);
    }

    let finalized_prepared = prepared_call_for(orch, &request.request_id);
    promote_call_lane(
        orch,
        &request.resource_lane,
        call_lane_concurrency(finalized_prepared.as_ref()),
    );
    Some(SettledCallAdmission {
        request_id: request.request_id,
        prior_state: request.state,
        closed,
        process_stopped,
    })
}

fn prepared_call_for(orch: &Orchestrator, request_id: &str) -> Option<PreparedCallRun> {
    run_db({
        let db = orch.db.local.clone();
        let id = request_id.to_string();
        async move {
            crate::storage::get_prepared_runtime_call(&db, &id)
                .await
                .map_err(|e| e.to_string())
        }
    })
    .ok()
    .flatten()
    .and_then(|p| decode_prepared_call(&p.prepared_json))
}

/// Lease and start the next launchable call on `lane`. Promotion is lane-driven
/// rather than tied to the request which happened to trigger it: fairness may
/// select a different queued request, and dropping that lease would strand it
/// until expiry.
///
/// A lease whose run is already over is closed, never launched. Startup
/// recovery requeues an earlier boot's rows, and a row can sit queued while its
/// run is settled elsewhere; launching either would spawn a process for a run
/// nothing will ever finalize again. The pass then moves on, so the slot that
/// triggered it still goes to live work.
///
/// Drains the lane: starts calls until it is empty, has no room for its head,
/// or is unreadable. One pass per lane runs at a time (see `private_lane`'s
/// "One pass per lane"); a call that finds one running returns `false` at
/// once, and the running pass drains once more on its behalf.
///
/// Returns whether this pass started a call (a start that failed still
/// consumed its lease and failed its run).
fn promote_call_lane(
    orch: &Orchestrator,
    lane: &str,
    concurrency: Option<crate::backends::CallConcurrency>,
) -> bool {
    // The row is durable before any pass reaches here, so a call held for
    // startup recovery waits queued for the drain that follows readiness.
    if !orch.startup_readiness.is_ready() {
        orch.private_lanes.record(
            crate::storage::CALL_ADMISSION_WORKSPACE,
            lane,
            call_lane_budget(orch, concurrency),
            Some(crate::storage::LaneShortfall::StartupRecovery),
        );
        return false;
    }
    let Some(mut pass) = orch
        .private_lanes
        .enter_pass(crate::storage::CALL_ADMISSION_WORKSPACE, lane)
    else {
        return false;
    };
    let mut started = false;
    loop {
        while start_next_call(orch, lane, concurrency) {
            started = true;
        }
        if !pass.again() {
            return started;
        }
    }
}

/// Lease and start the lane's next call; `false` when there is none to start.
fn start_next_call(
    orch: &Orchestrator,
    lane: &str,
    concurrency: Option<crate::backends::CallConcurrency>,
) -> bool {
    loop {
        let Ok(Some(next_lease)) = lease_next_call(orch, lane, concurrency) else {
            return false;
        };
        let Some(next) = prepared_call_for(orch, &next_lease.request_id) else {
            log::warn!(
                "call admission {} has no readable prepared call; failing it rather than holding its slot",
                next_lease.request_id
            );
            finish_call_lease(
                orch,
                &next_lease,
                "failed",
                Some("leased call has no prepared payload"),
            );
            continue;
        };
        // An unreadable owning database proves nothing, so only a positive
        // answer closes the lease.
        if super::private_lane::run_settled(orch, &next.run_id) == Some(true) {
            log::warn!(
                "call admission {}: run {} already settled; cancelling the lease instead of launching it",
                next_lease.request_id,
                &next.run_id[..next.run_id.len().min(8)]
            );
            finish_call_lease(
                orch,
                &next_lease,
                "cancelled",
                Some("run settled before launch"),
            );
            continue;
        }
        if let Err(e) = start_leased_call(orch, next_lease, next.clone()) {
            // Fail-closed: an admitted-but-unstartable call must fail its run via
            // the normal error path so the workflow await resolves — never leave
            // it silently queued/starting.
            log::error!("admitted call {} failed to start: {e}", next.run_id);
            crate::orchestrator::lifecycle::fail_run(
                orch,
                &next.run_id,
                &format!("call admission start failed: {e}"),
            );
        }
        return true;
    }
}

/// [`promote_call_lanes`] in the background, when a call is queued at all.
/// Ordinary-session promotion runs this once its queue is drained: calls
/// yield to queued sessions, so that is when they may admit again.
pub(crate) fn promote_queued_call_lanes(orch: &Orchestrator) {
    let db = orch.db.local.clone();
    let queued = run_db(async move {
        db.query_opt_text(
            "SELECT request_id FROM runtime_admission_requests WHERE workspace_key=?1 AND state='queued' LIMIT 1",
            (crate::storage::CALL_ADMISSION_WORKSPACE.to_string(),),
        )
        .await
        .map_err(|error| error.to_string())
    });
    if matches!(queued, Ok(Some(_))) {
        let promoter = orch.clone();
        orch.spawn_blocking_detached(move || promote_call_lanes(&promoter));
    }
}

/// Drain every call lane holding queued work as far as host memory and its
/// count bounds admit.
///
/// Event-driven promotion (an enqueue, a release, an ordinary-session queue
/// draining) covers the steady state; this is the backstop that runs at boot
/// and on every process sweep, so a queued call is never stranded by memory or
/// a slot that freed without an event, such as one the heal below released.
pub(crate) fn promote_call_lanes(orch: &Orchestrator) {
    let rows = match run_db({
        let db = orch.db.local.clone();
        async move {
            crate::storage::list_unsettled_call_admissions(&db)
                .await
                .map_err(|e| e.to_string())
        }
    }) {
        Ok(rows) => rows,
        Err(error) => {
            log::warn!("call admission could not list queued calls: {error}");
            return;
        }
    };
    let mut lanes: Vec<(String, Option<crate::backends::CallConcurrency>)> = Vec::new();
    for row in rows.iter().filter(|row| row.state == "queued") {
        if lanes.iter().any(|(lane, _)| *lane == row.resource_lane) {
            continue;
        }
        let concurrency = call_lane_concurrency(decode_prepared_call(&row.prepared_json).as_ref());
        lanes.push((row.resource_lane.clone(), concurrency));
    }
    for (lane, concurrency) in lanes {
        promote_call_lane(orch, &lane, concurrency);
    }
}

/// Close every call admission row whose run is already over, through the one
/// path that also stops the process behind it ([`on_call_run_finalized`], which
/// releases the row and then runs `release_and_stop_process`). Returns how
/// many rows were closed.
///
/// A row outliving its run is not a bookkeeping leak: an `active` row's
/// heartbeat renews it forever and holds a lane unit, and once its run is
/// settled nothing else will ever stop the process behind it. Both the boot
/// pass and the periodic sweep run this, so a host carrying such rows heals on
/// its own.
///
/// `orphans` is the boot pass's OS process table. After a restart a process the
/// predecessor started is registered nowhere, and the only identity left is
/// the session id in its argv. A call's session is minted for one run and never
/// resumed, so a process naming the session of a call whose run is over cannot
/// be live work. The periodic sweep passes `None`: on a live host every call
/// process it started is registered, so the release path stops it.
pub(crate) fn heal_settled_call_admissions(
    orch: &Orchestrator,
    orphans: Option<&dyn crate::agent_process::orphan::ProcessTable>,
) -> usize {
    let rows = match run_db({
        let db = orch.db.local.clone();
        async move {
            crate::storage::list_unsettled_call_admissions(&db)
                .await
                .map_err(|e| e.to_string())
        }
    }) {
        Ok(rows) => rows,
        Err(error) => {
            log::warn!("call admission heal could not list call rows: {error}");
            return 0;
        }
    };
    let settled: Vec<(crate::storage::UnsettledCallAdmission, String)> = rows
        .into_iter()
        .filter_map(|row| {
            let evidence = match row.local_run_status.as_deref() {
                Some("starting" | "live") => return None,
                Some(status) => format!("its run is {status}"),
                // The run lives in another database or is gone; an unreadable
                // owning database proves nothing.
                None => match super::private_lane::run_settled(orch, &row.run_id) {
                    Some(true) => "its run is terminal or gone in its owning database".into(),
                    _ => return None,
                },
            };
            Some((row, evidence))
        })
        .collect();
    if settled.is_empty() {
        return 0;
    }

    let unregistered_holder = |row: &crate::storage::UnsettledCallAdmission| {
        row.state != "queued" && orch.process_state.has_process(&row.run_id) == Some(false)
    };
    // One listing for the whole pass, and only when some row could have an
    // orphan behind it.
    let listing = orphans
        .filter(|_| settled.iter().any(|(row, _)| unregistered_holder(row)))
        .map(|table| table.list());

    let mut healed = 0;
    for (row, evidence) in settled {
        let run = &row.run_id[..row.run_id.len().min(8)];
        // Orphans first, so the promotion the release triggers measures the
        // memory they held.
        let mut orphan_outcome = None;
        if let (Some(table), Some(entries)) = (orphans, listing.as_ref()) {
            if unregistered_holder(&row) {
                if let Some(prepared) = decode_prepared_call(&row.prepared_json) {
                    let matches = crate::agent_process::orphan::orphans_naming_sessions(
                        entries,
                        std::slice::from_ref(&prepared.session_id),
                        std::process::id(),
                    );
                    let (stopped, survived): (Vec<u32>, Vec<u32>) = matches
                        .iter()
                        .map(|(pid, _)| *pid)
                        .partition(|pid| table.stop(*pid));
                    orphan_outcome = Some(match (stopped.is_empty(), survived.is_empty()) {
                        (true, true) => format!("no process names session {}", prepared.session_id),
                        (_, true) => format!("stopped orphan pid(s) {stopped:?}"),
                        _ => format!(
                            "stopped orphan pid(s) {stopped:?}, could NOT confirm {survived:?} stopped"
                        ),
                    });
                }
            }
        }
        let Some(settlement) = settle_call_admission(orch, &row.run_id) else {
            continue;
        };
        let process = if settlement.process_stopped {
            "stopped its registered process".to_string()
        } else if let Some(outcome) = orphan_outcome {
            outcome
        } else if settlement.prior_state == "queued" {
            "it never launched a process".to_string()
        } else {
            "no registered process".to_string()
        };
        log::warn!(
            "call admission heal: run {run} row {} was {} but {evidence}; {} the row; {process}",
            settlement.request_id,
            settlement.prior_state,
            if settlement.closed {
                "closed"
            } else {
                "could not close"
            },
        );
        if settlement.closed {
            healed += 1;
        }
    }
    healed
}

/// Recover the call admission ledger after a restart, then fail fast every
/// ephemeral call left in flight with nothing to launch it, so none strands
/// `starting` forever (CAIRN-2548).
///
/// The undelivered set is exactly the runs still holding a `workflow_call`
/// journal link. One whose admission row is still queued or in flight is left
/// to the promoter; each other non-terminal one is driven through the normal
/// [`crate::orchestrator::lifecycle::finalize_run`]`(Crashed)` path — identical
/// to how any other crashed call is handled — which journals the call as a
/// failure (the workflow replay then resolves that ordinal as `null`) and drops
/// its link.
///
/// Runs at host startup BEFORE `redispatch_crashed_workflows` re-spawns the
/// workflow scripts, so it only ever touches stale pre-crash runs, never a
/// freshly re-issued call.
pub(crate) async fn fail_orphaned_calls_on_startup(orch: &Orchestrator) {
    recover_calls_on_startup(
        orch,
        std::sync::Arc::new(crate::agent_process::orphan::OsProcessTable),
    )
    .await;
}

/// [`fail_orphaned_calls_on_startup`] against an injected process table.
///
/// Order matters. The heal runs first, while an `active` row still says a
/// process stood behind its call: requeueing it would erase that and turn it
/// into a launch to retry. Only this boot's leases are spared by the requeue.
/// Promotion then drains the lanes, closing any requeued row whose run the
/// boot reconcile already settled rather than launching it.
async fn recover_calls_on_startup(
    orch: &Orchestrator,
    orphans: std::sync::Arc<dyn crate::agent_process::orphan::ProcessTable>,
) {
    let healer = orch.clone();
    if let Err(error) = tokio::task::spawn_blocking(move || {
        heal_settled_call_admissions(&healer, Some(orphans.as_ref()));
    })
    .await
    {
        log::warn!("startup call recovery: heal worker failed: {error}");
    }
    if let Err(error) = crate::storage::requeue_orphaned_admissions(
        &orch.db.local,
        crate::storage::CALL_ADMISSION_WORKSPACE,
        Some(&call_lease_owner(orch)),
        chrono::Utc::now().timestamp(),
    )
    .await
    {
        log::warn!("startup call recovery: failed to reclaim orphaned leases: {error}");
    }
    if let Err(error) =
        crate::storage::reap_expired_admissions(&orch.db.local, chrono::Utc::now().timestamp())
            .await
    {
        log::warn!("startup call recovery: failed to reap expired leases: {error}");
    }
    let promoter = orch.clone();
    if let Err(error) = tokio::task::spawn_blocking(move || promote_call_lanes(&promoter)).await {
        log::warn!("startup call recovery: promotion worker failed: {error}");
    }
    let run_ids = match crate::workflow_journal::list_undelivered_call_run_ids(&orch.db.local).await
    {
        Ok(ids) => ids,
        Err(e) => {
            log::warn!("startup call recovery: failed to list undelivered calls: {e}");
            return;
        }
    };

    let mut failed = 0usize;
    for run_id in run_ids {
        let launch_key = format!("call:{run_id}");
        if let Ok(Some(request)) = crate::storage::get_admission_request_by_launch_key(
            &orch.db.local,
            crate::storage::CALL_ADMISSION_WORKSPACE,
            &launch_key,
        )
        .await
        {
            // Queued is recoverable state that the drain above or a later
            // release will promote, and a leased, starting or active row is a
            // launch this boot made. Neither is evidence the run should crash.
            if matches!(
                request.state.as_str(),
                "queued" | "leased" | "starting" | "active"
            ) {
                continue;
            }
        }
        let owning = crate::execution::routing::owning_db_for_run(&orch.db, &run_id)
            .await
            .unwrap_or_else(|_| orch.db.local.clone());
        let status = orphaned_call_run_status(owning, run_id.clone()).await;
        // Only the runs that lost their process (or never started one) need
        // failing; a terminal run is already settled and its link would be gone.
        if matches!(status.as_deref(), Some("starting") | Some("live")) {
            crate::orchestrator::lifecycle::finalize_run(
                orch,
                &run_id,
                crate::models::RunStatus::Crashed,
            );
            failed += 1;
        }
    }
    if failed > 0 {
        log::info!("Failed {failed} orphaned in-flight call run(s) on startup");
    }
}

/// The stored status of a run, or `None` if the row is gone.
async fn orphaned_call_run_status(db: Arc<LocalDb>, run_id: String) -> Option<String> {
    db.read(|conn| {
        let run_id = run_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query("SELECT status FROM runs WHERE id = ?1", (run_id.as_str(),))
                .await?;
            Ok(rows.next().await?.and_then(|r| r.text(0).ok()))
        })
    })
    .await
    .ok()
    .flatten()
}

// ============================================================================
// restart_call (CAIRN-2516)
// ============================================================================

/// Restart an undelivered workflow child call.
///
/// Only legal for an in-flight/undelivered call — one whose `workflow_call`
/// journal link still exists. A delivered call (link deleted at finalize) is
/// rejected server-side: the script already consumed its result and re-running
/// would diverge from the journal. The harness awaits the call by its JOB (the
/// latest run's status), so a restart is a fresh RUN under the SAME job, ordered
/// to avoid resolving the parked `?wait` with a premature `null` OR finalizing
/// the job before the new run takes over:
///
/// 1. create a new session+run under the same job (`starting`, newest
///    `created_at` -> the latest run -> `terminal_call_body` sees non-terminal ->
///    the parked await keeps waiting) and seed its prompt;
/// 2. move the journal link from the old run to the new one (delete old, store
///    new), so the OLD run's finalize journals nothing and the NEW run journals
///    on completion;
/// 3. start the new backend session — this creates the new run's turn and makes
///    it live, so a subsequent recompute tracks it;
/// 4. hard-kill the old run. Its link is gone (no journal write) and it is no
///    longer the latest run, so the await ignores it; the job stays live on the
///    new run's running turn rather than briefly finalizing.
///
/// This ordering stays deadlock-safe under call admission. At a ceiling of 1 with
/// the old run holding the slot, step 3's `start_call_run` enqueues the new run
/// (admission never blocks — it returns `Queued` immediately), the new run is the
/// latest and sits `starting` so the await stays parked, then step 4's kill drives
/// the old run through `finalize_run` → `on_call_run_finalized`, which releases the
/// slot and starts the queued new run.
pub fn restart_call(orch: &Orchestrator, call_job_id: &str) -> Result<(), String> {
    let owning = run_db({
        let dbs = orch.db.clone();
        let call_job_id = call_job_id.to_string();
        async move {
            crate::execution::routing::owning_db_for_job(&dbs, &call_job_id)
                .await
                .map_err(|e| e.to_string())
        }
    })?;

    let job = run_db(load_job(
        owning.clone(),
        call_job_id.to_string(),
        "Call job not found",
    ))?;

    if !job_parent_is_workflow(orch, call_job_id)? {
        return Err(format!(
            "Job {call_job_id} is not a workflow child call; refusing restart_call"
        ));
    }

    // Resolve the latest run + its workflow tags; reject if there is none.
    let Some((old_run_id, label, phase)) =
        run_db(latest_run_row(owning.clone(), call_job_id.to_string()))?
    else {
        return Err(format!("Call job {call_job_id} has no run to restart"));
    };

    // Boundary check: only an undelivered call (link still present) is
    // restartable. Validated here, not just hidden in the UI.
    let private = orch.db.local.clone();
    let Some(link) = run_db({
        let private = private.clone();
        let old_run_id = old_run_id.clone();
        async move { crate::workflow_journal::load_call_link(&private, &old_run_id).await }
    })?
    else {
        return Err(format!(
            "Call {call_job_id} is already delivered (no pending journal link); refusing restart"
        ));
    };

    // Reconstruct the spawn inputs from the persisted job + old run.
    let prompt = run_db(seed_user_prompt(owning.clone(), old_run_id.clone()))?
        .ok_or_else(|| format!("Call {call_job_id} has no seed prompt to restart"))?;
    let contract_json = run_db(job_output_contract(owning.clone(), call_job_id.to_string()))?
        .ok_or_else(|| format!("Call job {call_job_id} has no output contract"))?;
    let output_contract: crate::models::DelegatedOutputContract =
        serde_json::from_str(&contract_json)
            .map_err(|e| format!("Failed to parse call output contract: {e}"))?;

    // Retry must be idempotent on the completion contract: if the call already
    // wrote its return artifact, its work is DONE. A user-initiated stop must
    // never resurrect a completed call, and re-running one would waste a model
    // call and risk overwriting a finished result (CAIRN-2677 bug 3). Deliver the
    // existing artifact instead of re-prompting: finalize the latest (possibly
    // warm-parked or hung) run, and the artifact-first completion mapping journals
    // it Success and resolves the parked `agent()` await.
    let has_artifact = run_db({
        let owning = owning.clone();
        let call_job_id = call_job_id.to_string();
        let artifact_name = output_contract.artifact_name();
        async move {
            owning
                .query_opt_text(
                    "SELECT id FROM artifacts WHERE job_id = ?1 AND output_name = ?2 LIMIT 1",
                    (call_job_id, artifact_name),
                )
                .await
                .map_err(|e| e.to_string())
        }
    })?
    .is_some();
    if has_artifact {
        log::info!(
            "restart_call: call {call_job_id} already wrote its return artifact; delivering it \
             instead of re-running (finalizing run {old_run_id})"
        );
        crate::orchestrator::lifecycle::finalize_run(
            orch,
            &old_run_id,
            crate::models::RunStatus::Exited,
        );
        return Ok(());
    }

    let project_path = run_db(load_project_path(orch.db.clone(), job.project_id.clone()))?;
    let mut agent_config = load_agent_config(orch, &job, project_path.as_deref())?
        .ok_or_else(|| format!("Could not reconstruct agent config for call {call_job_id}"))?;
    let selected_model = job.model.as_deref().map(Model::new);
    // CAIRN-2550: the original call may have been rerouted to OpenRouter by the
    // `route_calls_via_openrouter` opt-in, which swapped `agent_config.selection` in
    // memory but persisted only `job.model`. Reconstructing the agent config from
    // files loses that reroute, and both admission (`admission_backend_name`) and
    // launch (`start_agent_session`) prefer `selection.backend` over `selected_model`
    // — so without this a restarted rerouted call could charge and launch the NATIVE
    // backend while carrying the OpenRouter model id. Rebuild the atomic selection
    // from the stored model so the restart is faithful to what the original call
    // actually resolved to (correct for native calls too).
    // A call that stored its selection already reads it back through
    // `load_agent_config`; only a call from before that needs rebuilding.
    if agent_config.selection.is_none() {
        agent_config.selection = reconstructed_call_selection(job.model.as_deref());
    }

    let run_id = ids::mint_child(call_job_id);
    let session_id = ids::mint_session_id().into_string();

    let now = chrono::Utc::now().timestamp() as i32;

    // 1. New session+run under the SAME job (starting -> newest -> latest run),
    //    then seed its transcript. No turn yet: the call start path creates it.
    run_db(insert_restart_run_rows(
        owning.clone(),
        call_job_id.to_string(),
        run_id.clone(),
        session_id.clone(),
        job.issue_id.clone(),
        job.project_id.clone(),
        label.clone(),
        phase.clone(),
        now,
    ))?;
    let _ = orch.services.emitter.emit(
        "db-change",
        crate::notify::run_db_change_ids(
            "insert",
            &run_id,
            Some(call_job_id),
            job.issue_id.as_deref(),
            Some(&job.project_id),
        ),
    );
    // A restart replays the original call prompt into a fresh session, so it
    // seeds the same launch event the first attempt did (CAIRN-3408).
    store_launch_event(orch, &run_id, &session_id, &prompt, now)?;

    // 2. Move the journal link to the new run BEFORE killing the old run, so the
    //    old run's finalize finds no link and journals nothing.
    run_db({
        let private = private.clone();
        let old_run_id = old_run_id.clone();
        let run_id = run_id.clone();
        let link = link.clone();
        async move {
            crate::workflow_journal::delete_call_link(&private, &old_run_id).await?;
            crate::workflow_journal::store_call_link(&private, &run_id, &link).await
        }
    })?;

    // 3. Start the new backend session (creates the new turn, makes it live).
    let output_schema = crate::models::OutputSchemaInfo {
        schema: output_contract.schema_type.clone(),
        artifact_name: Some(output_contract.artifact_name()),
        confirm_policy: crate::models::ConfirmPolicy::default(),
        tool_name: output_contract.tool_name.clone(),
        description: output_contract.description.clone(),
    };
    let prepared = PreparedCallRun {
        job_id: call_job_id.to_string(),
        run_id: run_id.clone(),
        session_id,
        agent_config,
        selected_model,
        prompt,
        output_schema,
        execution_id: job.execution_id.clone(),
    };
    start_call_run(orch, &prepared)?;

    // 4. Hard-kill the old run now that the new one is live. Its link is gone so
    //    it journals nothing, and it is no longer the latest run so the await
    //    ignores it; the job stays live on the new run's turn.
    if let Err(e) = crate::orchestrator::lifecycle::kill_session_with_reason(
        orch,
        &old_run_id,
        "user_stop",
        crate::orchestrator::lifecycle::TerminalPolicy::Keep,
    ) {
        log::warn!("restart_call: failed to kill prior run {old_run_id}: {e}");
    }

    Ok(())
}

/// Rebuild the atomic backend+model selection a restarted call must launch with,
/// from the persisted `job.model`. `backend_for_model` maps an OpenRouter model id
/// (`~...`/`.../...`/`openrouter/auto`) back to the `openrouter` backend, so a call
/// the opt-in rerouted to OpenRouter is restored to OpenRouter rather than a native
/// backend derived from a stale file reconstruction. `None` model → no override.
fn reconstructed_call_selection(job_model: Option<&str>) -> Option<crate::models::ModelSelection> {
    job_model.map(|model| {
        let backend = crate::backends::backend_for_model(model).unwrap_or("claude");
        crate::models::ModelSelection::new(backend, Model::new(model))
    })
}

/// Whether a job's parent node carries the synthetic `agent_config_id =
/// "workflow"`. Gates [`restart_call`] to genuine workflow child calls.
fn job_parent_is_workflow(orch: &Orchestrator, call_job_id: &str) -> Result<bool, String> {
    let dbs = orch.db.clone();
    let call_job_id = call_job_id.to_string();
    let aid = run_db(async move {
        let owning = crate::execution::routing::owning_db_for_job(&dbs, &call_job_id)
            .await
            .map_err(|e| e.to_string())?;
        owning
            .read(|conn| {
                let call_job_id = call_job_id.clone();
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT parent.agent_config_id FROM jobs j \
                             JOIN jobs parent ON j.parent_job_id = parent.id \
                             WHERE j.id = ?1 LIMIT 1",
                            (call_job_id.as_str(),),
                        )
                        .await?;
                    Ok(rows.next().await?.and_then(|row| row.text(0).ok()))
                })
            })
            .await
            .map_err(|e| e.to_string())
    })?;
    Ok(super::is_workflow_agent_config(aid.as_deref()))
}

/// `(run_id, label, phase)` of the latest run for a job (by `created_at`), or
/// `None` when it has no run.
async fn latest_run_row(
    db: Arc<LocalDb>,
    job_id: String,
) -> Result<Option<(String, Option<String>, Option<String>)>, String> {
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    // `rowid DESC` tiebreaks the second-resolution `created_at`
                    // so the latest run is deterministic when two runs share a
                    // timestamp (a fast restart), matching the wait path.
                    "SELECT id, label, phase FROM runs \
                     WHERE job_id = ?1 ORDER BY created_at DESC, rowid DESC LIMIT 1",
                    (job_id.as_str(),),
                )
                .await?;
            match rows.next().await? {
                Some(row) => Ok(Some((row.text(0)?, row.opt_text(1)?, row.opt_text(2)?))),
                None => Ok(None),
            }
        })
    })
    .await
    .map_err(|e| e.to_string())
}

/// The prompt a call run was launched on (its earliest user-slot event).
///
/// `restart_call` reconstructs its spawn inputs from this and refuses the
/// restart outright when it comes back empty, so the type set here is
/// load-bearing rather than cosmetic. A call seeded since CAIRN-3408 stores
/// `user:launch`; the plain `user` type stays in the set because runs recorded
/// before that keep it, and a restart must not become impossible for them.
async fn seed_user_prompt(db: Arc<LocalDb>, run_id: String) -> Result<Option<String>, String> {
    db.read(|conn| {
        let run_id = run_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    // A bound parameter inside an `IN (...)` list does not
                    // reliably match here, so the launch type is compared with
                    // its own equality term.
                    "SELECT data FROM events \
                     WHERE run_id = ?1 AND (event_type = 'user' OR event_type = ?2) \
                     ORDER BY sequence ASC, rowid ASC LIMIT 1",
                    (run_id.as_str(), crate::transcripts::LAUNCH_EVENT_TYPE),
                )
                .await?;
            let Some(row) = rows.next().await? else {
                return Ok(None);
            };
            let data = row.text(0)?;
            let content = serde_json::from_str::<serde_json::Value>(&data)
                .ok()
                .and_then(|value| {
                    value
                        .get("content")
                        .and_then(|c| c.as_str())
                        .map(str::to_string)
                });
            Ok(content)
        })
    })
    .await
    .map_err(|e| e.to_string())
}

/// The stored `output_contract` JSON for a job, or `None`.
async fn job_output_contract(db: Arc<LocalDb>, job_id: String) -> Result<Option<String>, String> {
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT output_contract FROM jobs WHERE id = ?1 LIMIT 1",
                    (job_id.as_str(),),
                )
                .await?;
            match rows.next().await? {
                Some(row) => Ok(row.opt_text(0)?),
                None => Ok(None),
            }
        })
    })
    .await
    .map_err(|e| e.to_string())
}

/// Insert a fresh session + `starting` run under an EXISTING call job and point
/// the job's `current_session_id` at the new session. The restart analogue of
/// the run/session tail of `insert_child_job_session_run`, minus the job insert.
#[allow(clippy::too_many_arguments)]
async fn insert_restart_run_rows(
    db: Arc<LocalDb>,
    job_id: String,
    run_id: String,
    session_id: String,
    issue_id: Option<String>,
    project_id: String,
    label: Option<String>,
    phase: Option<String>,
    now: i32,
) -> Result<(), String> {
    db.write(move |conn| {
        let job_id = job_id.clone();
        let run_id = run_id.clone();
        let session_id = session_id.clone();
        let issue_id = issue_id.clone();
        let project_id = project_id.clone();
        let label = label.clone();
        let phase = phase.clone();
        Box::pin(async move {
            insert_session_conn(conn, &session_id, Some(job_id.as_str()), None, "claude", now)
                .await?;
            conn.execute(
                "INSERT INTO runs(
                    id, issue_id, project_id, job_id, chat_id, status, session_id,
                    error_message, started_at, exited_at, created_at, updated_at,
                    exit_reason, start_mode, label, phase
                )
                VALUES (?1, ?2, ?3, ?4, NULL, 'starting', ?5, NULL, ?6, NULL, ?6, ?6, NULL, 'fresh', ?7, ?8)",
                params![
                    run_id.as_str(),
                    issue_id.as_deref(),
                    Some(project_id.as_str()),
                    Some(job_id.as_str()),
                    Some(session_id.as_str()),
                    now,
                    label.as_deref(),
                    phase.as_deref(),
                ],
            )
            .await?;
            conn.execute(
                "UPDATE jobs SET current_session_id = ?1, updated_at = ?2 WHERE id = ?3",
                params![session_id.as_str(), now, job_id.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|e| e.to_string())
}

#[cfg(test)]
mod admission_backend_tests_support {
    use super::*;
    use crate::models::{AgentConfig, ConfirmPolicy, OutputSchema, OutputSchemaInfo};

    /// A Claude call as `prepare_call_run` would build it.
    pub(super) fn prepared_for(run_id: &str) -> PreparedCallRun {
        PreparedCallRun {
            job_id: "j".into(),
            run_id: run_id.into(),
            session_id: "s".into(),
            agent_config: AgentConfig {
                id: "Explore".into(),
                name: "Explore".into(),
                description: String::new(),
                prompt: String::new(),
                tools: Vec::new(),
                models: Vec::new(),
                workspace_id: None,
                project_id: None,
                created_at: 0,
                updated_at: 0,
                disallowed_tools: None,
                skills: None,
                fence: None,
                icon: None,
                selection: None,
                extras: None,
            },
            selected_model: Some(Model::new("sonnet")),
            prompt: "do the thing".into(),
            output_schema: OutputSchemaInfo {
                schema: OutputSchema::Preset("return".into()),
                artifact_name: Some("return".into()),
                confirm_policy: ConfirmPolicy::default(),
                tool_name: None,
                description: None,
            },
            execution_id: None,
        }
    }
}

#[cfg(test)]
mod admission_backend_tests {
    use super::*;
    use crate::models::{
        AgentConfig, ConfirmPolicy, Model, ModelSelection, OutputSchema, OutputSchemaInfo,
    };

    fn prepared(selection: Option<ModelSelection>, model: Option<&str>) -> PreparedCallRun {
        PreparedCallRun {
            job_id: "j".into(),
            run_id: "r".into(),
            session_id: "s".into(),
            agent_config: AgentConfig {
                id: "Explore".into(),
                name: "Explore".into(),
                description: String::new(),
                prompt: String::new(),
                tools: Vec::new(),
                models: Vec::new(),
                workspace_id: None,
                project_id: None,
                created_at: 0,
                updated_at: 0,
                disallowed_tools: None,
                skills: None,
                fence: None,
                icon: None,
                selection,
                extras: None,
            },
            selected_model: model.map(Model::new),
            prompt: String::new(),
            output_schema: OutputSchemaInfo {
                schema: OutputSchema::Preset("return".into()),
                artifact_name: Some("return".into()),
                confirm_policy: ConfirmPolicy::default(),
                tool_name: None,
                description: None,
            },
            execution_id: None,
        }
    }

    /// The reviewer's example: an explicit OpenRouter selection carrying a
    /// concrete `gpt-5` model must admit against OpenRouter — model-derivation
    /// alone would wrongly charge Codex. The atomic selection wins, matching
    /// what `start_agent_session` launches.
    #[test]
    fn selection_backend_wins_over_model_derivation() {
        let p = prepared(
            Some(ModelSelection::new("openrouter", Model::new("gpt-5"))),
            Some("gpt-5"),
        );
        assert_eq!(admission_backend_name(&p).as_deref(), Some("openrouter"));
    }

    /// Model-derivation answers when no selection is set.
    #[test]
    fn model_derivation_answers_a_bare_config() {
        let p = prepared(None, Some("gpt-5"));
        assert_eq!(admission_backend_name(&p).as_deref(), Some("codex"));
    }

    /// A Claude model names Claude outright rather than leaving it implied, so
    /// admission and `start_agent_session` charge and launch the same provider.
    #[test]
    fn claude_model_keys_claude() {
        let p = prepared(None, Some("sonnet"));
        assert_eq!(admission_backend_name(&p).as_deref(), Some("claude"));
        assert_eq!(
            crate::backends::backend_for_name(admission_backend_name(&p).as_deref()).name(),
            "Claude"
        );
    }
}

#[cfg(test)]
mod route_calls_tests {
    use super::*;

    /// Restart must reconstruct an OpenRouter selection from a persisted OpenRouter
    /// model id, so a rerouted call that is restarted stays on OpenRouter rather
    /// than falling back to a native backend (which both admission and launch would
    /// otherwise pick from the file-reconstructed selection).
    #[test]
    fn reconstructed_call_selection_preserves_openrouter_reroute() {
        let s = reconstructed_call_selection(Some("~anthropic/claude-sonnet-latest")).unwrap();
        assert_eq!(s.backend, "openrouter");
        assert_eq!(s.model.as_str(), "~anthropic/claude-sonnet-latest");
        assert_eq!(
            reconstructed_call_selection(Some("openrouter/auto"))
                .unwrap()
                .backend,
            "openrouter"
        );
    }

    /// Native models reconstruct to their native backends; no model → no override.
    #[test]
    fn reconstructed_call_selection_native_models() {
        assert_eq!(
            reconstructed_call_selection(Some("opus")).unwrap().backend,
            "claude"
        );
        assert_eq!(
            reconstructed_call_selection(Some("gpt-5.3-codex"))
                .unwrap()
                .backend,
            "codex"
        );
        assert!(reconstructed_call_selection(None).is_none());
    }
}

#[cfg(test)]
mod restart_call_tests {
    use super::*;
    use crate::db::DbState;
    use crate::orchestrator::OrchestratorBuilder;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::SearchIndex;
    use std::sync::Arc;

    /// A fixture orchestrator admits calls against the fixed test host, never
    /// the machine running the suite, and a test can inject a tighter or
    /// unmeasurable one.
    #[tokio::test]
    async fn call_admission_measures_the_fixture_host_not_the_machine() {
        use crate::agent_process::memory::SystemMemory;
        use cairn_session::agent_process::gc::{memory_reserve, DEFAULT_SPAWN_RSS};
        let (orch, _db) = orch_with_db().await;
        let fixture = crate::orchestrator::TEST_HOST_MEMORY;
        assert_eq!(
            super::super::private_lane::host_memory_envelope(&orch),
            Some(crate::storage::HostMemoryEnvelope {
                available_bytes: fixture.available,
                reserve_bytes: memory_reserve(fixture.total),
                unmeasured_claim_bytes: DEFAULT_SPAWN_RSS,
            })
        );

        // 16 GiB total, 3 GiB available: a 2 GiB reserve and 1 GiB of headroom.
        let tight = orch_with_db_and_probe(Some(SystemMemory {
            total: 16 << 30,
            available: 3 << 30,
        }))
        .await;
        assert_eq!(
            super::super::private_lane::host_memory_envelope(&tight)
                .map(|envelope| envelope.headroom(0)),
            Some(1 << 30)
        );
        let unmeasured = orch_with_db_and_probe(None).await;
        assert_eq!(
            super::super::private_lane::host_memory_envelope(&unmeasured),
            None
        );
    }

    async fn orch_with_db_and_probe(
        system: Option<crate::agent_process::memory::SystemMemory>,
    ) -> Orchestrator {
        let local = Arc::new(
            LocalDb::open_migrated_for_test(
                tempfile::tempdir().unwrap().keep().join("calls-probe.db"),
            )
            .await
            .unwrap(),
        );
        let search =
            Arc::new(SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap());
        OrchestratorBuilder::new(
            Arc::new(DbState::new(local, search)),
            Arc::new(TestServicesBuilder::new().build()),
            tempfile::tempdir().unwrap().keep(),
        )
        .host_memory(Arc::new(
            crate::agent_process::memory::StubMemoryProbe::new(system),
        ))
        .build()
    }

    async fn orch_with_db() -> (Orchestrator, Arc<LocalDb>) {
        let local = Arc::new(
            LocalDb::open_migrated_for_test(
                tempfile::tempdir().unwrap().keep().join("restart-call.db"),
            )
            .await
            .unwrap(),
        );
        let search =
            Arc::new(SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap());
        let db = Arc::new(DbState::new(local.clone(), search));
        let orch = OrchestratorBuilder::new(
            db,
            Arc::new(TestServicesBuilder::new().build()),
            tempfile::tempdir().unwrap().keep(),
        )
        .build();
        (orch, local)
    }

    /// Seed a workflow node job `j-wf` and a child call job `j-call` with a live
    /// run `call-run`, but NO `workflow_call` link — i.e. an already-delivered
    /// call. `parent_agent` sets the parent node's `agent_config_id`.
    async fn seed_call(db: &LocalDb, parent_agent: &str) {
        for sql in [
            "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w','W',1,1)".to_string(),
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p','w','P','prj','/tmp/p',1,1)".to_string(),
            "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at) VALUES ('i','p',3,'T','active',1,1)".to_string(),
            format!("INSERT INTO jobs (id, project_id, issue_id, status, agent_config_id, uri_segment, node_name, created_at, updated_at) VALUES ('j-wf','p','i','running','{parent_agent}','wf','Flow',1,1)"),
            "INSERT INTO jobs (id, project_id, issue_id, parent_job_id, status, agent_config_id, output_contract, uri_segment, node_name, created_at, updated_at) VALUES ('j-call','p','i','j-wf','running','Explore','{\"schemaType\":\"return\"}','call','Explore',1,1)".to_string(),
            "INSERT INTO sessions (id, job_id, created_at, updated_at) VALUES ('sess','j-call',1,1)".to_string(),
            "INSERT INTO runs (id, job_id, issue_id, project_id, status, created_at, updated_at) VALUES ('call-run','j-call','i','p','live',1,1)".to_string(),
        ] {
            db.execute(&sql, ()).await.unwrap();
        }
    }

    /// Store a call run's opening prompt under `event_type`.
    async fn seed_launch_event(db: &LocalDb, event_type: &str, content: &str) {
        let data = serde_json::to_string(&crate::agent_process::stream::TranscriptEvent {
            event_type: event_type.to_string(),
            session_id: Some("sess".to_string()),
            parent_tool_use_id: None,
            content: Some(content.to_string()),
            thinking: None,
            tool_name: None,
            tool_input: None,
            tool_uses: None,
            tool_use_id: None,
            tool_result: None,
            is_error: false,
            thinking_ms: None,
            queued_message_id: None,
            raw: None,
        })
        .unwrap();
        db.execute(
            "INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data, created_at)
             VALUES('ev-launch','call-run','sess',0,1,?1,?2,1)",
            (event_type, data.as_str()),
        )
        .await
        .unwrap();
    }

    /// `restart_call` reconstructs its spawn inputs from the run's opening
    /// prompt and refuses the restart when it finds none, so namespacing that
    /// event (CAIRN-3408) without widening this lookup would have turned every
    /// call restart into a hard "has no seed prompt to restart" failure.
    ///
    /// Both types are asserted: new calls store `user:launch`, and runs recorded
    /// before the change keep `user` and must stay restartable.
    #[tokio::test]
    async fn a_call_restart_recovers_its_launch_prompt_under_either_type() {
        for event_type in ["user", crate::transcripts::LAUNCH_EVENT_TYPE] {
            let (_orch, db) = orch_with_db().await;
            seed_call(&db, "workflow").await;
            seed_launch_event(&db, event_type, "Explore the parser and report back").await;

            let recovered = seed_user_prompt(db.clone(), "call-run".to_string())
                .await
                .unwrap();

            assert_eq!(
                recovered.as_deref(),
                Some("Explore the parser and report back"),
                "a call stored as `{event_type}` must stay restartable"
            );
        }
    }

    /// A call whose `workflow_call` link is gone is delivered; restarting it
    /// would diverge from the journal, so it is rejected at the boundary.
    #[tokio::test]
    async fn delivered_call_restart_is_rejected() {
        let (orch, db) = orch_with_db().await;
        seed_call(&db, "workflow").await;
        let err = restart_call(&orch, "j-call").expect_err("delivered call must be rejected");
        assert!(
            err.contains("delivered"),
            "expected a delivered-call rejection, got: {err}"
        );
    }

    /// A non-workflow child call is refused before any link lookup.
    #[tokio::test]
    async fn non_workflow_call_restart_is_rejected() {
        let (orch, db) = orch_with_db().await;
        seed_call(&db, "Build").await;
        let err = restart_call(&orch, "j-call").expect_err("non-workflow call must be rejected");
        assert!(
            err.contains("not a workflow child call"),
            "expected a not-a-workflow-call rejection, got: {err}"
        );
    }

    /// The stored status of a run, or `None` when the row is gone.
    async fn run_status_now(db: &LocalDb, run_id: &str) -> Option<String> {
        let run_id = run_id.to_string();
        db.read(|conn| {
            let run_id = run_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query("SELECT status FROM runs WHERE id = ?1", (run_id.as_str(),))
                    .await?;
                Ok(rows.next().await?.and_then(|r| r.text(0).ok()))
            })
        })
        .await
        .ok()
        .flatten()
    }

    /// A workflow call left in-flight by a host crash (its `workflow_call` link
    /// still present, run `starting`) is failed-fast on startup so it cannot
    /// strand `starting` forever once the admission ceiling makes queuing
    /// reachable (CAIRN-2548): the run terminalizes, its link drops, and its
    /// ordinal journals a failure so the workflow replay resolves it as null.
    #[tokio::test]
    async fn startup_fails_an_orphaned_undelivered_call() {
        use crate::workflow_journal::{self, CallLink, JournalStatus};
        let (orch, db) = orch_with_db().await;
        seed_call(&db, "workflow").await;
        // Make `call-run` an undelivered, in-flight call: reset it to `starting`
        // and record its journal link, as a live workflow call would.
        db.execute(
            "UPDATE runs SET status = 'starting' WHERE id = 'call-run'",
            (),
        )
        .await
        .unwrap();
        workflow_journal::store_call_link(
            &db,
            "call-run",
            &CallLink {
                workflow_run_id: "wf-run".into(),
                ordinal: 0,
                prompt_hash: workflow_journal::hash_prompt("do the thing"),
            },
        )
        .await
        .unwrap();

        fail_orphaned_calls_on_startup(&orch).await;

        // Terminalized, not stranded.
        assert_eq!(
            run_status_now(&db, "call-run").await.as_deref(),
            Some("crashed")
        );
        // Link dropped and the ordinal journaled as a failure -> the workflow
        // replay resolves this call as null instead of hanging.
        assert!(workflow_journal::load_call_link(&db, "call-run")
            .await
            .unwrap()
            .is_none());
        let entry = workflow_journal::get_entry(&db, "wf-run", 0)
            .await
            .unwrap()
            .expect("the crashed call must journal a failure");
        assert_eq!(entry.status, JournalStatus::Failure);
    }

    /// A delivered call (no `workflow_call` link) is untouched by the startup
    /// sweep — only undelivered in-flight calls are failed, so a call the replay
    /// will re-issue is never pre-emptively killed.
    #[tokio::test]
    async fn startup_leaves_calls_without_a_link_untouched() {
        let (orch, db) = orch_with_db().await;
        seed_call(&db, "workflow").await;
        db.execute(
            "UPDATE runs SET status = 'starting' WHERE id = 'call-run'",
            (),
        )
        .await
        .unwrap();

        fail_orphaned_calls_on_startup(&orch).await;

        // No link -> not in the undelivered set -> left exactly as-is.
        assert_eq!(
            run_status_now(&db, "call-run").await.as_deref(),
            Some("starting")
        );
    }

    // ---- Call admission rows outliving their runs (cairn/5336) ------------

    /// Base rows every call-admission scenario hangs its calls off.
    async fn seed_call_project(db: &LocalDb) {
        db.execute_script(
            "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w','W',1,1);
             INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p','w','P','prj','/tmp/p',1,1);
             INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at) VALUES ('i','p',3,'T','active',1,1);
             INSERT INTO jobs (id, project_id, issue_id, status, agent_config_id, uri_segment, node_name, created_at, updated_at) VALUES ('j-wf','p','i','running','workflow','wf','Flow',1,1);",
        )
        .await
        .unwrap();
    }

    fn prepared_call_json(run_id: &str, session_id: &str) -> String {
        let mut prepared = super::admission_backend_tests_support::prepared_for(run_id);
        prepared.session_id = session_id.to_string();
        prepared.job_id = format!("j-{run_id}");
        serde_json::to_string(&prepared).unwrap()
    }

    /// One call: its job, session, run (`run_status`, created long past the
    /// stale-run grace) and its admission row in `state` with its prepared
    /// payload, exactly as `start_call_run` persists them.
    async fn seed_call_admission(
        db: &LocalDb,
        run: &str,
        run_status: &str,
        state: &str,
        owner: Option<&str>,
        seq: i64,
    ) {
        let owner = owner.map_or("NULL".to_string(), |owner| format!("'{owner}'"));
        db.execute_script(format!(
            "INSERT INTO jobs (id, project_id, issue_id, parent_job_id, status, agent_config_id, output_contract, uri_segment, node_name, created_at, updated_at) VALUES ('j-{run}','p','i','j-wf','running','Explore','{{\"schemaType\":\"return\"}}','{run}','Explore',1,1);
             INSERT INTO sessions (id, job_id, created_at, updated_at) VALUES ('s-{run}','j-{run}',1,1);
             INSERT INTO runs (id, job_id, issue_id, project_id, session_id, status, start_mode, created_at, updated_at) VALUES ('{run}','j-{run}','i','p','s-{run}','{run_status}','fresh',1,1);
             INSERT INTO runtime_admission_requests(request_id,workspace_key,resource_lane,launch_key,claim_json,priority,enqueue_seq,state,lease_owner,lease_generation,lease_expires_at,created_at,updated_at)
               VALUES('call-admission:{run}','local-runtime','claude-cli','call:{run}','{{\"residentProcessUnits\":1,\"logicalStreamUnits\":1}}',0,{seq},'{state}',{owner},1,9999999999,1,1);"
        ))
        .await
        .unwrap();
        crate::storage::persist_prepared_runtime_call(
            db,
            crate::storage::PreparedRuntimeCall {
                request_id: format!("call-admission:{run}"),
                run_id: run.to_string(),
                prepared_json: prepared_call_json(run, &format!("s-{run}")),
            },
            1,
        )
        .await
        .unwrap();
    }

    async fn admission_state(db: &LocalDb, run: &str) -> String {
        crate::storage::get_admission_request(db, &format!("call-admission:{run}"))
            .await
            .unwrap()
            .unwrap()
            .state
    }

    fn register_call_process(orch: &Orchestrator, run: &str) {
        use crate::agent_process::process::RunHandle;
        orch.process_state.processes.lock().unwrap().register(
            run.to_string(),
            RunHandle::test_handle(Some(&format!("s-{run}")), Some(&format!("j-{run}"))),
        );
    }

    /// A call waiting minutes for its promoter to reach process registration is
    /// covered by its row however long it waits; once the row is `active` and
    /// no handle stands behind it, the run IS stale, and settling it releases
    /// the row instead of leaving it to hold a lane unit forever.
    #[tokio::test]
    async fn a_waiting_call_row_is_cover_and_an_active_one_without_a_process_is_reaped_and_released(
    ) {
        let (orch, db) = orch_with_db().await;
        seed_call_project(&db).await;
        seed_call_admission(&db, "call-queued", "starting", "queued", None, 1).await;
        seed_call_admission(
            &db,
            "call-leased",
            "starting",
            "leased",
            Some("local-call-promoter"),
            2,
        )
        .await;
        seed_call_admission(
            &db,
            "call-starting",
            "starting",
            "starting",
            Some("local-call-promoter"),
            3,
        )
        .await;
        seed_call_admission(
            &db,
            "call-active",
            "live",
            "active",
            Some("local-call-promoter"),
            4,
        )
        .await;
        // Its own lane, so the slot its release frees promotes nothing: a
        // promoted call here would try to spawn a real CLI and fail its run.
        db.execute(
            "UPDATE runtime_admission_requests SET resource_lane='isolated' WHERE request_id='call-admission:call-active'",
            (),
        )
        .await
        .unwrap();

        // Every run was created at t=1, far past the grace, and none has a
        // handle. Only the active one lacks a claim.
        assert_eq!(crate::runs::reap::reap_stale_runs(&orch), 1);
        for run in ["call-queued", "call-leased", "call-starting"] {
            assert_eq!(
                run_status_now(&db, run).await.as_deref(),
                Some("starting"),
                "{run} is mid-launch and must not be crashed"
            );
        }
        assert_eq!(
            run_status_now(&db, "call-active").await.as_deref(),
            Some("crashed")
        );
        assert_eq!(admission_state(&db, "call-active").await, "released");
        assert_eq!(admission_state(&db, "call-queued").await, "queued");
    }

    /// The incident's step 5: the run was settled while its process was still
    /// spawning, so the process's real exit reaches `finalize_run` as a
    /// re-entry. It must still release the row and stop the process.
    #[tokio::test]
    async fn finalizing_an_already_crashed_call_still_releases_its_row_and_stops_its_process() {
        let (orch, db) = orch_with_db().await;
        seed_call_project(&db).await;
        seed_call_admission(
            &db,
            "call-run",
            "crashed",
            "active",
            Some("local-call-promoter"),
            1,
        )
        .await;
        register_call_process(&orch, "call-run");

        crate::orchestrator::lifecycle::finalize_run(
            &orch,
            "call-run",
            crate::models::RunStatus::Exited,
        );

        assert_eq!(admission_state(&db, "call-run").await, "released");
        assert_eq!(orch.process_state.has_process("call-run"), Some(false));
        assert_eq!(
            run_status_now(&db, "call-run").await.as_deref(),
            Some("crashed"),
            "the settled outcome stands"
        );
    }

    /// Settling a run withdraws its queued call row in the same write.
    #[tokio::test]
    async fn settling_a_stale_run_cancels_its_queued_call_row() {
        let (_orch, db) = orch_with_db().await;
        seed_call_project(&db).await;
        seed_call_admission(&db, "call-run", "starting", "queued", None, 1).await;

        let settled = db
            .write(|conn| {
                Box::pin(async move {
                    crate::runs::queries::settle_stale_run(
                        conn,
                        "call-run",
                        "crash",
                        crate::runs::queries::StaleRunSettlement::Reaped,
                        5,
                    )
                    .await
                })
            })
            .await
            .unwrap();

        assert!(settled.is_some());
        assert_eq!(admission_state(&db, "call-run").await, "cancelled");
    }

    /// The host state cairn/5336 measured, seeded exactly, heals in one boot
    /// pass: active rows whose run crashed are released and their processes
    /// stopped (a registered warm one through its handle, an unregistered one
    /// by the session id in its argv), a queued row whose run crashed is
    /// cancelled, an earlier boot's lease is released, and a call this boot
    /// already leased is left alone.
    #[tokio::test]
    async fn boot_recovery_heals_call_rows_whose_runs_are_over_and_stops_their_processes() {
        use crate::agent_process::orphan::RecordingProcessTable;
        let (orch, db) = orch_with_db().await;
        seed_call_project(&db).await;
        let this_boot = call_lease_owner(&orch);
        seed_call_admission(&db, "dead-queued", "crashed", "queued", None, 1).await;
        seed_call_admission(
            &db,
            "dead-warm",
            "crashed",
            "active",
            Some("local-call-promoter"),
            2,
        )
        .await;
        seed_call_admission(
            &db,
            "dead-orphan",
            "crashed",
            "active",
            Some("local-call-promoter"),
            3,
        )
        .await;
        seed_call_admission(
            &db,
            "live-this-boot",
            "starting",
            "leased",
            Some(&this_boot),
            4,
        )
        .await;
        register_call_process(&orch, "dead-warm");
        let table = std::sync::Arc::new(RecordingProcessTable::new(vec![
            (
                4242,
                "claude --print --session-id s-dead-orphan".to_string(),
            ),
            (4343, "claude --print --session-id s-unrelated".to_string()),
        ]));

        recover_calls_on_startup(&orch, table.clone()).await;

        assert_eq!(admission_state(&db, "dead-queued").await, "cancelled");
        assert_eq!(admission_state(&db, "dead-warm").await, "released");
        assert_eq!(orch.process_state.has_process("dead-warm"), Some(false));
        assert_eq!(admission_state(&db, "dead-orphan").await, "released");
        assert_eq!(
            table.stopped(),
            vec![4242],
            "only the orphan naming a dead call's session is stopped"
        );
        let live = crate::storage::get_admission_request(&db, "call-admission:live-this-boot")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (live.state.as_str(), live.lease_owner.as_deref()),
            ("leased", Some(this_boot.as_str())),
            "a lease this boot took is not requeued"
        );
        assert_eq!(
            run_status_now(&db, "live-this-boot").await.as_deref(),
            Some("starting")
        );
    }

    /// The periodic sweep heals the same state without a restart, and a lane
    /// whose dead rows held every unit admits again: a queued row whose run
    /// crashed is closed by the promoter rather than launched.
    #[tokio::test]
    async fn the_sweep_releases_dead_rows_and_promotion_never_launches_a_settled_run() {
        let (orch, db) = orch_with_db().await;
        seed_call_project(&db).await;
        seed_call_admission(
            &db,
            "dead-warm",
            "crashed",
            "active",
            Some("local-call-promoter"),
            1,
        )
        .await;
        register_call_process(&orch, "dead-warm");
        seed_call_admission(&db, "dead-queued", "crashed", "queued", None, 2).await;

        // Promotion alone reaches the queued row first: it must close it.
        promote_call_lanes(&orch);
        assert_eq!(admission_state(&db, "dead-queued").await, "cancelled");
        assert_eq!(orch.process_state.has_process("dead-queued"), Some(false));

        assert_eq!(heal_settled_call_admissions(&orch, None), 1);
        assert_eq!(admission_state(&db, "dead-warm").await, "released");
        assert_eq!(orch.process_state.has_process("dead-warm"), Some(false));
        assert_eq!(
            heal_settled_call_admissions(&orch, None),
            0,
            "a second pass finds nothing"
        );
    }

    /// The call lane's claim parked in the database: triggers fold into the
    /// running pass instead of claiming beside it, and its rerun claims the
    /// call enqueued after its own claim was taken. The call's run is already
    /// over, so the claim closes it rather than launching a process.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_parked_call_claim_folds_every_trigger_and_its_rerun_claims_what_they_enqueued() {
        use crate::execution::jobs::parked_writes::{
            hold_the_only_write_slot, until, waiting_from,
        };
        let (orch, db) = orch_with_db().await;
        seed_call_project(&db).await;
        seed_call_admission(&db, "late", "crashed", "queued", None, 1).await;
        db.execute(
            "UPDATE runtime_admission_requests SET resource_lane='elsewhere' WHERE request_id='call-admission:late'",
            (),
        )
        .await
        .unwrap();
        let (release, holder) = hold_the_only_write_slot(&db).await;

        let first = {
            let orch = orch.clone();
            tokio::task::spawn_blocking(move || promote_call_lane(&orch, "claude-cli", None))
        };
        until("the pass's claim parks", || {
            waiting_from(&db, "runtime_admission.rs") == 1
        })
        .await;
        let enqueue = {
            let db = db.clone();
            tokio::spawn(async move {
                db.execute(
                    "UPDATE runtime_admission_requests SET resource_lane='claude-cli' WHERE request_id='call-admission:late'",
                    (),
                )
                .await
                .unwrap();
            })
        };
        until("the enqueue waits behind the claim", || {
            db.pooled_writes_waiting() == 2
        })
        .await;
        let triggers: Vec<_> = (0..10)
            .map(|_| {
                let orch = orch.clone();
                tokio::task::spawn_blocking(move || promote_call_lane(&orch, "claude-cli", None))
            })
            .collect();
        for trigger in triggers {
            assert!(!trigger.await.unwrap(), "a folded trigger starts nothing");
        }
        assert_eq!(
            waiting_from(&db, "runtime_admission.rs"),
            1,
            "ten triggers against a parked claim add no claim beside it"
        );

        release.add_permits(1);
        holder.await.unwrap();
        enqueue.await.unwrap();
        first.await.unwrap();
        assert_eq!(
            admission_state(&db, "late").await,
            "cancelled",
            "the rerun claimed the call enqueued mid-pass"
        );
        assert_eq!(db.pooled_writes_waiting(), 0);
    }

    /// `cairn://executors`' Session admission section, as the read renders it.
    async fn session_admission_section(orch: &Orchestrator) -> String {
        crate::resources::executors::render_session_admission(
            &super::super::private_lane_budgets(orch).await.unwrap(),
            &super::super::call_lane_budgets(orch).await.unwrap(),
            chrono::Utc::now().timestamp(),
        )
    }

    /// cairn/6017: the call lane's last decision is named beside the ordinary
    /// lane of the same name. A call held back so queued sessions keep room
    /// says how much room it left them; once it leases, it reads as admitted.
    #[tokio::test]
    async fn the_session_admission_section_names_a_held_call_with_its_room_and_a_leased_one_as_admitted(
    ) {
        use crate::agent_process::memory::SystemMemory;
        // 16 GiB total, 4 GiB available: a 2 GiB reserve and 2 GiB of headroom.
        let orch = orch_with_db_and_probe(Some(SystemMemory {
            total: 16 << 30,
            available: 4 << 30,
        }))
        .await;
        let db = orch.db.local.clone();
        seed_call_project(&db).await;
        seed_call_admission(&db, "call-run", "starting", "queued", None, 1).await;
        // Two ordinary sessions queued on `claude-cli` in the private
        // workspace: the call (1 GiB) must leave 1 GiB for each of them.
        for n in 1..=2 {
            db.execute_script(format!(
                "INSERT INTO runtime_admission_requests(request_id,workspace_key,resource_lane,launch_key,claim_json,priority,enqueue_seq,state,lease_owner,lease_generation,lease_expires_at,created_at,updated_at)
                   VALUES('session-{n}','private-runtime','claude-cli','ordinary:session-{n}','{{\"residentProcessUnits\":1,\"logicalStreamUnits\":1}}',0,{},'queued',NULL,0,NULL,1,1);",
                10 + n
            ))
            .await
            .unwrap();
        }
        let concurrency = call_lane_concurrency(None);

        assert!(lease_next_call(&orch, "claude-cli", concurrency)
            .unwrap()
            .is_none());
        let held = session_admission_section(&orch).await;
        let session_line = held
            .lines()
            .find(|line| line.starts_with("- claude-cli: "))
            .expect("the ordinary lane is listed");
        assert!(
            session_line.starts_with("- claude-cli: 0 active, 2 queued,")
                && session_line.ends_with("; no admission pass has measured memory yet"),
            "the ordinary lane keeps its own (absent) decision: {held}"
        );
        assert!(
            held.contains("\n- claude-cli ephemeral calls: 0 active, 1 queued, fan-out cap 64; memory headroom 2.0 GiB above a 2.0 GiB reserve,"),
            "{held}"
        );
        assert!(
            held.contains("\n  - Last decision: held its queue head because it needs about 1.0 GiB and only 2.0 GiB is free above the 2.0 GiB reserve, of which it leaves 2.0 GiB for ordinary sessions.\n"),
            "{held}"
        );

        // One session leaves the queue: the call now fits beside the room it
        // still keeps for the other.
        db.execute(
            "UPDATE runtime_admission_requests SET state='cancelled' WHERE request_id='session-2'",
            (),
        )
        .await
        .unwrap();
        assert!(lease_next_call(&orch, "claude-cli", concurrency)
            .unwrap()
            .is_some());
        let admitted = session_admission_section(&orch).await;
        assert!(
            admitted
                .contains("\n- claude-cli ephemeral calls: 1 active, 0 queued, fan-out cap 64;"),
            "{admitted}"
        );
        assert!(
            admitted.contains("\n  - Last decision: admitted its queue head.\n"),
            "{admitted}"
        );
        assert!(!admitted.contains("held its queue head"), "{admitted}");
    }

    /// Regression for CAIRN-2526: a call must not be left at the default `Idle`
    /// occupancy while it runs. When the Claude reader's `was_host_interrupted`
    /// check read `occupancy == Idle` alone, an `Idle` call had every
    /// `tool_result` user event dropped as a post-interrupt transport artifact —
    /// which is why
    /// completed calls stored their `tool_use` rows but zero results and the
    /// monitor drill-in tool detail rendered forever "pending". A call establishes
    /// no DB turn, so unlike an ordinary run it never reaches the
    /// `set_current_turn_id -> begin_turn` that flips occupancy off `Idle`;
    /// `start_call_run` therefore marks the process active after spawning. This
    /// pins that contract at the occupancy layer the reader keys on.
    #[tokio::test]
    async fn call_process_marked_active_keeps_reader_from_dropping_tool_results() {
        use crate::agent_process::process::{RunHandle, RunOccupancy};
        let (orch, _db) = orch_with_db().await;
        let run_id = "call-run";
        {
            let mut processes = orch.process_state.processes.lock().unwrap();
            processes.register(
                run_id.to_string(),
                RunHandle::test_handle(Some("sess"), Some("j-call")),
            );
        }
        // A freshly registered process is Idle: the reader would read that as a
        // host interrupt and drop this call's tool_result events.
        assert!(matches!(
            orch.process_state.get_occupancy(run_id),
            Some(RunOccupancy::Idle)
        ));
        // The active-marking start_call_run performs after start_agent_session.
        assert!(orch.process_state.transition_to_active(run_id));
        // No longer Idle -> was_host_interrupted() is false -> tool_results persist.
        assert!(!matches!(
            orch.process_state.get_occupancy(run_id),
            Some(RunOccupancy::Idle)
        ));
    }
}
