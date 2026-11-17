use super::*;

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub(super) async fn run_planned_checks_at_commit<F, Fut, N, E>(
    db: Arc<LocalDb>,
    project_id: &str,
    commit: CheckRunCommit<'_>,
    tree_hash: &str,
    job_id: &str,
    plans: &[(CheckPlan, String)],
    tool_use_id: &str,
    mode: CheckExecMode,
    diagnostic_orch: Option<&Orchestrator>,
    execute: F,
    notify: N,
) -> Vec<CheckOutcome>
where
    F: Fn(usize, String, String) -> Fut,
    Fut: std::future::Future<Output = Result<CheckExecResult, E>>,
    E: Into<CheckExecutionFailure>,
    N: Fn(Vec<CheckStatusEntry>) + Send + Sync + 'static,
{
    let coordinates = sealed_coordinates(commit, tree_hash, plans);
    let infra_scopes = head_infra_scopes(plans.len(), job_id, commit.evaluated);
    run_planned_checks_at_commit_with_coordinates(
        db,
        project_id,
        commit,
        tree_hash,
        job_id,
        plans,
        &coordinates,
        &infra_scopes,
        tool_use_id,
        mode,
        diagnostic_orch,
        execute,
        notify,
    )
    .await
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub(super) async fn run_planned_checks_at_commit_with_env<F, Fut, N, E>(
    db: Arc<LocalDb>,
    project_id: &str,
    commit: CheckRunCommit<'_>,
    tree_hash: &str,
    job_id: &str,
    plans: &[(CheckPlan, String)],
    tool_use_id: &str,
    mode: CheckExecMode,
    diagnostic_orch: Option<&Orchestrator>,
    effective_env: &[(String, String)],
    execute: F,
    notify: N,
) -> Vec<CheckOutcome>
where
    F: Fn(usize, String, String) -> Fut,
    Fut: std::future::Future<Output = Result<CheckExecResult, E>>,
    E: Into<CheckExecutionFailure>,
    N: Fn(Vec<CheckStatusEntry>) + Send + Sync + 'static,
{
    let coordinates = sealed_coordinates(commit, tree_hash, plans);
    let infra_scopes = head_infra_scopes(plans.len(), job_id, commit.evaluated);
    run_planned_checks_at_commit_with_env_and_coordinates(
        db,
        project_id,
        commit,
        tree_hash,
        job_id,
        plans,
        &coordinates,
        &infra_scopes,
        tool_use_id,
        mode,
        diagnostic_orch,
        effective_env,
        execute,
        notify,
    )
    .await
}

pub(super) fn sealed_coordinates(
    commit: CheckRunCommit<'_>,
    tree_hash: &str,
    plans: &[(CheckPlan, String)],
) -> Vec<crate::execution::cache::SealedCheckCoordinate> {
    plans
        .iter()
        .map(|(_, input_hash)| {
            crate::execution::cache::SealedCheckCoordinate::new(
                commit.evaluated,
                commit.defined_by,
                tree_hash,
                input_hash,
            )
        })
        .collect()
}

pub(super) const SLOT_CHECK_DEV_DEBUG_ENV: (&str, &str) =
    ("CARGO_PROFILE_DEV_DEBUG", "line-tables-only");

pub(super) fn slot_check_env(mut env: Vec<(String, String)>) -> Vec<(String, String)> {
    env.retain(|(key, _)| key != SLOT_CHECK_DEV_DEBUG_ENV.0);
    env.push((
        SLOT_CHECK_DEV_DEBUG_ENV.0.to_string(),
        SLOT_CHECK_DEV_DEBUG_ENV.1.to_string(),
    ));
    env
}

/// The commit coordinates one planned check run is bound to.
///
/// `evaluated` and `defined_by` are distinct concepts and are recorded
/// separately: a verdict is only interpretable when you know both which content
/// was checked and which tree declared the check that produced it. They are the
/// same commit for every cadence; the write cadence's fix fold is the one
/// legitimate divergence, where a definition read from the sealed commit is
/// re-run against the commit the fix produced.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CheckRunCommit<'a> {
    /// The immutable commit whose content the checks evaluate.
    pub(crate) evaluated: &'a str,
    /// The commit whose `.cairn/config.yaml` declared these checks.
    pub(crate) defined_by: &'a str,
}

/// Test-only shorthand for a run with no commit coordinate of its own: the tree
/// identity stands in for both the evaluated and the defining commit. Production
/// callers name a real commit, because that is what makes a recorded verdict
/// attributable to a definition.
#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_planned_checks<F, Fut, N, E>(
    db: Arc<LocalDb>,
    project_id: &str,
    tree_hash: &str,
    job_id: &str,
    plans: &[(CheckPlan, String)],
    tool_use_id: &str,
    mode: CheckExecMode,
    diagnostic_orch: Option<&Orchestrator>,
    execute: F,
    notify: N,
) -> Vec<CheckOutcome>
where
    F: Fn(usize, String, String) -> Fut,
    Fut: std::future::Future<Output = Result<CheckExecResult, E>>,
    E: Into<CheckExecutionFailure>,
    N: Fn(Vec<CheckStatusEntry>) + Send + Sync + 'static,
{
    let commit = CheckRunCommit {
        evaluated: tree_hash,
        defined_by: tree_hash,
    };
    let coordinates = sealed_coordinates(commit, tree_hash, plans);
    let infra_scopes = head_infra_scopes(plans.len(), job_id, commit.evaluated);
    run_planned_checks_at_commit_with_coordinates(
        db,
        project_id,
        commit,
        tree_hash,
        job_id,
        plans,
        &coordinates,
        &infra_scopes,
        tool_use_id,
        mode,
        diagnostic_orch,
        execute,
        notify,
    )
    .await
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_planned_checks_at_commit_with_coordinates<F, Fut, N, E>(
    db: Arc<LocalDb>,
    project_id: &str,
    commit: CheckRunCommit<'_>,
    tree_hash: &str,
    job_id: &str,
    plans: &[(CheckPlan, String)],
    sealed_coordinates: &[crate::execution::cache::SealedCheckCoordinate],
    infra_scopes: &[InfraScopes],
    tool_use_id: &str,
    mode: CheckExecMode,
    diagnostic_orch: Option<&Orchestrator>,
    execute: F,
    notify: N,
) -> Vec<CheckOutcome>
where
    F: Fn(usize, String, String) -> Fut,
    Fut: std::future::Future<Output = Result<CheckExecResult, E>>,
    E: Into<CheckExecutionFailure>,
    N: Fn(Vec<CheckStatusEntry>) + Send + Sync + 'static,
{
    run_planned_checks_with_board(
        db,
        project_id,
        commit,
        tree_hash,
        job_id,
        plans,
        sealed_coordinates,
        infra_scopes,
        tool_use_id,
        mode,
        diagnostic_orch,
        None,
        &[],
        false,
        execute,
        notify,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn run_planned_checks_at_commit_with_env_and_coordinates<F, Fut, N, E>(
    db: Arc<LocalDb>,
    project_id: &str,
    commit: CheckRunCommit<'_>,
    tree_hash: &str,
    job_id: &str,
    plans: &[(CheckPlan, String)],
    sealed_coordinates: &[crate::execution::cache::SealedCheckCoordinate],
    infra_scopes: &[InfraScopes],
    tool_use_id: &str,
    mode: CheckExecMode,
    diagnostic_orch: Option<&Orchestrator>,
    effective_env: &[(String, String)],
    execute: F,
    notify: N,
) -> Vec<CheckOutcome>
where
    F: Fn(usize, String, String) -> Fut,
    Fut: std::future::Future<Output = Result<CheckExecResult, E>>,
    E: Into<CheckExecutionFailure>,
    N: Fn(Vec<CheckStatusEntry>) + Send + Sync + 'static,
{
    run_planned_checks_with_board(
        db,
        project_id,
        commit,
        tree_hash,
        job_id,
        plans,
        sealed_coordinates,
        infra_scopes,
        tool_use_id,
        mode,
        diagnostic_orch,
        None,
        effective_env,
        false,
        execute,
        notify,
    )
    .await
}

/// Why the verdict a manual run is about to report does not answer the
/// placement the run demanded, if it does not.
///
/// Reuse and coalescing each honour the demand on their own, so this should
/// stay empty. It is checked on the reply anyway because the caller reads a
/// `fresh` verdict as one produced where it asked, and an answer from anywhere
/// else must never read that way. A verdict whose platform or machine is
/// unknown is not flagged: there is nothing to compare.
pub(super) fn unmet_placement(
    placement: &crate::execution::check_placement::EffectiveCheckPlacement,
    executor: Option<&CheckMachine>,
    verdict_platform: Option<&str>,
) -> Option<String> {
    let machine = executor
        .map(|machine| format!(" on {}", machine.name))
        .unwrap_or_default();
    if let Some(platform) = verdict_platform {
        if !placement.verdict_platforms.is_empty()
            && !placement
                .verdict_platforms
                .iter()
                .any(|counted| counted.eq_ignore_ascii_case(platform))
        {
            return Some(format!(
                "this verdict was produced{machine} for {platform}, but this request counts only \
                 verdicts for {}",
                placement.verdict_platforms.join(", ")
            ));
        }
    }
    if let (Some(wanted), Some(machine)) = (placement.executor_name.as_deref(), executor) {
        if !cairn_common::executor_protocol::executor_names_match(&machine.name, wanted) {
            return Some(format!(
                "this verdict was produced on executor {}, but this request named executor \
                 {wanted}",
                machine.name
            ));
        }
    }
    None
}

pub(super) fn merge_batch_executor(
    items: &[PlannedCheckBatchItem],
) -> Result<Option<ExecutorSelector>, String> {
    fn merge_scalar(
        current: &mut Option<String>,
        incoming: &Option<String>,
        field: &str,
    ) -> Result<(), String> {
        let Some(incoming) = incoming else {
            return Ok(());
        };
        match current {
            Some(current) if current != incoming => Err(format!(
                "conflicting review check executor selector {field}: {current:?} vs {incoming:?}"
            )),
            Some(_) => Ok(()),
            None => {
                *current = Some(incoming.clone());
                Ok(())
            }
        }
    }

    let mut merged = ExecutorSelector::default();
    let mut toolchains = BTreeSet::new();
    for selector in items.iter().filter_map(|item| item.executor.as_ref()) {
        merge_scalar(&mut merged.name, &selector.name, "name")?;
        merge_scalar(&mut merged.os, &selector.os, "os")?;
        merged.requires_macos_nested_sandbox_write |= selector.requires_macos_nested_sandbox_write;
        // Containment merges strictly: a batch is placed by one decision, so it
        // must satisfy the union of what its members require. Two members
        // naming different process classes cannot both be honoured by one
        // placement, because the no-composition rule means neither seam's
        // evidence speaks for the other. That is a refusal, not a choice.
        if let Some(incoming) = &selector.containment {
            match &mut merged.containment {
                None => merged.containment = Some(incoming.clone()),
                Some(current) => {
                    if current.process_class != incoming.process_class {
                        return Err(format!(
                            "checks batched together require containment evidence for different process classes: {:?} vs {:?}",
                            current.process_class, incoming.process_class
                        ));
                    }
                    for requirement in &incoming.edges {
                        if !current.edges.contains(requirement) {
                            current.edges.push(requirement.clone());
                        }
                    }
                }
            }
        }
        toolchains.extend(selector.required_toolchains.iter().cloned());
    }
    merged.required_toolchains = toolchains.into_iter().collect();
    // Two checks in one batch can each be satisfiable while their union is not:
    // a named machine and a bare platform are different questions, and running
    // the batch under either alone would place a check somewhere it declined.
    if merged.name.is_some() && merged.os.is_some() {
        return Err(
            "checks batched together declare conflicting executor selectors: one names a machine and another names a platform"
                .into(),
        );
    }

    Ok((!merged.is_empty()).then_some(merged))
}

/// The platforms whose answer counts for EVERY check in one batch.
///
/// A batch is placed by a single decision, so the platforms it may be placed on
/// are the ones every member counts — an intersection, never a union. A member
/// that counts any platform constrains nothing and drops out of the fold.
///
/// An empty intersection is a conflict rather than a result, because empty is
/// already the vocabulary for "any platform counts": returning it would silently
/// invert two checks that agree on nothing into a batch that will run anywhere.
/// The pure-verdict path never reaches that, having already split the wave by
/// placement; the write cadence cannot split (its checks share one working tree,
/// in order), so there the conflict is real and is reported as the configuration
/// error it is.
pub(super) fn merge_batch_verdict_platforms(
    items: &[PlannedCheckBatchItem],
) -> Result<Vec<String>, String> {
    let mut merged: Option<Vec<String>> = None;
    for item in items {
        if item.verdict_platforms.is_empty() {
            continue;
        }
        merged = Some(match merged {
            None => item.verdict_platforms.clone(),
            Some(current) => current
                .into_iter()
                .filter(|platform| item.verdict_platforms.contains(platform))
                .collect(),
        });
        if merged.as_ref().is_some_and(|merged| merged.is_empty()) {
            return Err(format!(
                "checks batched together count their verdicts from disjoint platforms: {} counts only {}, which no other check in this batch shares",
                item.name,
                item.verdict_platforms.join(", ")
            ));
        }
    }
    Ok(merged.unwrap_or_default())
}

pub(crate) struct JobVerdictResult {
    pub(crate) coordinate: String,
    pub(crate) exit_code: Option<i32>,
    pub(crate) output: String,
    pub(crate) timed_out: bool,
}

/// Run one command against a job's current runner-owned branch coordinate in a
/// disposable pure-verdict cell. This is the canonical non-agent command path
/// for workflow checkpoints: it may lease execution capacity but cannot publish
/// a delta or become a ref authority.
pub(crate) async fn execute_job_verdict(
    orch: &Orchestrator,
    job_id: &str,
    name: &str,
    command: &str,
) -> Result<JobVerdictResult, String> {
    let db = crate::execution::routing::owning_db_for_job(&orch.db, job_id)
        .await
        .map_err(|error| error.to_string())?;
    let job_id_owned = job_id.to_string();
    let (project_id, project_key, branch, repository) = db
        .read(move |conn| {
            let job_id = job_id_owned.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT j.project_id, p.key, j.branch, p.repo_path
                         FROM jobs j
                         JOIN projects p ON p.id = j.project_id
                         WHERE j.id = ?1 AND j.branch IS NOT NULL
                         LIMIT 1",
                        (job_id.as_str(),),
                    )
                    .await?;
                let row = rows.next().await?.ok_or_else(|| {
                    crate::storage::DbError::Row(format!(
                        "checkpoint job {job_id} has no resolvable branch coordinate"
                    ))
                })?;
                Ok((row.text(0)?, row.text(1)?, row.text(2)?, row.text(3)?))
            })
        })
        .await
        .map_err(|error| error.to_string())?;

    let repository_path = std::path::PathBuf::from(&repository);
    let coordinate_repository = {
        let jj_binary_path = orch.jj_binary_path.clone();
        let config_dir = orch.config_dir.clone();
        let project_repo = repository_path.clone();
        tokio::task::spawn_blocking(move || {
            let jj = crate::jj::environment(&jj_binary_path, &config_dir);
            crate::jj::coordinate_repository(&jj, &config_dir, &project_repo)
        })
        .await
        .map_err(|error| format!("checkpoint coordinate repository task failed: {error}"))??
    };
    let store_dir = coordinate_repository.clone();
    let coordinate = cairn_vcs::resolve_coordinate(&coordinate_repository, &branch)
        .await
        .map_err(|error| format!("checkpoint branch '{branch}' is unresolvable: {error}"))?;

    let outcome = submit_planned_check(
        orch,
        PlannedCheckRequest {
            project_id: project_id.clone(),
            repository,
            store_dir,
            sealed_commit: coordinate.clone(),
            requesting_job_id: job_id.to_string(),
            owner: cairn_common::executor_protocol::CellOwnerRef {
                project_id,
                project_key: Some(project_key),
                issue_number: None,
                job_id: Some(job_id.to_string()),
                execution_seq: None,
                node_kind: Some(name.to_string()),
            },
            affinity_key: Some(check_affinity_key(job_id, name)),
            priority: CellPriority::ReviewCheck,
            // No PATH: a check resolves its tools against the PATH the executor
            // composed on the machine that runs it, the same as every other
            // check batch. Stating this host's PATH here would name directories
            // that exist on the runner and nowhere else.
            env: Vec::new(),
            item: PlannedCheckBatchItem {
                recording_plan: None,
                index: 0,
                name: name.to_string(),
                input_hash: coordinate.clone(),
                resource_identity: CheckResourceIdentityInput::Checkpoint {
                    name: name.to_string(),
                    command: command.to_string(),
                },
                command: command.to_string(),
                stream_id: format!("checkpoint:{job_id}"),
                env: Vec::new(),
                verdict_environment_names: Vec::new(),
                // Workflow checkpoints are check verdicts, so elapsed time has
                // no termination authority here either.
                timeout_ms: 0,
                executor: None,
                // A checkpoint is an undeclared command whose exit status gates a
                // workflow, which is a verdict by every meaning that matters. It
                // gets the same default as an undeclared project check: the
                // platform the runner is on.
                verdict_platforms: vec![
                    crate::execution::check_identity::gating_platform().to_string()
                ],
                resource_class: CheckResourceClass::Shared,
                // A checkpoint names no machine, so there is no placement
                // question to derive a fleet counter from.
                infra_scopes: InfraScopes::head_only(job_id, &coordinate),
                avoid_executor_ids: Vec::new(),
            },
            run_context: None,
            status_board: None,
            on_complete: None,
            purpose: None,
            publication: None,
        },
    )
    .await?;
    let result = outcome
        .results
        .into_iter()
        .find_map(|(index, result)| (index == 0).then_some(result))
        .ok_or_else(|| "checkpoint executor returned no result".to_string())?
        .map_err(|error| match error {
            CheckExecutionFailure::Process(error) => error,
            // The checkpoint's caller is a workflow surface, not an operator
            // console: it gets the composed half, and the substrate detail is
            // correlated here by job id and checkpoint name.
            CheckExecutionFailure::Substrate(failure) => {
                log::warn!(
                    "checkpoint check infrastructure failure: {}",
                    serde_json::json!({
                        "jobId": job_id,
                        "checkpoint": name,
                        "substrateDiagnostic": failure.diagnostic(),
                    })
                );
                failure.agent_message()
            }
            // The checkpoint's triple has spent its bounded retries on
            // infrastructure failures, so Cairn declined to run it. Say that,
            // rather than reporting a verdict the command never produced. The
            // refusal carries the rows it was taken on, so this names the real
            // diagnostic instead of pointing at an operator-only log.
            CheckExecutionFailure::Suppressed(suppressed) => suppressed_check_message(&suppressed),
            CheckExecutionFailure::NotPlaceable(reason)
            | CheckExecutionFailure::ConfigUnreadable(reason) => reason,
            CheckExecutionFailure::LeftRunning(attempt) => left_running_message(&attempt),
            CheckExecutionFailure::Cancelled(cause) => format!(
                "Cairn cancelled this check before it produced a verdict: {}.",
                cause.sentence()
            ),
        })?;
    Ok(JobVerdictResult {
        coordinate,
        exit_code: result.exit_code,
        output: result.output,
        timed_out: result.timed_out,
    })
}

/// The placement question one check asks, which is also what decides the fleet
/// counter it is bounded by.
pub(crate) struct CheckPlacementQuestion {
    pub selector: Option<ExecutorSelector>,
    pub verdict_platforms: Vec<String>,
    /// Whether the check's command compiles Rust, which placement holds to the
    /// machine's build-prerequisite verdict whatever the selector names.
    pub compiles_rust: bool,
}

impl CheckPlacementQuestion {
    /// Whether `command` is Rust work as placement classifies the request.
    pub(crate) fn command_compiles_rust(command: &str) -> bool {
        cairn_common::executor_protocol::CellCommandClass::classify(command).compiles_rust()
    }
}

/// The head and the repository a wave of checks runs against.
pub(crate) struct InfraScopeInputs<'a> {
    pub project_id: &'a str,
    pub job_id: &'a str,
    pub commit_sha: &'a str,
    pub repository: &'a str,
    pub mutation_policy: MutationPolicy,
    /// The class the wave's checks are placed as, which a machine's role
    /// accepts or refuses like any other eligibility fact.
    pub work_class: crate::fleet::PlacementWorkClass,
}

/// The suppression scope set for each planned check in a wave.
///
/// ONE producer, on purpose. The value has to be identical in two places that
/// sit in different call stacks — the reservation that refuses an item before
/// submission, and the row a returning failure writes — and resolving it
/// separately on each side is how the two would drift into charging different
/// counters for the same attempt. Callers resolve it once and hand the same
/// array to both.
///
/// `orch` is `None` wherever the caller genuinely cannot see the fleet, which
/// yields head-only scopes: the narrow, always-correct answer rather than a
/// guess at a capability set.
pub(crate) async fn plan_infra_scopes(
    orch: Option<&Orchestrator>,
    inputs: InfraScopeInputs<'_>,
    placements: &[CheckPlacementQuestion],
) -> Vec<InfraScopes> {
    let head = infra_suppression_scope(inputs.job_id, inputs.commit_sha);
    let mobility = batch_placement_mobility(&inputs.mutation_policy);
    let Some(orch) = orch else {
        return placements
            .iter()
            .map(|placement| InfraScopes {
                head: head.clone(),
                fleet: None,
                environments: Vec::new(),
                platform_heads: InfraScopes::platform_heads_for(
                    inputs.job_id,
                    inputs.commit_sha,
                    &placement.verdict_platforms,
                ),
                fleet_blind: Vec::new(),
                lane_breaker: false,
            })
            .collect();
    };
    // Read once for the whole wave: an operator's repair declaration is a fact
    // about a machine, not about a check.
    let repair_epochs = crate::fleet::repair::repair_epochs(&orch.db.local).await;
    let repository = RepositoryLocator::ColocatedPath {
        project_id: inputs.project_id.to_string(),
        repository_id: inputs.project_id.to_string(),
        absolute_path: inputs.repository.to_string(),
    };
    // The same requirements preparation stamps on the request, so the machines
    // these scopes count are the machines the lane can actually be placed on.
    let project_requirements = orch
        .fleet
        .project_requirements_at(inputs.project_id, std::path::Path::new(inputs.repository));
    placements
        .iter()
        .map(|placement| {
            let scope = crate::fleet::PlacementScope {
                project_id: inputs.project_id,
                repository: &repository,
                selector: placement
                    .selector
                    .as_ref()
                    .filter(|selector| !selector.is_empty()),
                pinned_executor_id: None,
                mobility,
                verdict_platforms: &placement.verdict_platforms,
                // What the fleet CAN do, never what suppression has steered
                // away from: feeding an avoid set back in would make both
                // identities move with the counters they are meant to bound.
                avoided_executor_ids: &[],
                project_requirements: &project_requirements,
                work_class: inputs.work_class,
                compiles_rust: placement.compiles_rust,
                pipe_floor: cairn_common::pipe_health::PipeFloorWork::Process,
                publication: None,
            };
            let environments: Vec<(String, String)> = orch
                .fleet
                .eligible_environments(scope, &repair_epochs)
                .into_iter()
                .map(|(executor_id, identity)| {
                    (
                        executor_id,
                        crate::execution::cache::infra_env_scope(&identity),
                    )
                })
                .collect();
            let eligible: Vec<String> = environments
                .iter()
                .map(|(executor_id, _)| executor_id.clone())
                .collect();
            InfraScopes {
                head: head.clone(),
                platform_heads: InfraScopes::platform_heads_for(
                    inputs.job_id,
                    inputs.commit_sha,
                    &placement.verdict_platforms,
                ),
                fleet: Some(crate::execution::cache::infra_fleet_scope(
                    &orch.fleet.capability_identity(scope, &repair_epochs),
                )),
                fleet_blind: orch
                    .fleet
                    .measurement_blindness(&eligible)
                    .into_iter()
                    .map(|machine| machine.name)
                    .collect(),
                environments,
                lane_breaker: false,
            }
        })
        .collect()
}

/// What the machines a review lane could be placed on are running now: the
/// fact a lane refused for capacity waits on. Asked of the same placement
/// scope a review wave's lanes are placed under.
pub(crate) fn review_placement_occupancy(
    orch: &Orchestrator,
    project_id: &str,
    repository: &str,
    placement: &CheckPlacementQuestion,
) -> crate::fleet::occupancy::MachineOccupancy {
    let locator = RepositoryLocator::ColocatedPath {
        project_id: project_id.to_string(),
        repository_id: project_id.to_string(),
        absolute_path: repository.to_string(),
    };
    let project_requirements = orch
        .fleet
        .project_requirements_at(project_id, Path::new(repository));
    orch.fleet.occupancy_for(crate::fleet::PlacementScope {
        project_id,
        repository: &locator,
        selector: placement
            .selector
            .as_ref()
            .filter(|selector| !selector.is_empty()),
        pinned_executor_id: None,
        mobility: batch_placement_mobility(&MutationPolicy::PureVerdict),
        verdict_platforms: &placement.verdict_platforms,
        avoided_executor_ids: &[],
        project_requirements: &project_requirements,
        work_class: crate::fleet::PlacementWorkClass::ReviewChecks,
        compiles_rust: placement.compiles_rust,
        pipe_floor: cairn_common::pipe_health::PipeFloorWork::Process,
        publication: None,
    })
}

/// Head-only scopes for every plan in a wave.
///
/// The honest resolution wherever the caller cannot see the selectors its checks
/// were placed by — a checkpoint, a colocated shell run — because a fleet
/// counter computed from the wrong placement question would bound the wrong set
/// of machines.
pub(crate) fn head_infra_scopes(count: usize, job_id: &str, commit_sha: &str) -> Vec<InfraScopes> {
    (0..count)
        .map(|_| InfraScopes::head_only(job_id, commit_sha))
        .collect()
}

/// Reserve one bounded retry for every item about to be launched, dropping the
/// items whose budget is spent.
///
/// This belongs at SUBMISSION because submission is what launches the command.
/// Both cadences build a batch, hand it to a build cell, and only afterwards
/// settle the results through the engine — so a decision taken later can discard
/// a result but cannot prevent the work: the cell has already run, the admission
/// is already spent, and the triple has already cost another execution. Every
/// path to a build cell passes through here, which is what makes the bound hold
/// no matter which cadence arrives first.
pub(crate) fn reserve_batch_items(
    db: Arc<LocalDb>,
    project_id: &str,
    items: Vec<PlannedCheckBatchItem>,
) -> (
    Vec<PlannedCheckBatchItem>,
    HashMap<usize, Result<CheckExecResult, CheckExecutionFailure>>,
) {
    let mut admitted = Vec::with_capacity(items.len());
    let mut refused: HashMap<usize, Result<CheckExecResult, CheckExecutionFailure>> =
        HashMap::new();
    for mut item in items {
        match claim_check_execution(
            db.clone(),
            project_id,
            &item.name,
            &item.input_hash,
            &item.infra_scopes,
        ) {
            Ok(CheckExecutionClaim::Suppressed(suppressed)) => {
                refused.insert(
                    item.index,
                    Err(CheckExecutionFailure::Suppressed(suppressed)),
                );
            }
            // Cleared, but not necessarily everywhere. A machine whose own
            // budget for this check is spent is steered away from rather than
            // refused: somewhere else may still be healthy, and a verdict from a
            // healthy machine beats a diagnostic from a broken one.
            Ok(CheckExecutionClaim::Clear {
                avoid,
                avoid_platforms,
            }) => {
                // Added to what the caller already ruled out, never in place
                // of it: a caller's avoidance is a placement requirement.
                item.avoid_executor_ids.extend(avoid);
                item.avoid_executor_ids.sort();
                item.avoid_executor_ids.dedup();
                // A platform whose own stall budget is spent at this head is
                // dropped from what this request counts. The claim refuses the
                // item outright when every platform is spent, so what is left
                // is never empty, which would read as "any platform".
                if !avoid_platforms.is_empty() {
                    item.verdict_platforms.retain(|platform| {
                        !avoid_platforms
                            .iter()
                            .any(|avoided| avoided.eq_ignore_ascii_case(platform))
                    });
                }
                admitted.push(item);
            }
            // Fail open. A reservation that could not be read must not become one
            // more way for a check to go unmeasured: the bound exists to end a
            // loop, not to start a different outage.
            Err(_) => admitted.push(item),
        }
    }
    (admitted, refused)
}

/// Remove every item no machine the fleet knows of could ever take, returning
/// each one's refusal by index.
///
/// Runs ahead of [`reserve_batch_items`] on purpose: nothing will be attempted,
/// so no retry may be reserved and no counter may move, and offering the item to
/// placement would only buy a refusal that is already known. The question is
/// re-asked on every submission, so the first one after a satisfying machine
/// attaches (or enrolls) runs the lane.
pub(crate) fn refuse_unplaceable_items(
    fleet: &crate::fleet::Fleet,
    items: &mut Vec<PlannedCheckBatchItem>,
) -> HashMap<usize, Result<CheckExecResult, CheckExecutionFailure>> {
    let mut refused = HashMap::new();
    items.retain(|item| {
        match fleet.unplaceable_reason(item.executor.as_ref(), &item.verdict_platforms) {
            Some(reason) => {
                refused.insert(item.index, Err(CheckExecutionFailure::NotPlaceable(reason)));
                false
            }
            None => true,
        }
    });
    refused
}

/// Remove every cell item while the project's canonical execution policy does
/// not load, returning each one's refusal by index.
///
/// Preparation reads that policy from the project checkout before it places
/// anything, and refuses every request in the project while it fails. Asked
/// here first, the refusal is known for what it is, a file this build cannot
/// read rather than a machine, so it is recorded as its own outcome and moves
/// no infrastructure streak. Like [`refuse_unplaceable_items`] it is re-asked
/// on every submission, so the first one after the file loads runs the lanes.
/// Runner-answered lanes are not passed in: they read no setup policy.
pub(super) async fn refuse_items_under_unreadable_config(
    orch: &Orchestrator,
    project_id: &str,
    items: &mut Vec<PlannedCheckBatchItem>,
) -> HashMap<usize, Result<CheckExecResult, CheckExecutionFailure>> {
    if items.is_empty() {
        return HashMap::new();
    }
    // Without a local checkout there is no canonical policy to read here, and
    // preparation answers for itself.
    let Ok((Some(project_path), _)) =
        crate::projects::crud::resolve_local_repo_path_and_key(&orch.db, project_id).await
    else {
        return HashMap::new();
    };
    let Err(error) = crate::config::project_settings::load_execution_project_policy(
        std::path::Path::new(&project_path),
    ) else {
        return HashMap::new();
    };
    let refusal = crate::fleet::project_config_refusal(&error);
    items
        .drain(..)
        .map(|item| {
            (
                item.index,
                Err(CheckExecutionFailure::ConfigUnreadable(refusal.clone())),
            )
        })
        .collect()
}

/// Pause before re-presenting a request whose selected executor disappeared or
/// otherwise returned a capacity-shaped outcome instead of keeping its queue entry.
pub(super) const CAPACITY_RETRY_BACKOFF_MS: u64 = 2_000;

pub(super) struct CheckPatience {
    pub(super) waiting_since_unix_ms: u64,
    pub(super) mobility: PlacementMobility,
    pub(super) work_class: crate::fleet::PlacementWorkClass,
}

#[derive(Clone)]
pub(super) struct GroupWait {
    pub(super) description: String,
    /// The relief this wait quoted, when the machines it waits on forecast one.
    pub(super) relief_ms: Option<u64>,
    /// What holds the machines has no measured duration, or has outlived it.
    pub(super) unforecastable: bool,
}

impl CheckPatience {
    pub(super) fn declare(batch: &PlannedCheckBatchRequest) -> Self {
        Self {
            waiting_since_unix_ms: unix_time_ms_for_checks(),
            mobility: batch_placement_mobility(&batch.mutation_policy),
            work_class: crate::execution::wire::check_placement_work_class(batch.priority),
        }
    }

    pub(super) fn for_group(
        &self,
        orch: &Orchestrator,
        batch: &PlannedCheckBatchRequest,
        selector: Option<&ExecutorSelector>,
        verdict_platforms: &[String],
        avoided_executor_ids: &[String],
    ) -> GroupWait {
        let repository = RepositoryLocator::ColocatedPath {
            project_id: batch.project_id.clone(),
            repository_id: batch.project_id.clone(),
            absolute_path: batch.repository.clone(),
        };
        // Scoped by the avoid set as well as the selector: a group must never be
        // told it is waiting on machines that could not take it anyway.
        let project_requirements = orch
            .fleet
            .project_requirements_at(&batch.project_id, std::path::Path::new(&batch.repository));
        GroupWait::from_occupancy(orch.fleet.occupancy_for(crate::fleet::PlacementScope {
            project_id: &batch.project_id,
            repository: &repository,
            selector,
            pinned_executor_id: None,
            mobility: self.mobility,
            verdict_platforms,
            avoided_executor_ids,
            project_requirements: &project_requirements,
            work_class: self.work_class,
            // The class the batch's request is placed with.
            compiles_rust:
                crate::execution::wire::batch_command_class(&batch.items).compiles_rust(),
            pipe_floor: cairn_common::pipe_health::PipeFloorWork::Process,
            publication: None,
        }))
    }
}

impl GroupWait {
    pub(super) fn from_occupancy(occupancy: crate::fleet::occupancy::MachineOccupancy) -> Self {
        match occupancy {
            crate::fleet::occupancy::MachineOccupancy::Predicted(forecast) => {
                // Every occupant of the machine, not the ones blocking this
                // request: a count of blockers would claim a queue position
                // the occupancy read does not know.
                let others = match forecast.occupant_count.saturating_sub(1) {
                    0 => String::new(),
                    1 => ", with 1 other cell running there".to_string(),
                    more => format!(", with {more} other cells running there"),
                };
                Self {
                    description: format!(
                        "parked behind {}, predicted to finish in {}{others}; holding this check's queue entry until capacity is released",
                        forecast.blocking,
                        describe_ms(forecast.relief_ms),
                    ),
                    relief_ms: Some(forecast.relief_ms),
                    unforecastable: false,
                }
            }
            crate::fleet::occupancy::MachineOccupancy::Unforecastable => Self {
                description: "parked behind resident or external work with no measured duration; holding this check's queue entry until capacity is released".into(),
                relief_ms: None,
                unforecastable: true,
            },
            crate::fleet::occupancy::MachineOccupancy::Idle => Self {
                description: "parked in the admission queue; holding this check's queue entry until capacity is released".into(),
                relief_ms: None,
                unforecastable: false,
            },
        }
    }
}

/// Ask the fleet for one suite's verdict. The request is placed and priced by
/// itself; a caller wanting several suites submits several requests.
pub(crate) async fn submit_planned_check(
    orch: &Orchestrator,
    request: PlannedCheckRequest,
) -> Result<PlannedCheckBatchOutcome, String> {
    submit_admitted_check_batch(orch, request.into_batch()).await
}

/// Run a change-returning lease: its items in order in one cell, whose
/// changes come back with the results. A verdict is never a lease.
pub(crate) async fn submit_check_lease(
    orch: &Orchestrator,
    lease: PlannedCheckBatchRequest,
) -> Result<PlannedCheckBatchOutcome, String> {
    if lease.mutation_policy == MutationPolicy::PureVerdict {
        return Err(
            "a verdict is asked for one suite at a time; a lease is only for work whose changes come back"
                .into(),
        );
    }
    submit_admitted_check_batch(orch, lease).await
}

pub(super) async fn submit_admitted_check_batch(
    orch: &Orchestrator,
    mut batch: PlannedCheckBatchRequest,
) -> Result<PlannedCheckBatchOutcome, String> {
    use crate::execution::impact_slice::{run_lanes, runner_globs};
    batch.on_group_complete = batch
        .on_group_complete
        .take()
        .map(complete_each_member_once);
    // `tree: impact` lanes the runner can answer never meet the fleet, so the
    // fleet's placeability says nothing about them; only cell lanes are asked.
    let (runner_items, mut cell_items): (Vec<_>, Vec<_>) = std::mem::take(&mut batch.items)
        .into_iter()
        .partition(|item| runner_globs(item).is_some());
    let unreadable =
        refuse_items_under_unreadable_config(orch, &batch.project_id, &mut cell_items).await;
    let unplaceable = refuse_unplaceable_items(&orch.fleet, &mut cell_items);
    cell_items.extend(runner_items);
    let (admitted, mut refused) =
        reserve_batch_items(orch.db.local.clone(), &batch.project_id, cell_items);
    refused.extend(unreadable);
    refused.extend(unplaceable);
    // Runner lanes leave the batch after the reservation (so their suppression
    // budgets are the cell path's own) and run beside the cell lanes rather
    // than ahead of them, so a slice never delays a cell in the same batch.
    let (sliced, celled): (Vec<_>, Vec<_>) = admitted
        .into_iter()
        .partition(|item| runner_globs(item).is_some());
    batch.items = celled;
    let sliced = sliced
        .into_iter()
        .filter_map(|item| runner_globs(&item).map(|globs| (item, globs)))
        .collect::<Vec<_>>();
    if let Some(complete) = &batch.on_group_complete {
        if !refused.is_empty() {
            complete(&refused).await;
        }
    }
    let slice_batch = batch.clone();
    let slice_run = async move {
        let results = if sliced.is_empty() {
            HashMap::new()
        } else {
            run_lanes(&slice_batch, sliced).await
        };
        if let Some(complete) = &slice_batch.on_group_complete {
            if !results.is_empty() {
                complete(&results).await;
            }
        }
        results
    };
    // Nothing survived the reservation for a cell: request no cell, so a fully
    // suppressed (or fully runner-answered) suite costs no build-slot admission.
    if batch.items.is_empty() {
        refused.extend(slice_run.await);
        return Ok(PlannedCheckBatchOutcome {
            results: refused,
            request: None,
            delta: None,
            store_dir: Some(batch.store_dir.clone()),
        });
    }
    let cell_run = async {
        // The reservation above is claimed ONCE and the retries below sit inside
        // it, so an immediate retry cannot spend the persisted per-check
        // infrastructure-failure budget. That budget remains the outer circuit
        // breaker across cadences; this loop is the inner one within a single ask.
        let patience = CheckPatience::declare(&batch);
        let names = batch
            .items
            .iter()
            .map(|item| item.name.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        loop {
            let presented =
                off_frame(|| submit_reserved_check_batch(orch, batch.clone(), &patience)).await?;
            let CapacityRetry::Again { backoff } = capacity_retry_decision(&presented.outcome)
            else {
                break Ok::<_, String>(presented.outcome);
            };
            log::info!(
                "check batch [{names}] retained no executor queue entry after a capacity response; re-presenting for placement — {}",
                describe_capacity_waits(&presented.outcome)
            );
            tokio::time::sleep(backoff).await;
        }
    };
    let (slice_results, outcome) = tokio::join!(slice_run, cell_run);
    let mut outcome = outcome?;
    outcome.results.extend(refused);
    outcome.results.extend(slice_results);
    Ok(outcome)
}

/// Fold what the batch waited on into the capacity refusals it is about to
/// return.
///
/// A verdictless red row that says only "there was no room" leaves its reader
/// with no next step, and the reader who most needs one is an operator watching
/// their own fleet refuse their own work. Naming the occupant is not substrate
/// detail: an agent cannot act on a cell id or a queue position, but "queued
/// behind CAIRN-3414's rust-tests" is a fact about the world that explains the
/// row and predicts its own recovery.
///
/// Only capacity-shaped failures are touched. Every other shape describes
/// something that was never about waiting.
pub(super) fn attribute_capacity_wait(outcome: &mut PlannedCheckBatchOutcome, wait: &GroupWait) {
    for result in outcome.results.values_mut() {
        if let Err(CheckExecutionFailure::Substrate(failure)) = result {
            if failure.shape() == SubstrateFailureShape::Capacity {
                failure.name_the_wait(&wait.description);
            }
        }
    }
}

/// The distinct waits a batch's capacity refusals were attributed to, for one
/// operator log line.
///
/// Read back off the refusals rather than recomputed, so the log and the rows
/// cannot disagree, and distinct because a batch split across selectors was
/// genuinely queued behind different work in each group.
pub(super) fn describe_capacity_waits(outcome: &PlannedCheckBatchOutcome) -> String {
    let mut waits: Vec<&str> = outcome
        .results
        .values()
        .filter_map(|result| match result {
            Err(CheckExecutionFailure::Substrate(failure)) => failure.waited_on(),
            _ => None,
        })
        .collect();
    waits.sort_unstable();
    waits.dedup();
    waits.join("; also ")
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum CapacityRetry {
    /// Present the same request again after this pause.
    Again { backoff: std::time::Duration },
    /// Let the outcome stand as the batch's answer.
    Surface,
}

/// Whether to present this batch again, from the outcome it just got.
///
/// Two conditions, both required. The bound must not be spent — the retries are
/// an inner circuit breaker within one ask, and the persisted per-check
/// infrastructure-failure bound remains the outer one across cadences. And the
/// refusal must be WHOLLY transient: a batch may split across machines, so a
/// partial outcome means some of it already ran, and re-presenting the whole
/// request would spend the machine twice for an answer it already has. A
/// structural refusal (no matching machine, a toolchain fault, a draining host),
/// a cancellation, a storage failure, and anything that happened after the
/// command ran are all re-presented unchanged, so none of them retry. A terminal
/// placement refusal also surfaces, even if its cause is transient: placement
/// has already exhausted that wait. The next commit or changed placement fact
/// may re-arm the lane, but this ask must not restart the expired wait.
pub(super) fn capacity_retry_decision(outcome: &PlannedCheckBatchOutcome) -> CapacityRetry {
    let wholly_capacity =
        !outcome.results.is_empty() && outcome.results.values().all(awaits_capacity_retry);
    if wholly_capacity {
        CapacityRetry::Again {
            backoff: std::time::Duration::from_millis(CAPACITY_RETRY_BACKOFF_MS),
        }
    } else {
        CapacityRetry::Surface
    }
}

/// Whether one member's result is a transient refusal that a batch made only of
/// such refusals presents again ([`capacity_retry_decision`]). A member whose
/// result is not can never be re-presented, so it may be recorded the moment
/// it lands.
pub(super) fn awaits_capacity_retry(
    result: &Result<CheckExecResult, CheckExecutionFailure>,
) -> bool {
    matches!(
        result,
        Err(CheckExecutionFailure::Substrate(failure))
            if !failure.placement_refused && matches!(
                failure.shape(),
                SubstrateFailureShape::Capacity
                    | SubstrateFailureShape::EnvironmentRetired
                    | SubstrateFailureShape::CommandPreempted
            )
    )
}

/// `complete`, answering each member once: a member handed over as it
/// finished is left out when its group completes. Every path that hands a
/// result to a batch's completion shares the one wrapper the batch carries, so
/// a group's final call records only what its members' early ones did not.
///
/// A member counts as answered only once its recording has returned without
/// reporting it unrecorded, so a member whose early write failed is recorded
/// by the next call that carries it: the group's final call at the latest.
/// Calls run one at a time, so a group's final call waits for an early
/// recording still in flight instead of racing it into a second row.
///
/// The recording, and the claim that follows it, run in a task of their own
/// that holds the lock until the claim is made, so dropping a call changes
/// nothing: its recording finishes, claims only what it persisted, and only
/// then lets the next call look. A recording that panics claims nothing.
pub(crate) fn complete_each_member_once(complete: CheckGroupCompletion) -> CheckGroupCompletion {
    let answered = Arc::new(tokio::sync::Mutex::new(
        std::collections::HashSet::<usize>::new(),
    ));
    Arc::new(move |results: &CheckGroupResults| {
        let answered = answered.clone();
        let complete = complete.clone();
        Box::pin(async move {
            let mut answered = answered.lock_owned().await;
            let fresh: CheckGroupResults = results
                .iter()
                .filter(|(index, _)| !answered.contains(*index))
                .map(|(index, result)| (*index, result.clone()))
                .collect();
            if fresh.is_empty() {
                return crate::execution::checks::CheckGroupUnrecorded::new();
            }
            let members: crate::execution::checks::CheckGroupUnrecorded =
                fresh.keys().copied().collect();
            let recording = tokio::spawn(async move {
                let unrecorded = complete(&fresh).await;
                answered.extend(fresh.keys().filter(|index| !unrecorded.contains(*index)));
                unrecorded
            });
            recording.await.unwrap_or(members)
        })
    })
}

pub(super) struct PresentedBatch {
    pub(super) outcome: PlannedCheckBatchOutcome,
}

pub(super) async fn submit_reserved_check_batch(
    orch: &Orchestrator,
    batch: PlannedCheckBatchRequest,
    patience: &CheckPatience,
) -> Result<PresentedBatch, String> {
    // The selector is whatever the items agree on. A conflict is a
    // configuration error the submission below reports; there is no forecast to
    // scope by a selector that cannot exist, so it reads as unconstrained here.
    let (forecast_selector, forecast_verdict_platforms) =
        merge_batch_selector(&batch.items).unwrap_or_default();
    let forecast_avoid = merge_batch_avoid(&batch.items).unwrap_or_default();
    let wait = patience.for_group(
        orch,
        &batch,
        forecast_selector.as_ref(),
        &forecast_verdict_platforms,
        &forecast_avoid,
    );
    let complete = batch.on_group_complete.clone();
    let mut outcome = submit_single_planned_check_batch(orch, batch, &wait, patience).await?;
    attribute_capacity_wait(&mut outcome, &wait);
    if capacity_retry_decision(&outcome) == CapacityRetry::Surface {
        if let Some(complete) = complete {
            complete(&outcome.results).await;
        }
    }
    Ok(PresentedBatch { outcome })
}

/// A batch's executor selector and the platforms it may be placed on, merged
/// over its items.
///
/// A requirement that means something on one platform only is dropped when
/// the batch cannot land there: `requiresMacosNestedSandboxWrite` constrains
/// macOS machines alone, so a batch that only Linux can run carries no such
/// requirement, and placement does not wait on a capability report that
/// cannot matter.
pub(super) fn merge_batch_selector(
    items: &[PlannedCheckBatchItem],
) -> Result<(Option<ExecutorSelector>, Vec<String>), String> {
    let verdict_platforms = merge_batch_verdict_platforms(items)?;
    let executor = merge_batch_executor(items)?
        .map(|mut selector| {
            if !verdict_platforms.is_empty()
                && !verdict_platforms
                    .iter()
                    .any(|platform| platform.eq_ignore_ascii_case("macos"))
            {
                selector.requires_macos_nested_sandbox_write = false;
            }
            selector
        })
        .filter(|selector| !selector.is_empty());
    Ok((executor, verdict_platforms))
}

/// The one set of machines a batch may not be placed on.
///
/// A batch travels as one cell, so the whole of it goes to one machine. Where
/// its items agree, this is a read. Where they disagree — one check suppressed
/// on A, another on B — there is no single placement that satisfies both. A
/// verdict request carries one suite and so can never disagree with itself;
/// a disagreement belongs to a lease, which CANNOT split because its items
/// must share one cell's changes.
///
/// For those, the honest statement is that no single machine can answer right
/// now. It is charged to the head domain by its `Dispatch` shape, which is
/// correct: this is a coalescing conflict, not a fleet capability gap, and it
/// clears the moment either environment is repaired.
pub(super) fn merge_batch_avoid(items: &[PlannedCheckBatchItem]) -> Result<Vec<String>, String> {
    let mut eligible: Vec<String> = Vec::new();
    for item in items {
        for (executor_id, _) in &item.infra_scopes.environments {
            if !eligible.contains(executor_id) {
                eligible.push(executor_id.clone());
            }
        }
    }
    // No item is bounded by any machine's environment, so there is nothing to
    // steer around and the grouping is exactly what it was before avoidance.
    if eligible.is_empty() {
        return Ok(Vec::new());
    }
    let mut avoid: Vec<String> = eligible
        .iter()
        .filter(|id| {
            items
                .iter()
                .any(|item| item.avoid_executor_ids.contains(id))
        })
        .cloned()
        .collect();
    if avoid.len() == eligible.len() {
        return Err(format!(
            "no single machine is clear for every check in this batch, and this batch cannot be split: {}",
            items
                .iter()
                .filter(|item| !item.avoid_executor_ids.is_empty())
                .map(|item| format!(
                    "{} is infrastructure-suppressed on {}",
                    item.name,
                    item.avoid_executor_ids.join(", ")
                ))
                .collect::<Vec<_>>()
                .join("; ")
        ));
    }
    avoid.sort();
    Ok(avoid)
}

pub(super) async fn submit_single_planned_check_batch(
    orch: &Orchestrator,
    batch: PlannedCheckBatchRequest,
    wait: &GroupWait,
    patience: &CheckPatience,
) -> Result<PlannedCheckBatchOutcome, String> {
    // Configuration conflicts are deterministic caller errors and must surface
    // before any transient infrastructure preflight can obscure them.
    if batch.mutation_policy == MutationPolicy::PureVerdict && batch.items.len() != 1 {
        return Err(format!(
            "a verdict request carries exactly one suite, not {}",
            batch.items.len()
        ));
    }
    let (executor, verdict_platforms) = merge_batch_selector(&batch.items)?;
    // Unlike the two above, this is not a configuration error: it is a live
    // condition of the fleet, so it is reported as a substrate refusal per item
    // rather than as a caller mistake. Nothing is dispatched either way.
    let avoided_executor_ids = match merge_batch_avoid(&batch.items) {
        Ok(avoided) => avoided,
        Err(conflict) => {
            return Ok(PlannedCheckBatchOutcome::failed(
                batch.items.iter().map(|item| item.index).collect(),
                SubstrateFailure::new(SubstrateFailureShape::Dispatch, conflict),
            ))
        }
    };
    if let Some(failure) =
        active_build_service_failure(&orch.build_service_diagnostic_snapshot("sccache"))
    {
        return Ok(PlannedCheckBatchOutcome::failed(
            batch.items.iter().map(|item| item.index).collect(),
            SubstrateFailure::new(SubstrateFailureShape::Dispatch, failure)
                .implicating_build_service(),
        ));
    }
    let request_id = uuid::Uuid::new_v4().to_string();
    let mut temporary_ref = match publish_check_commit_ref(
        orch,
        Path::new(&batch.repository),
        &batch.store_dir,
        &batch.sealed_commit,
        &request_id,
    )
    .await
    {
        Ok(reference) => reference,
        Err(error) => {
            return Ok(PlannedCheckBatchOutcome::failed(
                batch.items.iter().map(|item| item.index).collect(),
                SubstrateFailure::new(SubstrateFailureShape::Dispatch, error),
            ))
        }
    };
    // What the parked forecast is re-read against while the batch waits.
    let wait_scope = (
        executor.clone(),
        verdict_platforms.clone(),
        avoided_executor_ids.clone(),
    );
    let wire = build_check_batch_wire(
        &batch,
        request_id,
        uuid::Uuid::new_v4().to_string(),
        executor,
        verdict_platforms,
        avoided_executor_ids,
    );
    let indexed: Vec<_> = batch.items.iter().map(|item| item.index).collect();
    let mut request = wire.request;
    // All groups, including capacity retries, share the wave's waiting origin.
    // Check requests retain the wire's durable parked horizon; elapsed time has
    // no authority to turn an unanswered check into a verdict.
    request.waiting_since_unix_ms = patience.waiting_since_unix_ms;
    let items = wire.items;
    let stream = batch
        .items
        .first()
        .map(|item| item.stream_id.as_str())
        .unwrap_or("");
    let (consumer, consumer_ref) = check_consumer_identity(
        orch,
        &batch.requesting_job_id,
        stream,
        batch.purpose.as_ref(),
    )
    .await?;
    let adoption_claim = crate::storage::check_lane_adoptions::Claim {
        request_id: request.request_id.clone(),
        attempt_id: request.attempt_id.clone(),
        project_id: batch.project_id.clone(),
        check_names: batch.items.iter().map(|item| item.name.clone()).collect(),
        recording_plans: batch
            .items
            .iter()
            .map(|item| {
                serde_json::to_value(&item.recording_plan).unwrap_or(serde_json::Value::Null)
            })
            .collect(),
        platform: request
            .executor
            .as_ref()
            .and_then(|selector| selector.os.clone())
            .or_else(|| {
                batch
                    .items
                    .first()
                    .and_then(|item| item.verdict_platforms.first().cloned())
            })
            .unwrap_or_else(|| "any".to_string()),
        commit_sha: batch.sealed_commit.clone(),
        input_hashes: batch
            .items
            .iter()
            .map(|item| item.input_hash.clone())
            .collect(),
        consumer: consumer.to_string(),
        consumer_ref,
        executor_id: String::new(),
        machine: String::new(),
        granted_at_ms: 0,
        state: "running".to_string(),
        stopped_at_ms: None,
        evidence: None,
        observation_id: None,
        updated_ms: 0,
    };
    let identity = (request.request_id.clone(), request.attempt_id.clone());
    orch.fleet
        .register_check_claim(identity.clone(), adoption_claim.clone());
    let mutation_policy = request.mutation_policy.clone();
    let direct_consumer_guards: Vec<_> = if mutation_policy != MutationPolicy::PureVerdict {
        batch
            .items
            .iter()
            .map(|item| {
                orch.fleet
                    .check_consumer_guard_for(&identity, &item.name, &adoption_claim)
            })
            .collect()
    } else {
        Vec::new()
    };
    if let Some(board) = &batch.status_board {
        board.note_request(&request.request_id);
        board.set_phase(
            Some("queued"),
            Some("queued"),
            Some(wait.description.clone()),
        );
    }
    let submitted_at = std::time::Instant::now();
    let submitted = if mutation_policy == MutationPolicy::PureVerdict {
        let subscriber = crate::fleet::Fleet::check_subscriber(&request);
        let item = items
            .into_iter()
            .next()
            .expect("a verdict request was checked to carry one suite");
        let wait_batch = PlannedCheckBatchRequest {
            items: Vec::new(),
            status_board: None,
            ..batch.clone()
        };
        let board = batch.status_board.clone();
        // Built off-frame: the fleet's submission future is the bulk of every
        // check-request future above it, and held inline it is paid for again
        // in each of their poll frames (see `cairn_platform::stack`).
        let submission = off_frame(|| {
            orch.fleet.submit_pure_verdict_check(
                orch,
                request.clone(),
                item,
                batch.run_context,
                // Only a board whose verdicts the engine owns takes the
                // executor's per-item reports; any other pure-verdict board
                // would read an exit code as a verdict. An observed board
                // takes them too: its caller reads output, never a verdict,
                // from it.
                batch
                    .status_board
                    .clone()
                    .filter(|board| board.engine_owns_verdicts() || board.is_observed()),
                batch.purpose,
            )
        });
        match board {
            None => submission.await,
            Some(board) => {
                narrate_dispatch(
                    submission,
                    wait,
                    || check_dispatch(&orch.fleet, &subscriber),
                    || {
                        patience.for_group(
                            orch,
                            &wait_batch,
                            wait_scope.0.as_ref(),
                            &wait_scope.1,
                            &wait_scope.2,
                        )
                    },
                    |status| board.show_phase(status),
                )
                .await
            }
        }
    } else {
        let processes = items.into_iter().map(|item| item.process).collect();
        // The queued row says what it is queued BEHIND. "Waiting for build
        // slot" told an operator watching their own fleet only that Cairn was
        // waiting on Cairn, which is the one thing they could already see.
        // A write-check batch is one queue entry under its own identity, never
        // coalesced, so where it stands is read by that identity; the claim
        // and every decline then reach the board, and through it the
        // committing call's inline wait.
        let identity = (request.request_id.clone(), request.attempt_id.clone());
        let wait_batch = PlannedCheckBatchRequest {
            items: Vec::new(),
            status_board: None,
            ..batch.clone()
        };
        let board = batch.status_board.clone();
        // Off-frame for the same reason as the pure-verdict submission above.
        let submission = off_frame(|| {
            orch.fleet.submit_write_check_batch(
                orch,
                request.clone(),
                processes,
                batch.run_context,
                batch.status_board.clone(),
            )
        });
        let executed = match board {
            None => submission.await,
            Some(board) => {
                narrate_dispatch(
                    submission,
                    wait,
                    || {
                        orch.fleet.request_dispatch_phase(&identity).map(|phase| {
                            crate::fleet::CheckDispatch {
                                phase,
                                joined: None,
                            }
                        })
                    },
                    || {
                        patience.for_group(
                            orch,
                            &wait_batch,
                            wait_scope.0.as_ref(),
                            &wait_scope.1,
                            &wait_scope.2,
                        )
                    },
                    |status| board.show_phase(status),
                )
                .await
            }
        };
        let (mut item_outcomes, delta) =
            split_write_check_batch_outcome(executed.outcome, &executed.account, indexed.len());
        for (item, guard) in item_outcomes.iter_mut().zip(direct_consumer_guards) {
            if let Ok(result) = item {
                result.publication = Some(
                    crate::fleet::PublicationCoordination::for_request(identity.clone())
                        .with_consumer_guard(guard),
                );
            }
        }
        let mut results = indexed.iter().copied().zip(item_outcomes).collect();
        stamp_substrate_elapsed(&mut results, submitted_at.elapsed());
        settle_unrecordable_delivery(orch, &request, &results).await;
        temporary_ref.retire();
        return Ok(PlannedCheckBatchOutcome {
            results,
            request: Some(request),
            delta,
            store_dir: Some(batch.store_dir),
        });
    };
    // Measured and answered before the sealed ref is released, so a lane's
    // outcome is reported at its own time.
    let left_running = orch
        .fleet
        .check_left_running(&(request.request_id.clone(), request.attempt_id.clone()))
        .and_then(|claim| {
            orch.fleet.runner_stopping_since().map(|stopped_at_ms| {
                crate::fleet::LeftRunningAttempt {
                    machine: claim.machine,
                    request_id: claim.request_id,
                    attempt_id: claim.attempt_id,
                    executor_id: claim.executor_id,
                    stopped_at_ms,
                }
            })
        });
    let result = match left_running {
        Some(attempt) => Err(CheckExecutionFailure::LeftRunning(attempt)),
        None => check_result_from_delivery(submitted),
    };
    let mut results = HashMap::from([(indexed[0], result)]);
    stamp_substrate_elapsed(&mut results, submitted_at.elapsed());
    settle_unrecordable_delivery(orch, &request, &results).await;
    temporary_ref.retire();
    Ok(PlannedCheckBatchOutcome {
        results,
        request: Some(request),
        delta: None,
        store_dir: Some(batch.store_dir),
    })
}

pub(super) async fn lose_check_claim(
    orch: &Orchestrator,
    identity: &(String, String),
    evidence: &str,
) {
    let Some(claim) = orch.fleet.check_claim(identity) else {
        return;
    };
    loop {
        let settled = crate::storage::check_lane_adoptions::settle_failure(
            &orch.db.local,
            &identity.0,
            &identity.1,
            "lost",
            evidence,
        )
        .await;
        match settled {
            Ok(()) => {
                orch.fleet
                    .cancel_adopted(&identity.0, &identity.1, &claim.executor_id);
                return;
            }
            Err(error) => {
                log::error!("could not settle check claim {}: {error}; retaining the result and retrying without acknowledgement", identity.0);
                tokio::time::sleep(crate::fleet::RETAINED_RESULT_RETRY).await;
            }
        }
    }
}

pub(super) async fn settle_unrecordable_delivery(
    orch: &Orchestrator,
    request: &CellRequest,
    results: &CheckGroupResults,
) {
    if results.values().any(|result| matches!(result, Err(error) if !matches!(error, CheckExecutionFailure::LeftRunning(_)))) {
        lose_check_claim(
            orch, &(request.request_id.clone(), request.attempt_id.clone()),
            "the granted check did not deliver every item to its recorder",
        ).await;
    }
}

/// Why a branch's tip moved, as far as a review wave running on it is concerned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BranchAdvance {
    /// A commit sealed onto the branch: the change the branch claims is a
    /// different change now.
    Commit,
    /// The branch was replayed onto an advanced base. `patch_unchanged` is
    /// whether its own diff against its fork point is the same before and after
    /// the replay ([`crate::jj::own_patch_against`]).
    BaseReplay { patch_unchanged: bool },
}

impl BranchAdvance {
    /// Whether a wave verifying the pre-advance tip is no longer worth finishing.
    ///
    /// A replay that kept the patch leaves the branch claiming the same change,
    /// so the wave's verdict is still the verdict about that change; the new
    /// base's own reds belong to the base. Withdrawing it would buy nothing, and
    /// when merges land faster than a long lane runs it guarantees the branch
    /// never records a verdict at all (cairn/4210: four waves withdrawn in
    /// fifteen minutes, zero rows).
    pub(crate) fn supersedes_review(self) -> bool {
        !matches!(
            self,
            Self::BaseReplay {
                patch_unchanged: true
            }
        )
    }
}

/// Cancel any in-flight `when:review` check suite for `job_id` when its branch
/// advances in a way that changes its patch: a commit sealing mid-turn, or a
/// base replay that altered the change. A replay that kept the patch keeps the
/// wave; it finishes and records against the head it ran on, and the `/checks`
/// projection names that head and its base for the lanes it answers.
///
/// For a commit, the branch just advanced, so that suite — launched at the
/// previous turn-end against the now-superseded tree — is validating a tree
/// nobody will look at again, while its bounded concurrent full Rust compiles
/// (each in its own COW clone) saturate CPU and I/O. That starves this commit's
/// own `when:write` checks (which fire right after the seal via
/// [`run_write_checks_after_seal`]) and the agent's next manual `bun run
/// test:rust`. Cancelling frees those resources for the checks that validate the
/// NEW code; the review cadence relaunches a fresh suite for the advanced tree
/// at the next turn-end via the normal `spawn_turn_end_checks` path.
///
/// This reuses the CAIRN-2648 [`Orchestrator::cancel_turn_end_checks`] lever, so
/// it is best-effort and idempotent: a no-op when no review suite is in flight
/// for the job, hence safe on every commit. Keyed by the committing run's own
/// `job_id` — the dominant path is the builder committing its own fix, which
/// cancels exactly its suite. A sub-agent/task that commits into the builder's
/// inherited worktree under a *different* job id would not hit the builder's
/// suite; that edge case is deliberately left as-is.
pub(crate) async fn cancel_stale_review_on_branch_advance(
    orch: &Orchestrator,
    job_id: &str,
    advance: BranchAdvance,
) {
    cancel_stale_review_on_branch_advance_except(orch, job_id, advance, None).await;
}

/// [`cancel_stale_review_on_branch_advance`], sparing the wave of `except`: a
/// review wave whose own capture prepared the branch. That wave has not planned
/// anything yet and judges the head the advance produced, so withdrawing it
/// would only cancel the review the advance was performed for. Every other wave
/// sharing the branch still follows the rule.
pub(crate) async fn cancel_stale_review_on_branch_advance_except(
    orch: &Orchestrator,
    job_id: &str,
    advance: BranchAdvance,
    except: Option<&str>,
) {
    if !advance.supersedes_review() {
        if orch.turn_end_checks_in_flight(job_id) {
            log::info!(
                "review wave for job {}: kept across a base advance — the branch's own patch is unchanged",
                &job_id[..job_id.len().min(8)]
            );
        }
        return;
    }
    for stale in jobs_sharing_branch(&orch.db.local, job_id).await {
        if except == Some(stale.as_str()) {
            continue;
        }
        // Name the tree the withdrawn wave was verifying. A supersession logged
        // as "cancelled" alone leaves a reader unable to tell a wave stopped for
        // the right reason — the tree it holds is no longer the node's — from
        // one lost to a fault.
        if orch.turn_end_checks_in_flight(&stale) {
            let verifying = orch
                .turn_end_check_runtime_status(&stale)
                .map(|status| {
                    let tree = &status.plan.tree_hash;
                    format!("tree {}", &tree[..tree.len().min(12)])
                })
                .unwrap_or_else(|| "a tree it had not published yet".to_string());
            log::info!(
                "review wave for job {}: superseded — the branch advanced past the {verifying} it was verifying",
                &stale[..stale.len().min(8)]
            );
        }
        orch.cancel_turn_end_checks(
            &stale,
            crate::orchestrator::TurnEndCancelReason::BranchAdvanced,
        );
    }
}

/// Every job whose in-flight review suite a commit on `job_id`'s branch
/// invalidates: `job_id` itself, plus any other job sharing that branch.
///
/// Suites are registered per job, but a wave's inputs are a BRANCH's sealed
/// tree. A sub-agent or task commits into the branch under its own job id, so
/// keying the cancellation to the committing job alone left the node's suite
/// running against a tree that no longer existed — the wave still burned a full
/// exclusive lane, and its verdicts were discarded on arrival because they were
/// keyed to the superseded tree. Jobs that share a branch share a tree, so a
/// commit supersedes all of their waves at once.
///
/// Scoped to the owner's project, because a branch name identifies a tree only
/// within one repository. Names like `main` recur across projects, and generated
/// names are only unique per project, so matching on the name alone would let a
/// commit in one project cancel live review suites in an unrelated one whose
/// tree it never touched.
///
/// Always yields `job_id`, so a job with no recorded branch (or an unreadable
/// row) still cancels its own suite exactly as before.
pub(crate) async fn jobs_sharing_branch(db: &LocalDb, job_id: &str) -> Vec<String> {
    let owner = job_id.to_string();
    let siblings = db
        .read(|conn| {
            let owner = owner.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT id FROM jobs
                         WHERE branch IS NOT NULL
                           AND branch <> ''
                           AND branch = (SELECT branch FROM jobs WHERE id = ?1)
                           AND project_id = (SELECT project_id FROM jobs WHERE id = ?1)",
                        (owner.as_str(),),
                    )
                    .await?;
                let mut ids = Vec::new();
                while let Some(row) = rows.next().await? {
                    ids.push(row.text(0)?);
                }
                Ok(ids)
            })
        })
        .await
        .unwrap_or_default();
    let mut jobs = vec![job_id.to_string()];
    jobs.extend(siblings.into_iter().filter(|id| id != job_id));
    jobs
}

/// Execute the planned checks against the sealed tree, consulting the cache
/// first. Each plan is paired with its per-check input hash (the cache key);
/// `tree_hash` is the evaluated tree, recorded on every fresh observation and on
/// the commit alias a reuse hit writes. Generic
/// over the spawn closure so the cache hit/miss behavior is unit-testable without
/// spawning a real process. Returns one [`CheckOutcome`] per check in plan order.
///
/// ## Ordering and isolation
///
/// Two phases. Phase 1 resolves cache HITS sequentially (a cheap re-stamp +
/// transition; a hit runs nothing). Phase 2 executes the MISSES, whose ordering
/// depends on `mode`:
///
/// - `Isolated`: each miss runs against its OWN copy-on-write clone of the sealed
///   worktree (resolved by the caller's `execute` closure). The futures are polled
///   concurrently, but every process spawn first crosses the runner-wide fair
///   admission controller. A formatter's writes land in its
///   private clone and are copied back only after every check finishes, so no check
///   ever observes another's half-written tree — every check validates exactly the
///   sealed tree.
/// - `Shared`: the fallback when a cheap clone is unavailable. All misses share
///   the one sealed checkout, so they MUST run SEQUENTIALLY, in plan order — a
///   mutating check's edits have to settle before the next check observes the
///   worktree, or a read-only check (e.g. `migrations` reading a Rust file) could
///   see a formatter's partial write.
///
/// One `run_miss` future serves both paths so the fallback is not a code fork.
/// Outcomes are reassembled into plan order regardless of completion order, so a
/// concurrent miss finishing first never reorders the summary. The
/// snapshot/`transition` machinery is a `std::sync::Mutex` with no guard held
/// across an await, and the per-check output streams are namespaced
/// `{toolUseId}:check-{index}`, so concurrent transitions and streams are safe.
///
/// ## Live status snapshots
///
/// `notify` receives a FULL checklist snapshot on every state transition (never a
/// delta), so a frontend consumer stays stateless — the latest snapshot wins. The
/// planned set (all `pending`) is emitted immediately; each entry then moves to
/// `running` when its command starts and to `passed`/`failed` (annotated exactly
/// as the final summary via [`summary_annotation`]) when it finishes. A cache hit
/// jumps straight from `pending` to its final state with no `running` phase.
///
/// A miss parses the runner's output into structured per-test results
/// ([`parse_check_output`]) and persists them in the cache row's
/// `target_results_json`; a hit rehydrates that column. Parsing is pure
/// enrichment — `passed` / `exit_code` stay exit-code-driven either way, so a
/// parser miss can never turn a failing exit into a pass.
// Each parameter is a distinct scalar/closure the runner genuinely needs (cache
// identity, plan set, spawn closure, live-status notifier); grouping them into a
// struct would only add indirection here.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_planned_checks_with_board<F, Fut, N, E>(
    db: Arc<LocalDb>,
    project_id: &str,
    commit: CheckRunCommit<'_>,
    tree_hash: &str,
    job_id: &str,
    plans: &[(CheckPlan, String)],
    sealed_coordinates: &[crate::execution::cache::SealedCheckCoordinate],
    infra_scopes: &[InfraScopes],
    tool_use_id: &str,
    mode: CheckExecMode,
    diagnostic_orch: Option<&Orchestrator>,
    status_board: Option<CheckStatusBoard>,
    effective_env: &[(String, String)],
    retry: bool,
    execute: F,
    notify: N,
) -> Vec<CheckOutcome>
where
    F: Fn(usize, String, String) -> Fut,
    Fut: std::future::Future<Output = Result<CheckExecResult, E>>,
    E: Into<CheckExecutionFailure>,
    N: Fn(Vec<CheckStatusEntry>) + Send + Sync + 'static,
{
    run_planned_checks_selected(
        db,
        project_id,
        commit,
        tree_hash,
        job_id,
        plans,
        sealed_coordinates,
        infra_scopes,
        tool_use_id,
        mode,
        diagnostic_orch,
        status_board,
        effective_env,
        retry,
        None,
        &record_fresh_check_observation,
        execute,
        notify,
    )
    .await
}

/// The canonical engine, optionally restricted to original plan indices whose
/// authoritative placement results have arrived. Unselected members are untouched.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_planned_checks_selected<F, Fut, N, E>(
    db: Arc<LocalDb>,
    project_id: &str,
    commit: CheckRunCommit<'_>,
    tree_hash: &str,
    job_id: &str,
    plans: &[(CheckPlan, String)],
    sealed_coordinates: &[crate::execution::cache::SealedCheckCoordinate],
    infra_scopes: &[InfraScopes],
    tool_use_id: &str,
    mode: CheckExecMode,
    diagnostic_orch: Option<&Orchestrator>,
    status_board: Option<CheckStatusBoard>,
    effective_env: &[(String, String)],
    retry: bool,
    selected_indices: Option<&[usize]>,
    record_observation: &(dyn Fn(Arc<LocalDb>, FreshCheckObservationWrite) -> Result<(), String>
          + Send
          + Sync),
    execute: F,
    notify: N,
) -> Vec<CheckOutcome>
where
    F: Fn(usize, String, String) -> Fut,
    Fut: std::future::Future<Output = Result<CheckExecResult, E>>,
    E: Into<CheckExecutionFailure>,
    N: Fn(Vec<CheckStatusEntry>) + Send + Sync + 'static,
{
    assert_eq!(
        plans.len(),
        sealed_coordinates.len(),
        "every planned check needs a sealed coordinate witness"
    );
    assert_eq!(
        plans.len(),
        infra_scopes.len(),
        "every planned check needs the same suppression scopes its submission was reserved under"
    );
    for ((_, input_hash), coordinate) in plans.iter().zip(sealed_coordinates) {
        assert_eq!(
            input_hash,
            coordinate.input_hash(),
            "planned input must match its sealed witness"
        );
        assert_eq!(
            commit.evaluated,
            coordinate.evaluated_commit(),
            "evaluated commit must match its sealed witness"
        );
        assert_eq!(
            commit.defined_by,
            coordinate.defined_by_commit(),
            "defining commit must match its sealed witness"
        );
        assert_eq!(
            tree_hash,
            coordinate.tree_hash(),
            "tree hash must match its sealed witness"
        );
    }
    // Checklist snapshot, seeded all-`pending` from the plan list. Each check
    // transitions ITS OWN entry and re-emits the whole snapshot, so the live line
    // is self-healing (latest snapshot wins). A std Mutex keeps the transition
    // helper a plain `Fn`; it is only ever locked to mutate + clone and released
    // before the (synchronous) emit, so no guard is held across an await.
    let board = status_board
        .unwrap_or_else(|| CheckStatusBoard::new(plans, Arc::new(move |checks, _| notify(checks))));
    board.emit_initial();

    // Phase 1: resolve cache HITS sequentially, and collect the MISS indices to
    // execute. Reuse is admitted by the project's declared platform trust set,
    // so it is valid before executor placement and a hit places nothing.
    // `outcomes` is index-addressed so misses can complete out of order
    // (concurrent `Isolated` mode) and still reassemble into plan order.
    let mut outcomes: Vec<Option<CheckOutcome>> = (0..plans.len()).map(|_| None).collect();
    let mut misses: Vec<usize> = Vec::new();
    for (index, (plan, input_hash)) in plans.iter().enumerate() {
        if selected_indices.is_some_and(|selected| !selected.contains(&index)) {
            continue;
        }
        if retry {
            misses.push(index);
            continue;
        }
        // Cache hit ⇒ reuse the stored verdict and rehydrate the structured
        // detail; run nothing. The lookup is keyed by the per-check INPUT hash, so
        // a commit that changed none of this check's impact-matched files hits
        // even though the whole-tree hash moved.
        let schema_version = crate::execution::check_identity::CHECK_RESULT_SCHEMA_VERSION as i64;
        // Legacy, incomplete, unstable, or environment-mismatched evidence is diagnostic only.
        let Some(reused) =
            reusable_verdict_for_plan(db.clone(), project_id, plan, input_hash, effective_env)
        else {
            misses.push(index);
            continue;
        };
        let entry = reused.entry;
        let source_observation = reused.source_observation_id;
        let source_environment_fingerprint = reused.environment_fingerprint;
        let public_handle = reused.public_handle;
        let recorded = crate::execution::cache::RecordedCheckObservation {
            id: source_observation.clone(),
            public_handle,
            ran_at: entry.ran_at,
            environment_fingerprint: source_environment_fingerprint.clone(),
            executor_id: entry.executor_id.clone(),
            // Only a reusable observation is ever admitted as a hit.
            reusable: true,
        };
        let cached_recorded = record_cached_check_observation(
            db.clone(),
            CachedCheckObservationWrite {
                manual_handle: manual_check_handle(tool_use_id).map(str::to_string),
                project_id: project_id.to_string(),
                commit_sha: commit.evaluated.to_string(),
                // The reused verdict came from another commit's observation, but
                // THIS alias is a statement about the commit being evaluated: it
                // is that commit's definition the reuse was admitted under. The
                // source observation keeps its own defining commit.
                defined_by_commit_sha: commit.defined_by.to_string(),
                tree_hash: tree_hash.to_string(),
                check_name: plan.name.clone(),
                input_hash: input_hash.to_string(),
                environment_fingerprint: source_environment_fingerprint.clone(),
                result_schema_version: schema_version,
                source_observation_id: source_observation,
                evaluated_at: chrono::Utc::now().timestamp_millis(),
            },
        )
        .is_ok();
        if cached_recorded {
            if let Some(orch) = diagnostic_orch {
                orch.invalidate_project_check_results(project_id);
            }
        }
        if let Some(orch) = diagnostic_orch {
            orch.fleet.record_cached_completion(
                project_id,
                job_id,
                entry.executor_id.as_deref(),
                &plan.command,
                if tool_use_id.starts_with("turn-checks:") {
                    CellPriority::ReviewCheck
                } else {
                    CellPriority::WriteCheck
                },
                entry.passed,
            );
            let _ = orch
                .services
                .emitter
                .emit("substrate-health-change", serde_json::json!({}));
        }
        // Rehydrate the structured per-test detail persisted at run time.
        let parsed = entry
            .target_results_json
            .as_deref()
            .and_then(|s| serde_json::from_str::<ParsedCheckResult>(s).ok());
        let outcome = CheckOutcome {
            left_running: None,
            consumer_guard: None,
            name: plan.name.clone(),
            passed: entry.passed,
            exit_code: Some(entry.exit_code),
            failure_kind: entry
                .failure_kind
                .as_deref()
                .and_then(CheckFailureKind::from_stored),
            parsed,
            output_tail: entry.output_tail,
            cached: true,
            duration_ms: entry.duration_ms,
            suppressed_after: None,
            recorded: Some(recorded),
            not_recorded: None,
        };
        // A cache hit jumps straight from pending to its final state.
        board.transition_cached(
            index,
            if outcome.passed { "passed" } else { "failed" },
            summary_annotation(&outcome),
        );
        outcomes[index] = Some(outcome);
    }

    // One miss: transition running → run (streaming) → record → transition final,
    // yielding `(index, outcome)` so the caller can reassemble into plan order.
    // Borrows shared state by reference so the returned future is not tied to a
    // moved closure capture (mirrors how `orch`/`run_context` flow through
    // `execute`), letting concurrent mode hold many of these futures at once.
    let run_miss = |index: usize| {
        let db = &db;
        let execute = &execute;
        let board = &board;
        async move {
            let (plan, input_hash) = &plans[index];
            // Every suppression counter this miss is live against — the same
            // set its submission was reserved under. One value feeds the row a
            // failure writes, the escalation it may claim, and the scopes a
            // verdict clears, so those three can never name different scopes.
            let infra_scopes = &infra_scopes[index];
            // Admission happens once, in the executor's priority-aware queue, and
            // this submission declares what it will fan out to so that queue can
            // account for it. There is deliberately no second gate here: a
            // runner-local permit taken BEFORE entering the executor's queue is
            // what let a review request hold host capacity while a later
            // write-cadence request waited behind it (CAIRN-3108).
            let mut uncached_retry = false;
            let mut pending_publication = None;
            loop {
                board.transition(index, "running", None);
                let stream_id = crate::mcp::handlers::run::check_stream_id(tool_use_id, index);
                let command = if uncached_retry {
                    uncached_retry_command(&plan.command)
                } else {
                    plan.command.clone()
                };
                let started = Instant::now();
                let exec_outcome = execute(index, command, stream_id)
                    .await
                    .map_err(Into::<CheckExecutionFailure>::into);

                // A refused reservation is not an execution. Submission declined to
                // launch this command because a repair domain's retry budget is spent,
                // so there is no output to parse, no verdict to classify, and nothing
                // to store — doing any of those would invent a result for a command
                // that never ran. The refusal already carries the rows the decision was
                // taken on, so the suppression renders from those rather than from a
                // second lookup that has to invent an outcome when it misses.
                let exec_outcome = match exec_outcome {
                    Err(CheckExecutionFailure::Suppressed(suppressed)) => {
                        let outcome = record_suppressed_check(
                            db,
                            tree_hash,
                            job_id,
                            commit.evaluated,
                            &plan.name,
                            &suppressed,
                        );
                        board.transition(index, "failed", summary_annotation(&outcome));
                        return (index, outcome);
                    }
                    // Never offered to placement: the same absence of a run, with
                    // the fleet's own words for why.
                    Err(CheckExecutionFailure::NotPlaceable(reason)) => {
                        let outcome = record_unplaceable_check(
                            db, project_id, commit, tree_hash, job_id, &plan.name, input_hash,
                            &reason,
                        );
                        // Terminal, but no verdict: the live board must not draw
                        // it as a red.
                        board.transition(index, "unverified", summary_annotation(&outcome));
                        return (index, outcome);
                    }
                    // Never offered to placement either: the project's config
                    // does not load in this build.
                    Err(CheckExecutionFailure::ConfigUnreadable(refusal)) => {
                        let outcome = record_unreadable_config_check(
                            db, project_id, commit, tree_hash, job_id, &plan.name, input_hash,
                            &refusal,
                        );
                        board.transition(index, "unverified", summary_annotation(&outcome));
                        return (index, outcome);
                    }
                    // Cairn ended the check itself. Nothing was evaluated, and
                    // nothing failed, so nothing is recorded or counted.
                    Err(CheckExecutionFailure::LeftRunning(attempt)) => {
                        let outcome = record_left_running_check(&plan.name, attempt);
                        board.transition(index, "unverified", Some(outcome.output_tail.clone()));
                        return (index, outcome);
                    }
                    Err(CheckExecutionFailure::Cancelled(cause)) => {
                        let outcome = record_cancelled_check(job_id, &plan.name, cause);
                        board.transition(index, "unverified", summary_annotation(&outcome));
                        return (index, outcome);
                    }
                    other => other,
                };

                let transient_within_grace = false;

                let (
                    exit_code,
                    output,
                    timed_out,
                    spawn_error,
                    authoritative_duration_ms,
                    substrate_failure,
                    provenance,
                    publication,
                ) = match exec_outcome {
                    Ok(CheckExecResult {
                        exit_code,
                        output,
                        timed_out,
                        duration_ms,
                        provenance,
                        publication,
                    }) => (
                        exit_code,
                        output,
                        timed_out,
                        false,
                        duration_ms,
                        None,
                        provenance,
                        publication,
                    ),
                    Err(CheckExecutionFailure::Process(err)) => {
                        (None, err, false, true, None, None, None, None)
                    }
                    // The two halves part here: the agent-facing message becomes
                    // the check's output, while the composed failure travels
                    // beside it — its diagnostic to the operator log record
                    // below, its applicability to the advisory gate.
                    // A stopped command still ran somewhere, so its provenance
                    // survives: it picks the platform the stall is charged to.
                    Err(CheckExecutionFailure::Substrate(failure)) => {
                        let provenance = failure.provenance().cloned();
                        (
                            None,
                            failure.agent_message(),
                            false,
                            false,
                            failure.elapsed_ms(),
                            Some(failure),
                            provenance,
                            None,
                        )
                    }
                    Err(
                        CheckExecutionFailure::Suppressed(_)
                        | CheckExecutionFailure::NotPlaceable(_)
                        | CheckExecutionFailure::ConfigUnreadable(_)
                        | CheckExecutionFailure::Cancelled(_)
                        | CheckExecutionFailure::LeftRunning(_),
                    ) => {
                        unreachable!("a refused submission returns above")
                    }
                };
                let duration_ms = authoritative_duration_ms
                    .unwrap_or_else(|| started.elapsed().as_millis() as i64);
                let passed = exit_code == Some(0);

                // Parse before classifying: positive assertion failures outrank any
                // incidental infrastructure warning in the combined output.
                let parsed = parse_check_output(&plan.command, &output);
                let mut classification = if let Some(substrate) = substrate_failure.as_ref() {
                    Some(FailureClassification {
                        kind: substrate_failure_kind(substrate),
                        reason: "Cairn: this check produced no verdict because Cairn's own \
                             infrastructure failed"
                            .to_string(),
                        evidence_line: None,
                        cache_corruption: false,
                        implied_requirement: None,
                    })
                } else {
                    classify_check_failure(
                        &plan.command,
                        exit_code,
                        timed_out,
                        spawn_error,
                        parsed.as_ref(),
                        &output,
                    )
                };
                attach_executor_provenance(
                    &mut classification,
                    provenance.as_ref().map(|meta| meta.executor_id.as_str()),
                );
                let cache_corruption = classification
                    .as_ref()
                    .is_some_and(|classification| classification.cache_corruption);
                let failure_kind = classification.as_ref().map(|c| c.kind);
                // Which counter an infrastructure failure here is charged to,
                // and which row it writes. Classified from the failure itself
                // rather than from the stored kind, which cannot tell a
                // structural fleet gap from a cell race.
                //
                // The environment scopes were resolved when this wave was
                // submitted. If the machine that produced this result has
                // reattached since, it has been re-measured, and charging the
                // identity we snapshotted would bound a machine state that no
                // longer exists. Naming no executor narrows the failure to the
                // head domain, which is the honest answer.
                let environment_still_current = provenance.as_ref().is_some_and(|meta| {
                    diagnostic_orch.is_none_or(|orch| {
                        orch.fleet
                            .live_generation(&meta.executor_id)
                            .is_none_or(|live| live == meta.executor_connection_generation)
                    })
                });
                let infra_scope = infra_scopes.scope_for(
                    infra_repair_domain(
                        substrate_failure.as_ref().map(SubstrateFailure::shape),
                        failure_kind,
                    ),
                    provenance
                        .as_ref()
                        .filter(|_| environment_still_current)
                        .map(|meta| meta.executor_id.as_str()),
                    provenance
                        .as_ref()
                        .and_then(|meta| meta.verdict_platform.as_deref()),
                );
                let target_results_json =
                    parsed.as_ref().and_then(|p| serde_json::to_string(p).ok());
                // A substrate failure's `output` is already the message composed for
                // the agent; framing authored text as "evidence" beneath a reason
                // header would only wrap it in a second voice.
                let mut output_tail = if substrate_failure.is_some() {
                    tail(&output, OUTPUT_TAIL_CHARS)
                } else {
                    classified_output_excerpt(&output, classification.as_ref(), parsed.as_ref())
                };

                if failure_kind.is_some_and(CheckFailureKind::is_infrastructure) {
                    let resources = crate::pressure::platform::read_process_resources();
                    let host = crate::pressure::platform::read_host_resources();
                    let process_tree = crate::pressure::process_tree::sample_ps_rows()
                        .map(|rows| {
                            let mut rows = crate::pressure::process_tree::select_process_tree(
                                &rows,
                                std::process::id(),
                            );
                            rows.sort_by_key(|row| std::cmp::Reverse(row.rss_kb));
                            rows.into_iter()
                                .take(16)
                                .map(|row| {
                                    serde_json::json!({
                                        "pid": row.pid,
                                        "parentPid": row.ppid,
                                        "cpuPercent": row.cpu_percent,
                                        "rssKb": row.rss_kb,
                                        "command": row.command,
                                    })
                                })
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default();
                    let build_service = diagnostic_orch
                        .map(|orch| orch.build_service_diagnostic_snapshot("sccache"));
                    output_tail = with_build_service_advisory(
                        output_tail,
                        build_service.as_ref(),
                        substrate_failure.as_ref(),
                    );
                    log::warn!(
                        "{}",
                        infrastructure_failure_log_line(
                            &plan.name,
                            substrate_failure.as_ref().map(SubstrateFailure::diagnostic),
                            serde_json::json!({
                                "jobId": job_id,
                                "suiteId": tool_use_id,
                                "cadence": if tool_use_id.starts_with("turn-checks:") { "review" } else { "write" },
                                "resourceClass": plan.resource_class.as_str(),
                                "declaredConcurrencyUnits": declared_check_reservation(plan.resource_class).concurrency_units,
                                "durationMs": duration_ms,
                                "exitCode": exit_code,
                                "timedOut": timed_out,
                                "runnerRssBytes": resources.rss_bytes,
                                "runnerPhysicalFootprintBytes": resources.phys_footprint_bytes,
                                "hostTotalMemoryBytes": host.total_memory_bytes,
                                "hostAvailableMemoryBytes": host.available_memory_bytes,
                                "hostLoadAverage": host.load_average,
                                "processTree": process_tree,
                                "buildService": build_service,
                            })
                        )
                    );
                }

                let consumer_guard = publication
                    .as_ref()
                    .and_then(|publication| publication.consumer_guard());
                let request_identity = publication
                    .as_ref()
                    .and_then(|publication| publication.request_identity());
                // A corruption result is provisional. The publisher retains the
                // original execution's guard across the one uncached retry, keeping
                // every coalesced sibling blocked until the retry is recorded.
                let (publication_role, publication_id) =
                    if let Some((guard, id)) = pending_publication.take() {
                        (Some(crate::fleet::PublicationRole::Publisher(guard)), id)
                    } else {
                        match publication {
                            Some(publication) => {
                                let id = Some(publication.id().to_string());
                                (Some(publication.acquire().await), id)
                            }
                            None => (None, None),
                        }
                    };
                if cache_corruption && !uncached_retry {
                    if let (Some(orch), Some(request)) =
                        (diagnostic_orch, request_identity.as_ref())
                    {
                        lose_check_claim(orch, request, "the result requires an uncached retry; the original attempt produced no accepted observation").await;
                    }
                    match publication_role {
                        Some(crate::fleet::PublicationRole::Publisher(guard)) => {
                            pending_publication = Some((guard, publication_id));
                        }
                        Some(crate::fleet::PublicationRole::Published(recorded)) => {
                            let attachment_error =
                                match (manual_check_handle(tool_use_id), recorded.as_ref()) {
                                    (Some(handle), Some(source)) => {
                                        crate::execution::cache::attach_manual_check_observation(
                                            db,
                                            project_id,
                                            handle,
                                            &source.id,
                                            commit.evaluated,
                                            tree_hash,
                                            input_hash,
                                            "fresh",
                                        )
                                        .await
                                        .err()
                                    }
                                    _ => None,
                                };
                            let entry = get_check_result(
                                db.clone(),
                                project_id,
                                &plan.name,
                                input_hash,
                            )
                            .ok()
                            .flatten()
                            .expect("published check observation must have a canonical cache row");
                            let parsed = entry
                                .target_results_json
                                .as_deref()
                                .and_then(|json| serde_json::from_str(json).ok());
                            let outcome = CheckOutcome {
                                left_running: None,
                                consumer_guard: None,
                                name: plan.name.clone(),
                                passed: entry.passed,
                                exit_code: Some(entry.exit_code),
                                failure_kind: entry
                                    .failure_kind
                                    .as_deref()
                                    .and_then(CheckFailureKind::from_stored),
                                parsed,
                                output_tail: entry.output_tail,
                                cached: false,
                                duration_ms: entry.duration_ms,
                                suppressed_after: None,
                                recorded,
                                not_recorded: attachment_error,
                            };
                            board.transition(
                                index,
                                if outcome.passed { "passed" } else { "failed" },
                                summary_annotation(&outcome),
                            );
                            return (index, outcome);
                        }
                        None => {}
                    }
                    if let Some(orch) = diagnostic_orch {
                        orch.report_compile_cache_corruption(&output_tail);
                    }
                    uncached_retry = true;
                    continue;
                }

                // Either a sibling already recorded this verdict and named the row it
                // wrote, or this evaluation records it and names that row for every
                // sibling. Both arms end holding the same answer to "what is this
                // verdict's durable identity", which is what the caller reports.
                let (recorded, published_here, mut not_recorded) = match publication_role {
                    Some(crate::fleet::PublicationRole::Published(recorded)) => {
                        (recorded, false, None)
                    }
                    role => {
                        // A failure with no executor provenance may still have
                        // had a machine: the one that claimed it. Its rows name
                        // that machine, so a lost link is attributable.
                        let observed_on = ObservedOn::resolve(
                            provenance.as_ref(),
                            substrate_failure
                                .as_ref()
                                .and_then(SubstrateFailure::placement),
                            substrate_failure.is_some(),
                        );
                        let (verdict_platform, verdict_arch) = observed_on.verdict_platform();
                        let legacy_write = CheckResultCacheWrite {
                            project_id: project_id.to_string(),
                            defined_by_commit_sha: Some(commit.defined_by.to_string()),
                            tree_hash: tree_hash.to_string(),
                            input_hash: input_hash.clone(),
                            check_name: plan.name.clone(),
                            exit_code: exit_code.unwrap_or(-1),
                            passed,
                            output_tail: output_tail.clone(),
                            duration_ms,
                            target_results_json,
                            job_id: Some(job_id.to_string()),
                            cached: Some(false),
                            failure_kind: failure_kind.map(|kind| kind.as_str().to_string()),
                            executor_id: observed_on.executor_id(),
                            executor_device_id: provenance
                                .as_ref()
                                .map(|meta| meta.executor_device_id.clone()),
                            executor_connection_generation: observed_on.connection_generation(),
                            executor_cell_id: provenance.as_ref().map(|meta| meta.cell_id.clone()),
                            executor_lease_epoch: provenance
                                .as_ref()
                                .map(|meta| meta.cell_epoch as i64),
                            executor_started_at_unix_ms: provenance
                                .as_ref()
                                .map(|meta| meta.started_at_unix_ms as i64),
                            executor_finished_at_unix_ms: provenance
                                .as_ref()
                                .map(|meta| meta.finished_at_unix_ms as i64),
                            toolchain_fingerprint: observed_on.toolchain_fingerprint(),
                            environment_fingerprint: infra_scope.clone(),
                            verdict_platform,
                            verdict_arch,
                            verdict_environment_hash: provenance
                                .as_ref()
                                .and_then(|meta| meta.verdict_environment_hash.clone()),
                            transient_within_grace,
                        };
                        let reuse =
                            check_reuse_decision(&plan.command, failure_kind, parsed.as_ref());
                        let mut observation = fresh_observation_write(
                            project_id,
                            &sealed_coordinates[index],
                            plan,
                            job_id,
                            tool_use_id,
                            exit_code.unwrap_or(-1),
                            failure_kind,
                            duration_ms,
                            &output_tail,
                            observed_on,
                            parsed.as_ref(),
                            reuse,
                            infra_scopes.verdict_scopes(
                                provenance.as_ref().map(|meta| meta.executor_id.as_str()),
                                provenance
                                    .as_ref()
                                    .and_then(|meta| meta.verdict_platform.as_deref()),
                            ),
                        );
                        observation.publication_id = publication_id;
                        if let Some((request, attempt)) = &request_identity {
                            observation.id =
                                format!("remote-check:{request}:{attempt}:{}", plan.name);
                            let physical_plan = diagnostic_orch
                                .and_then(|orch| {
                                    orch.fleet.check_claim(&(request.clone(), attempt.clone()))
                                })
                                .and_then(|claim| {
                                    let index = claim
                                        .check_names
                                        .iter()
                                        .position(|name| name == &plan.name)?;
                                    let recording: CheckAdoptionRecordingPlan =
                                        serde_json::from_value(
                                            claim.recording_plans.get(index)?.clone(),
                                        )
                                        .ok()?;
                                    (recording.input_hash == *input_hash
                                        && recording.evaluated_commit == claim.commit_sha)
                                        .then_some(recording)
                                });
                            if let Some(physical) = physical_plan {
                                // Coalesced subscribers may ask from different commits.
                                // The immutable source describes the physical execution,
                                // never whichever subscriber wins publication.
                                observation.sealed_coordinate =
                                    crate::execution::cache::SealedCheckCoordinate::new(
                                        &physical.evaluated_commit,
                                        &physical.defined_by_commit,
                                        &physical.tree_hash,
                                        &physical.input_hash,
                                    );
                                observation.commit_sha = physical.evaluated_commit;
                                observation.defined_by_commit_sha = physical.defined_by_commit;
                                observation.tree_hash = physical.tree_hash;
                                observation.job_id = Some(physical.job_id);
                                observation.manual_handle =
                                    manual_check_handle(&physical.tool_use_id).map(str::to_string);
                                observation.run_id = physical
                                    .tool_use_id
                                    .strip_prefix("manual-check:")
                                    .and_then(|value| {
                                        value.split_once(':').map(|(run, _)| run.to_string())
                                    });
                                observation.cadence =
                                    observation_cadence(&physical.tool_use_id).to_string();
                                if physical.plan.scope == CheckScope::Partial {
                                    observation.selection = Some("partial".to_string());
                                }
                            }
                        }
                        if let Some(kind) = failure_kind {
                            let (cause, evidence) = failure_cause_and_evidence(
                                substrate_failure.as_ref(),
                                classification.as_ref(),
                                &output,
                            );
                            observation.failure_signature =
                                Some(crate::execution::lane_breaker::failure_signature(
                                    kind.as_str(),
                                    &cause,
                                    exit_code.unwrap_or(-1),
                                    &evidence,
                                ));
                            observation.placement_facts = Some(infra_scopes.placement_facts());
                            observation.substrate_shape = substrate_failure
                                .as_ref()
                                .map(|failure| failure.shape().stored().to_string());
                        }
                        let retry_recording = request_identity.as_ref().is_some_and(|request| {
                            diagnostic_orch
                                .is_some_and(|orch| orch.fleet.check_claim(request).is_some())
                        });
                        let recording = loop {
                            let existing = match &request_identity {
                            Some(_) => db.query_opt(
                                "SELECT id, public_handle, ran_at, environment_fingerprint, executor_id, reusable FROM check_result_observations WHERE id=?1 AND project_id=?2 AND commit_sha=?3 AND check_name=?4 AND input_hash=?5",
                                cairn_db::turso::params![observation.id.clone(), project_id, observation.commit_sha.as_str(), plan.name.as_str(), input_hash.as_str()],
                                |row| Ok(crate::execution::cache::RecordedCheckObservation {
                                    id: row.text(0)?, public_handle: row.text(1)?, ran_at: row.i64(2)?,
                                    environment_fingerprint: row.text(3)?, executor_id: row.opt_text(4)?,
                                    reusable: row.i64(5)? != 0,
                                }),
                            ).await.map_err(|error| error.to_string()),
                            None => Ok(None),
                        };
                            let identity = crate::execution::cache::RecordedCheckObservation {
                                id: observation.id.clone(),
                                public_handle: observation.public_handle.clone(),
                                ran_at: observation.ran_at,
                                environment_fingerprint: observation
                                    .environment_fingerprint
                                    .clone(),
                                executor_id: observation.executor_id.clone(),
                                reusable: observation.reusable,
                            };
                            let attempt = match existing {
                                Ok(Some(existing)) => Ok(existing),
                                Ok(None) => record_observation(db.clone(), observation.clone())
                                    .map(|()| identity),
                                Err(error) => Err(error),
                            };
                            if let Err(error) = &attempt {
                                if retry_recording {
                                    log::error!("remote check observation {} could not commit: {error}; retaining its result and retrying the same immutable write", observation.id);
                                    #[cfg(test)]
                                    adoption_tests::notify_observation_retry(
                                        &observation.id,
                                        error,
                                    );
                                    tokio::time::sleep(crate::fleet::RETAINED_RESULT_RETRY).await;
                                    continue;
                                }
                            }
                            break attempt;
                        };
                        let (recorded, not_recorded) = match recording {
                            Ok(identity) => {
                                if let Some(orch) = diagnostic_orch {
                                    orch.invalidate_project_check_results(project_id);
                                }
                                let reason = provenance.as_ref().is_some_and(|meta| {
                                meta.executor_id != crate::fleet::COLOCATED_EXECUTOR_ID
                                    && meta.environment_fingerprint.is_empty()
                            }).then(|| "the selected legacy executor did not report a complete verdict environment identity".to_string());
                                (Some(identity), reason)
                            }
                            Err(error) => {
                                log::warn!("failed to record immutable check observation: {error}");
                                (
                                    None,
                                    Some(format!(
                                        "{}{error}",
                                        crate::execution::checks::OBSERVATION_WRITE_FAILED
                                    )),
                                )
                            }
                        };
                        // The legacy row is retained only for infrastructure retry/suppression diagnosis.
                        if failure_kind.is_some_and(CheckFailureKind::is_infrastructure) {
                            let _ = store_check_result(db.clone(), legacy_write);
                        }
                        if let Some(crate::fleet::PublicationRole::Publisher(guard)) = role {
                            guard.published(recorded.clone());
                        }
                        (recorded, true, not_recorded)
                    }
                };

                if let Some(source) = recorded.as_ref() {
                    let retry_projection = request_identity.as_ref().is_some_and(|request| {
                        diagnostic_orch
                            .is_some_and(|orch| orch.fleet.check_claim(request).is_some())
                    });
                    loop {
                        let projected = async {
                            // Cross-commit sharing creates an alias only when the
                            // normal reuse contract admits this exact source.
                            if request_identity.is_some() && reusable_verdict_for_plan(
                                db.clone(), project_id, plan, input_hash, effective_env,
                            ).is_some_and(|reused| reused.source_observation_id == source.id) {
                                record_cached_check_observation(db.clone(), CachedCheckObservationWrite {
                                    manual_handle: None,
                                    project_id: project_id.to_string(),
                                    commit_sha: commit.evaluated.to_string(),
                                    defined_by_commit_sha: commit.defined_by.to_string(),
                                    tree_hash: tree_hash.to_string(),
                                    check_name: plan.name.clone(),
                                    input_hash: input_hash.clone(),
                                    environment_fingerprint: source.environment_fingerprint.clone(),
                                    result_schema_version: crate::execution::check_identity::CHECK_RESULT_SCHEMA_VERSION as i64,
                                    source_observation_id: source.id.clone(),
                                    evaluated_at: chrono::Utc::now().timestamp_millis(),
                                })?;
                            }
                            if let Some(handle) = manual_check_handle(tool_use_id) {
                                crate::execution::cache::attach_manual_check_observation(
                                    db, project_id, handle, &source.id,
                                    commit.evaluated, tree_hash, input_hash, "fresh",
                                ).await?;
                            }
                            Ok::<_, String>(())
                        }.await;
                        match projected {
                            Ok(()) => break,
                            Err(error) if retry_projection => {
                                log::error!("check observation {} is durable, but consumer projection failed: {error}; retaining the result and retrying", source.id);
                                tokio::time::sleep(crate::fleet::RETAINED_RESULT_RETRY).await;
                            }
                            Err(error) => {
                                not_recorded = Some(error);
                                break;
                            }
                        }
                    }
                }
                if let (Some(orch), Some(request), Some(recorded)) = (
                    diagnostic_orch,
                    request_identity.as_ref(),
                    recorded.as_ref(),
                ) {
                    loop {
                        let settlement = async {
                            orch.fleet
                                .record_check_claim_observation(request, &plan.name, &recorded.id)
                                .await?;
                            // Main cadence delivers only after reporting its lane.
                            // Reloaded batches have their own consumer delivery driver.
                            if !orch.fleet.is_check_adoption_reloaded(request)
                                && !tool_use_id.starts_with(MAIN_CADENCE_STREAM_PREFIX)
                                && orch.fleet.check_claim(request).is_some()
                            {
                                if let Some(guard) = consumer_guard.as_ref() {
                                    guard.deliver(&recorded.id).await?;
                                } else {
                                    let (consumer, consumer_ref) =
                                        check_consumer_identity(orch, job_id, tool_use_id, None)
                                            .await?;
                                    orch.fleet
                                        .deliver_check_consumer(
                                            request,
                                            &plan.name,
                                            consumer,
                                            &consumer_ref,
                                            &recorded.id,
                                        )
                                        .await?;
                                }
                            }
                            Ok::<(), String>(())
                        }
                        .await;
                        match settlement {
                            Ok(()) => break,
                            Err(error) => {
                                log::error!("check observation {} committed, but claim settlement failed: {error}; retaining the result and retrying without acknowledgement", recorded.id);
                                #[cfg(test)]
                                adoption_tests::notify_observation_retry(&recorded.id, &error);
                                tokio::time::sleep(crate::fleet::RETAINED_RESULT_RETRY).await;
                            }
                        }
                    }
                }
                // A failure that shows the project needs a capability of its
                // machines teaches placement to require it, so the lane re-arms
                // somewhere that has it rather than back on this machine.
                if let (true, Some(orch), Some(implied), Some(meta), Some(recorded)) = (
                    published_here,
                    diagnostic_orch,
                    classification
                        .as_ref()
                        .and_then(|classification| classification.implied_requirement.clone()),
                    provenance.as_ref(),
                    recorded.as_ref(),
                ) {
                    orch.fleet
                        .learn_project_requirement(
                            project_id,
                            implied.capability,
                            implied.repository,
                            &meta.executor_id,
                            &recorded.id,
                        )
                        .await;
                }

                // The bound may have just been reached by the store above. Claim the
                // ONE escalation this triple is allowed and, if we won it, put the
                // operator's half of the story where the rest of it already lives.
                if published_here && failure_kind.is_some_and(CheckFailureKind::is_infrastructure) {
                    match claim_infra_escalation(
                        db.clone(),
                        project_id,
                        &plan.name,
                        input_hash,
                        &infra_scope,
                    ) {
                        Ok(true) => log::error!(
                            "check infrastructure failure: {}",
                            serde_json::json!({
                                "escalation": "suppressed",
                                "check": plan.name,
                                "projectId": project_id,
                                "inputHash": input_hash,
                                "jobId": job_id,
                                "suiteId": tool_use_id,
                                "bound": crate::execution::cache::OBSERVED_INFRA_FAILURE_BOUND,
                                "lastDiagnostic": output_tail,
                                "consequence": "Cairn has stopped executing this check for these inputs. It \
                                                runs again when its inputs change, or for these same inputs \
                                                once the repair this failure names moves its identity: \
                                                a machine re-measured or declared repaired, or the \
                                                fleet that can run it changing.",
                            })
                        ),
                        Ok(false) => {}
                        Err(error) => log::warn!(
                            "failed to claim the infrastructure escalation for check {:?}: {error}",
                            plan.name
                        ),
                    }
                }

                let outcome = CheckOutcome {
                    left_running: None,
                    consumer_guard,
                    name: plan.name.clone(),
                    passed,
                    exit_code,
                    failure_kind,
                    parsed,
                    output_tail,
                    cached: false,
                    duration_ms,
                    suppressed_after: None,
                    recorded,
                    not_recorded,
                };
                board.transition(
                    index,
                    if passed { "passed" } else { "failed" },
                    summary_annotation(&outcome),
                );
                return (index, outcome);
            }
        }
    };

    // Phase 2: execute misses concurrently when the caller owns independent
    // execution substrates, or sequentially when they share one mutable checkout.
    match mode {
        CheckExecMode::Isolated => {
            // Poll every independently placed miss. Admission is owned either by the
            // external substrate or by the shared host controller.
            let done: Vec<(usize, CheckOutcome)> =
                futures_util::future::join_all(misses.iter().map(|&index| run_miss(index))).await;
            for (index, outcome) in done {
                outcomes[index] = Some(outcome);
            }
        }
        CheckExecMode::Shared => {
            for &index in &misses {
                let (index, outcome) = run_miss(index).await;
                outcomes[index] = Some(outcome);
            }
        }
    }

    outcomes
        .into_iter()
        .enumerate()
        .filter(|(index, _)| selected_indices.is_none_or(|selected| selected.contains(index)))
        .map(|(_, o)| o.expect("every selected plan resolved to a hit or a miss outcome"))
        .collect()
}

/// Whether a plan lacks a reusable verdict and therefore needs placement.
///
/// This is an advisory capacity filter. The execution engine repeats the same
/// lookup while constructing outcomes, so a concurrent observation that lands
/// after this read can still turn the candidate into a hit without executing.
pub(super) fn effective_verdict_environment_hash(
    plan: &CheckPlan,
    effective_env: &[(String, String)],
) -> String {
    let declared =
        cairn_checkenv::verdict_environment_hash(plan.verdict_environment_names.clone(), |name| {
            effective_env
                .iter()
                .rev()
                .find(|(key, _)| key == name)
                .map(|(_, value)| value.clone())
                .or_else(|| std::env::var(name).ok())
        });
    cairn_checkenv::verdict_environment_with_runtime(&declared, &plan.verdict_runtime)
}

pub(super) fn needs_execution(
    db: Arc<LocalDb>,
    project_id: &str,
    plan: &CheckPlan,
    input_hash: &str,
    effective_env: &[(String, String)],
) -> bool {
    get_reusable_check_result(
        db,
        ReusableCheckLookup {
            project_id,
            check_name: &plan.name,
            input_hash,
            verdict_platforms: &plan.verdict_platforms,
            executor_ids: plan.verdict_executor_ids.as_deref(),
            implementation_identity: cairn_checkenv::implementation_identity(),
            verdict_environment_hash: &effective_verdict_environment_hash(plan, effective_env),
            result_schema_version: crate::execution::check_identity::CHECK_RESULT_SCHEMA_VERSION
                as i64,
        },
    )
    .ok()
    .flatten()
    .is_none()
}

/// Turn a suppression's stored evidence into this evaluation's outcome, and
/// re-stamp it onto the current tree so the tree-keyed `/checks` listing still
/// shows the check instead of silently dropping it off the checklist.
///
/// The re-stamp is always written under the HEAD scope, whichever domain
/// bounded the check. That is the presentation mechanism rather than the
/// bounding one: each head gets its own row, at its own tree, holding its own
/// counter at zero, so one head's diagnostic never renders as another head's
/// result while a standing machine or fleet defect stays bounded underneath.
///
/// The re-stamp carries `cached: true`, which the cache layer reads as
/// [`crate::execution::cache::InfraStreakOp::Hold`]: nothing executed, so no
/// counter moves. The row's own `output_tail` is left as the faithful record of
/// the last real infrastructure failure; the suppression is rendered FROM the
/// counter by every surface, rather than overwritten into the evidence.
pub(crate) fn record_suppressed_check(
    db: &Arc<LocalDb>,
    tree_hash: &str,
    job_id: &str,
    commit_sha: &str,
    check_name: &str,
    suppressed: &SuppressedCheck,
) -> CheckOutcome {
    let entry = suppressed.evidence();
    let streak = suppressed.streak();
    let _ = store_check_result(
        db.clone(),
        CheckResultCacheWrite {
            project_id: entry.project_id.clone(),
            tree_hash: tree_hash.to_string(),
            input_hash: entry.input_hash.clone(),
            check_name: check_name.to_string(),
            exit_code: entry.exit_code,
            passed: entry.passed,
            output_tail: entry.output_tail.clone(),
            duration_ms: entry.duration_ms,
            target_results_json: entry.target_results_json.clone(),
            job_id: Some(job_id.to_string()),
            cached: Some(true),
            failure_kind: entry.failure_kind.clone(),
            executor_id: entry.executor_id.clone(),
            executor_device_id: entry.executor_device_id.clone(),
            executor_connection_generation: entry.executor_connection_generation,
            executor_cell_id: entry.executor_cell_id.clone(),
            executor_lease_epoch: entry.executor_lease_epoch,
            executor_started_at_unix_ms: entry.executor_started_at_unix_ms,
            executor_finished_at_unix_ms: entry.executor_finished_at_unix_ms,
            toolchain_fingerprint: entry.toolchain_fingerprint.clone(),
            // A re-stamp reports the row that already exists; the definition
            // behind it is the one that failed, so it carries forward unchanged.
            defined_by_commit_sha: entry.defined_by_commit_sha.clone(),
            environment_fingerprint: infra_suppression_scope(job_id, commit_sha),
            verdict_platform: None,
            verdict_arch: None,
            verdict_environment_hash: None,
            transient_within_grace: false,
        },
    );
    log::info!(
        "check {check_name:?} is infrastructure-suppressed after {streak} consecutive \
         infrastructure failures; not executing it for job {job_id}"
    );
    CheckOutcome {
        left_running: None,
        consumer_guard: None,
        name: check_name.to_string(),
        passed: false,
        exit_code: None,
        failure_kind: entry
            .failure_kind
            .as_deref()
            .and_then(CheckFailureKind::from_stored)
            .or(Some(CheckFailureKind::Infrastructure)),
        parsed: None,
        output_tail: suppressed_check_message(suppressed),
        cached: false,
        duration_ms: 0,
        suppressed_after: Some(streak),
        // Nothing ran, so there is no observation of this evaluation. The
        // re-stamped legacy row is the last real failure's record, not this
        // evaluation's verdict.
        recorded: None,
        not_recorded: None,
    }
}

/// Record a lane submission never offered to placement because no machine the
/// fleet knows of could take it.
#[allow(clippy::too_many_arguments)]
pub(crate) fn record_unplaceable_check(
    db: &Arc<LocalDb>,
    project_id: &str,
    commit: CheckRunCommit<'_>,
    tree_hash: &str,
    job_id: &str,
    check_name: &str,
    input_hash: &str,
    reason: &str,
) -> CheckOutcome {
    log::info!("check {check_name:?} is not placeable for job {job_id}; not offered to placement");
    record_unattempted_check(
        db,
        project_id,
        commit,
        tree_hash,
        job_id,
        check_name,
        input_hash,
        CheckFailureKind::NotPlaceable,
        format!(
            "{reason} Not attempted: it runs on the first wave after a machine that satisfies it attaches."
        ),
    )
}

/// Record a lane submission never offered to placement because the project's
/// config does not load in this build. `refusal` is the project-wide refusal
/// preparation would have given.
#[allow(clippy::too_many_arguments)]
pub(super) fn record_unreadable_config_check(
    db: &Arc<LocalDb>,
    project_id: &str,
    commit: CheckRunCommit<'_>,
    tree_hash: &str,
    job_id: &str,
    check_name: &str,
    input_hash: &str,
    refusal: &str,
) -> CheckOutcome {
    log::info!(
        "check {check_name:?} for job {job_id} not offered to placement: the project config \
         does not load in this build"
    );
    record_unattempted_check(
        db,
        project_id,
        commit,
        tree_hash,
        job_id,
        check_name,
        input_hash,
        CheckFailureKind::ConfigUnreadable,
        format!(
            "{}. Not attempted: it runs on the first wave after the file loads.",
            refusal.trim_end_matches('.')
        ),
    )
}

/// Record a lane submission that was never offered to placement, as a `kind`
/// for which [`CheckFailureKind::attempted_nothing`] holds.
///
/// One row per head, written under the head scope so it renders on this commit
/// alone and is replaced in place by every later wave on the same commit rather
/// than accumulating. No observation is recorded, because nothing was evaluated,
/// and no streak moves (see [`crate::execution::cache::CheckResultCacheWrite::infra_streak_op`]):
/// the lane is not failing, it is waiting for its cause to clear.
#[allow(clippy::too_many_arguments)]
pub(super) fn record_unattempted_check(
    db: &Arc<LocalDb>,
    project_id: &str,
    commit: CheckRunCommit<'_>,
    tree_hash: &str,
    job_id: &str,
    check_name: &str,
    input_hash: &str,
    kind: CheckFailureKind,
    output_tail: String,
) -> CheckOutcome {
    debug_assert!(kind.attempted_nothing(), "{kind:?} describes an attempt");
    if let Err(error) = store_check_result(
        db.clone(),
        CheckResultCacheWrite {
            project_id: project_id.to_string(),
            tree_hash: tree_hash.to_string(),
            input_hash: input_hash.to_string(),
            check_name: check_name.to_string(),
            exit_code: -1,
            passed: false,
            output_tail: output_tail.clone(),
            duration_ms: 0,
            target_results_json: None,
            job_id: Some(job_id.to_string()),
            cached: Some(false),
            failure_kind: Some(kind.as_str().to_string()),
            executor_id: None,
            executor_device_id: None,
            executor_connection_generation: None,
            executor_cell_id: None,
            executor_lease_epoch: None,
            executor_started_at_unix_ms: None,
            executor_finished_at_unix_ms: None,
            toolchain_fingerprint: None,
            defined_by_commit_sha: Some(commit.defined_by.to_string()),
            environment_fingerprint: infra_suppression_scope(job_id, commit.evaluated),
            verdict_platform: None,
            verdict_arch: None,
            verdict_environment_hash: None,
            transient_within_grace: false,
        },
    ) {
        log::warn!(
            "check {check_name:?}: could not record that it was not attempted ({}): {error}",
            kind.as_str()
        );
    }
    CheckOutcome {
        left_running: None,
        consumer_guard: None,
        name: check_name.to_string(),
        passed: false,
        exit_code: None,
        failure_kind: Some(kind),
        parsed: None,
        output_tail,
        cached: false,
        duration_ms: 0,
        suppressed_after: None,
        recorded: None,
        not_recorded: None,
    }
}

/// Account for a check Cairn cancelled before it produced a verdict.
///
/// Nothing is written. No observation, because nothing was evaluated; no cache
/// row, because a cancellation says nothing about the tree or the machine, so it
/// must not advance an infrastructure streak, claim an escalation, or replace
/// the last real verdict on the board. The next wave for this tree runs it.
pub(super) fn record_cancelled_check(
    job_id: &str,
    check_name: &str,
    cause: crate::fleet::CancelCause,
) -> CheckOutcome {
    log::info!(
        "check {check_name} for job {job_id}: cancelled: {}",
        cause.sentence()
    );
    CheckOutcome {
        left_running: None,
            consumer_guard: None,
        name: check_name.to_string(),
        passed: false,
        exit_code: None,
        failure_kind: Some(CheckFailureKind::Cancelled),
        parsed: None,
        output_tail: format!(
            "Cairn cancelled this check before it produced a verdict: {}. No verdict was recorded; it runs again the next time checks run for this tree.",
            cause.sentence()
        ),
        cached: false,
        duration_ms: 0,
        suppressed_after: None,
        recorded: None,
        not_recorded: Some(format!("cancelled: {}", cause.sentence())),
    }
}

pub(super) fn left_running_message(attempt: &crate::fleet::LeftRunningAttempt) -> String {
    format!(
        "the runner stopped at {} while this lane ran on {} (request {}); awaiting adoption",
        crate::clock::stamp_millis(attempt.stopped_at_ms as i64)
            .unwrap_or_else(|| attempt.stopped_at_ms.to_string()),
        attempt.machine,
        attempt.request_id,
    )
}

pub(super) fn record_left_running_check(
    check_name: &str,
    attempt: crate::fleet::LeftRunningAttempt,
) -> CheckOutcome {
    CheckOutcome {
        name: check_name.to_string(),
        passed: false,
        exit_code: None,
        failure_kind: None,
        parsed: None,
        output_tail: left_running_message(&attempt),
        cached: false,
        duration_ms: 0,
        suppressed_after: None,
        recorded: None,
        not_recorded: Some("awaiting adoption".to_string()),
        left_running: Some(attempt),
        consumer_guard: None,
    }
}

/// Whether this outcome holds no verdict at all — the check never ran, or Cairn
/// ended it — so a summary must not mark it red.
pub(super) fn holds_no_verdict(outcome: &CheckOutcome) -> bool {
    outcome.left_running.is_some()
        || outcome
            .failure_kind
            .is_some_and(|kind| kind.attempted_nothing() || kind == CheckFailureKind::Cancelled)
}

pub(super) async fn submit_review_check(
    orch: &Orchestrator,
    result_identity: crate::execution::cache::CheckResultIdentity,
    request: CellRequest,
    mut recording_plan: CheckAdoptionRecordingPlan,
) -> Result<CheckExecResult, CheckExecutionFailure> {
    let build_service = orch.build_service_diagnostic_snapshot("sccache");
    if let Some(failure) = active_build_service_failure(&build_service) {
        return Err(CheckExecutionFailure::Substrate(
            SubstrateFailure::new(SubstrateFailureShape::Dispatch, failure)
                .implicating_build_service(),
        ));
    }
    let (consumer, consumer_ref) = check_consumer_identity(
        orch,
        request.requesting_job_id.as_deref().unwrap_or_default(),
        &recording_plan.tool_use_id,
        None,
    )
    .await?;
    recording_plan.result_shape = CheckAdoptionResultShape::Single;
    orch.fleet.register_check_claim(
        (request.request_id.clone(), request.attempt_id.clone()),
        crate::storage::check_lane_adoptions::Claim {
            request_id: request.request_id.clone(),
            attempt_id: request.attempt_id.clone(),
            project_id: result_identity.project_id.clone(),
            check_names: vec![result_identity.check_name.clone()],
            recording_plans: vec![
                serde_json::to_value(recording_plan).map_err(|error| error.to_string())?
            ],
            input_hashes: vec![result_identity.input_hash.clone()],
            platform: request
                .executor
                .as_ref()
                .and_then(|selector| selector.os.clone())
                .unwrap_or_else(|| "any".into()),
            commit_sha: request.base_commit.clone(),
            consumer: consumer.into(),
            consumer_ref,
            executor_id: String::new(),
            machine: String::new(),
            granted_at_ms: 0,
            state: "running".into(),
            stopped_at_ms: None,
            evidence: None,
            observation_id: None,
            updated_ms: 0,
        },
    );
    let identity = (request.request_id.clone(), request.attempt_id.clone());
    let result = check_result_from_delivery(
        orch.fleet
            .submit_pure_verdict(orch, result_identity, request)
            .await,
    );
    if matches!(&result, Err(error) if !matches!(error, CheckExecutionFailure::LeftRunning(_))) {
        lose_check_claim(
            orch,
            &identity,
            "the granted check did not deliver an item to its recorder",
        )
        .await;
    }
    result
}

#[derive(Debug)]
pub(crate) enum ReviewTreeGateResult {
    Green,
    CheckFailed {
        name: String,
        detail: String,
    },
    InfrastructureFailure(String),
    /// Cairn cancelled a gating check before it produced a verdict, naming
    /// which check and why. Neither a red nor a substrate failure: the tree
    /// was not judged, so the caller asks again rather than refusing it.
    Cancelled(String),
}

#[allow(clippy::too_many_arguments)]
pub(super) fn key_review_tree_plans(
    plans: Vec<CheckPlan>,
    checks: &HashMap<String, CheckCommand>,
    inputs: &ResolvedInputs,
    tree_entries: &[(String, String)],
    tree_hash: &str,
    platform: &str,
    toolchain: &str,
    basis: ChangeBasis<'_>,
) -> Vec<(CheckPlan, String)> {
    plans
        .into_iter()
        .map(|plan| {
            let configured = checks
                .get(&plan.name)
                .expect("planned check must retain its configured definition");
            let key = check_result_key_for_executed_command(
                configured,
                &plan.command,
                inputs.for_check(&plan.name),
                Some(tree_entries),
                tree_hash,
                platform,
                toolchain,
                CheckChange {
                    name: &plan.name,
                    basis,
                },
            );
            (plan, key)
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn verify_review_tree(
    orch: &Orchestrator,
    result_db: std::sync::Arc<LocalDb>,
    project_id: &str,
    repository: &str,
    planning_root: &Path,
    commit_id: &str,
    base_commit_id: &str,
    tree_hash: &str,
    tree_entries: &[(String, String)],
    changed: &[GraphFileChange],
    requesting_job_id: &str,
    priority: CellPriority,
) -> ReviewTreeGateResult {
    match run_prospective_tree_checks(
        orch,
        result_db,
        project_id,
        repository,
        planning_root,
        commit_id,
        base_commit_id,
        tree_hash,
        tree_entries,
        changed,
        requesting_job_id,
        priority,
    )
    .await
    {
        Ok(outcomes) => review_tree_gate_result(outcomes),
        Err(error) => ReviewTreeGateResult::InfrastructureFailure(error),
    }
}

/// Run the combined-tree gate's checks (applicable `policy: gate` review
/// checks on the runner's gating platform) against a prospective commit, keyed
/// and sealed to that commit's whole tree. `Ok` with no outcomes means nothing
/// applied; `Err` means planning failed before anything was submitted.
#[allow(clippy::too_many_arguments)]
pub(super) async fn run_prospective_tree_checks(
    orch: &Orchestrator,
    result_db: std::sync::Arc<LocalDb>,
    project_id: &str,
    repository: &str,
    planning_root: &Path,
    commit_id: &str,
    base_commit_id: &str,
    tree_hash: &str,
    tree_entries: &[(String, String)],
    changed: &[GraphFileChange],
    requesting_job_id: &str,
    priority: CellPriority,
) -> Result<Vec<CheckOutcome>, String> {
    if changed.is_empty() {
        return Ok(Vec::new());
    }
    // The gate's definition comes from the PROSPECTIVE commit it is verifying,
    // not from the project checkout: the combined tree is what merges, so the
    // checks it must satisfy are the ones that tree declares.
    let Some(CommitChecksContract {
        contract:
            ChecksContract {
                checks,
                extra_inputs,
                ..
            },
        defined_by_commit,
    }) = load_checks_contract_at_commit(Path::new(repository), commit_id).await
    else {
        return Ok(Vec::new());
    };
    assert_eq!(
        defined_by_commit, commit_id,
        "a combined-tree gate must be defined by the tree it verifies"
    );
    // The gate evaluates the PROSPECTIVE tree, so its input closures are derived
    // from that tree's manifests — the same entries it keys verdicts by.
    let gate_repository = PathBuf::from(repository);
    // The prospective commit may be hidden (built and not installed); the pin
    // indexes it anyway, and the caller's object lease keeps it readable.
    let gate_jj = immutable_read_jj(
        &crate::jj::environment(&orch.jj_binary_path, &orch.config_dir),
        &gate_repository,
        &[commit_id, base_commit_id],
    )?;
    let blobs = TreeBlobs {
        jj: &gate_jj,
        repository: &gate_repository,
    };
    let snapshot = TreeSnapshot::new(Some(tree_entries), &blobs);
    let inputs = ResolvedInputs::resolve(&checks, &extra_inputs, &snapshot);
    let mut plans = applicable_combined_tree_gate_checks(&checks, &inputs, changed, planning_root);
    retain_gating_platform_plans(
        &mut plans,
        &checks,
        crate::execution::check_identity::gating_platform(),
    );
    if plans.is_empty() {
        return Ok(Vec::new());
    }
    if let Some(error) = plans.iter().find_map(|plan| plan.config_error.as_deref()) {
        return Err(format!("check planning failed before execution: {error}"));
    }

    let platform = check_platform_identity();
    let toolchain = check_toolchain_identity();
    let base_entries = key_base_entries(
        &checks,
        &gate_jj,
        &gate_repository,
        base_commit_id,
        commit_id,
        changed,
    );
    // Read before keying: a base-subtracting lane's key names the set it ships.
    let base_findings = if plans
        .iter()
        .any(|plan| crate::execution::lint_baseline::subtracts_base_findings(&plan.name))
    {
        let planned: Vec<String> = plans.iter().map(|plan| plan.name.clone()).collect();
        let (jj, repository, base, head, db, project) = (
            gate_jj.clone(),
            gate_repository.clone(),
            base_commit_id.to_string(),
            commit_id.to_string(),
            result_db.clone(),
            project_id.to_string(),
        );
        tokio::task::spawn_blocking(move || {
            crate::execution::lint_baseline::lint_baseline_for(
                &crate::execution::lint_baseline::LintBaselineSource {
                    jj: &jj,
                    object_repository: &repository,
                    repository: &repository,
                    base: &base,
                    head: &head,
                    base_label: None,
                },
                db,
                &project,
                planned.iter().map(String::as_str),
            )
        })
        .await
        .map_err(|error| format!("lint baseline task failed: {error}"))?
    } else {
        None
    };
    let keyed = key_review_tree_plans(
        plans,
        &checks,
        &inputs,
        tree_entries,
        tree_hash,
        &platform,
        toolchain,
        ChangeBasis::Against {
            changed,
            base_entries: base_entries.as_deref(),
            base_findings: base_findings.as_ref(),
        },
    );

    let attributed_changed = changed;
    let base_content = if keyed
        .iter()
        .any(|(plan, _)| needs_executor_base_content(&plan.name))
    {
        match executor_base_content_bundle(
            &gate_jj,
            &gate_repository,
            base_commit_id,
            commit_id,
            attributed_changed,
        ) {
            Ok(content) => Some(content),
            Err(error) => {
                return Err(format!(
                    "build executor protocol base content from {base_commit_id}: {error}"
                ))
            }
        }
    } else {
        None
    };
    // Carry attribution and comparison content in the request itself. Coordinator-local
    // temporary paths are unreadable when this pure-verdict check spills to a remote cell.
    let mut env = check_transport_env(attributed_changed, base_content);
    env.extend(
        base_findings
            .as_ref()
            .map(crate::execution::lint_baseline::lint_baseline_env),
    );
    let timeouts: Vec<u32> = keyed
        .iter()
        .map(|(plan, _)| {
            resolve_check_timeout_ms(checks.get(&plan.name), DEFAULT_REVIEW_CHECK_TIMEOUT_MS)
        })
        .collect();
    let command_identity_inputs: Vec<_> = keyed
        .iter()
        .map(|(plan, _)| {
            let configured = checks
                .get(&plan.name)
                .expect("planned check must retain its configured definition");
            let mut executed = configured.clone();
            executed.command = plan.command.clone();
            CheckResourceIdentityInput::Configured {
                name: plan.name.clone(),
                check: executed,
            }
        })
        .collect();
    let executors: Vec<_> = keyed
        .iter()
        .map(|(plan, _)| {
            checks
                .get(&plan.name)
                .and_then(|check| check.executor.clone())
        })
        .collect();
    let verdict_platforms: Vec<_> = keyed
        .iter()
        .map(|(plan, _)| {
            checks
                .get(&plan.name)
                .map(crate::execution::check_identity::verdict_platforms)
                .unwrap_or_default()
        })
        .collect();
    // Checks park until admission. Capacity is a delay, including when worktree
    // population legitimately pins a pure verdict to the colocated executor;
    // requester liveness and cancellation remain the bounds.
    let project = project_id.to_string();
    let repository = repository.to_string();
    let commit = commit_id.to_string();
    let job = requesting_job_id.to_string();
    // Resolved once, here, and handed to both the submission that reserves each
    // item and the runner that stores what comes back. These are the checks that
    // can name a machine the fleet may not have, so the fleet counter is the one
    // that keeps `executor-windows-check` from re-arming on every commit.
    let infra_scopes = plan_infra_scopes(
        Some(orch),
        InfraScopeInputs {
            project_id,
            job_id: requesting_job_id,
            commit_sha: commit_id,
            repository: &repository,
            mutation_policy: MutationPolicy::PureVerdict,
            work_class: crate::execution::wire::check_placement_work_class(priority),
        },
        &(0..keyed.len())
            .map(|index| CheckPlacementQuestion {
                selector: executors[index].clone(),
                verdict_platforms: verdict_platforms[index].clone(),
                compiles_rust: CheckPlacementQuestion::command_compiles_rust(
                    &keyed[index].0.command,
                ),
            })
            .collect::<Vec<_>>(),
    )
    .await;
    let resolved_owner =
        crate::execution::checks_turn_end::resolve_job_coords(&result_db, requesting_job_id)
            .await
            .ok()
            .flatten()
            .map(|coords| cairn_common::executor_protocol::CellOwnerRef {
                project_id: coords.project_id,
                project_key: Some(coords.project_key),
                issue_number: Some(coords.number),
                job_id: Some(requesting_job_id.to_string()),
                execution_seq: Some(coords.exec_seq),
                node_kind: Some(coords.node_segment),
            });
    let keyed_ref = &keyed;
    let outcomes = {
        let execution_env = env.clone();
        let commit_coordinate = CheckRunCommit {
            evaluated: commit_id,
            defined_by: &defined_by_commit,
        };
        let sealed_coordinates = sealed_coordinates(commit_coordinate, tree_hash, &keyed);
        let recording_plans: Vec<_> = keyed
            .iter()
            .zip(&sealed_coordinates)
            .map(|((plan, _), coordinate)| {
                CheckAdoptionRecordingPlan::new(
                    plan,
                    coordinate,
                    requesting_job_id,
                    &format!("merge-gate:{requesting_job_id}"),
                    &execution_env,
                )
            })
            .collect();
        run_planned_checks_at_commit_with_env_and_coordinates(
            result_db,
            project_id,
            commit_coordinate,
            tree_hash,
            requesting_job_id,
            &keyed,
            &sealed_coordinates,
            &infra_scopes,
            &format!("merge-gate:{requesting_job_id}"),
            CheckExecMode::Isolated,
            Some(orch),
            &env,
            move |index, command, _| {
                let project = project.clone();
                let repository = repository.clone();
                let commit = commit.clone();
                let job = job.clone();
                let owner = resolved_owner.clone();
                let env = execution_env.clone();
                let timeout_ms = timeouts[index];
                let executor = executors[index].clone();
                let verdict_platforms = verdict_platforms[index].clone();
                let resource_identity = command_identity_inputs[index].clone();
                let resource_class = keyed_ref[index].0.resource_class;
                let check_name = keyed_ref[index].0.name.clone();
                let recording_plan = recording_plans[index].clone();
                async move {
                    let request_id = uuid::Uuid::new_v4().to_string();
                    orch.set_turn_end_check_request_id(
                        &job,
                        check_name.clone(),
                        request_id.clone(),
                    );
                    let request = build_review_check_request(ReviewCheckWireInput {
                        request_id,
                        attempt_id: uuid::Uuid::new_v4().to_string(),
                        project_id: project.clone(),
                        repository,
                        base_commit: commit,
                        command,
                        owner,
                        env,
                        priority,
                        timeout_ms: timeout_ms.into(),
                        requesting_job_id: job.clone(),
                        executor,
                        verdict_platforms,
                        resource_identity,
                        resource_class,
                    });
                    submit_review_check(
                        orch,
                        crate::execution::cache::CheckResultIdentity::new(
                            &project,
                            &keyed_ref[index].0.name,
                            &keyed_ref[index].1,
                        ),
                        request,
                        recording_plan,
                    )
                    .await
                }
            },
            |_| {},
        )
        .await
    };
    Ok(outcomes)
}

pub(super) fn review_tree_gate_result(outcomes: Vec<CheckOutcome>) -> ReviewTreeGateResult {
    // A cancelled lane says nothing about the tree, so it must neither pass the
    // gate nor hide a real red behind it: it answers only when nothing else did.
    let mut cancelled = None;
    for outcome in outcomes {
        if outcome.passed {
            continue;
        }
        if outcome.failure_kind == Some(CheckFailureKind::Cancelled) {
            cancelled.get_or_insert_with(|| {
                format!(
                    "check '{}' {}",
                    outcome.name,
                    outcome.not_recorded.as_deref().unwrap_or("cancelled")
                )
            });
            continue;
        }
        if outcome
            .failure_kind
            .is_some_and(CheckFailureKind::is_infrastructure)
        {
            return ReviewTreeGateResult::InfrastructureFailure(format!(
                "check '{}': {}",
                outcome.name, outcome.output_tail
            ));
        }
        return ReviewTreeGateResult::CheckFailed {
            name: outcome.name,
            detail: outcome.output_tail,
        };
    }
    match cancelled {
        Some(detail) => ReviewTreeGateResult::Cancelled(detail),
        None => ReviewTreeGateResult::Green,
    }
}
