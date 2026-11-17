use super::*;

pub(super) fn format_fixed_batch_summary(summary: &str, commit: &str, paths: &[String]) -> String {
    let short = commit.get(..12).unwrap_or(commit);
    let verdicts = summary.strip_prefix("Checks: ").unwrap_or(summary);
    format!(
        "Checks: ✓ write-check fixes (fixed, {short}; {} file{}) · {verdicts}",
        paths.len(),
        if paths.len() == 1 { "" } else { "s" }
    )
}

pub(super) fn delta_patch_excerpt(repo: &Path, delta: &crate::fleet::MutationDelta) -> String {
    std::process::Command::new("git")
        .args(["diff", "--binary", &delta.base_commit, &delta.delta_commit])
        .current_dir(repo)
        .output()
        .ok()
        .map(|output| {
            String::from_utf8_lossy(&output.stdout)
                .chars()
                .take(4_000)
                .collect()
        })
        .unwrap_or_else(|| "(delta patch unavailable)".into())
}

/// The write cadence's SUBMISSION order: declared fixers first, then every
/// other check in plan order.
///
/// This ordering is the whole mechanism behind the one-execution-per-commit
/// contract. Write-cadence checks share ONE slot working tree and run
/// sequentially inside it under `MutationPolicy::AllowDelta`, which takes no
/// per-item snapshot (`after_item` is wired only for the pure-verdict lane), so
/// each check observes every write the checks before it made. Running the
/// declared fixers first therefore makes every later check a verdict on the tree
/// that will actually land, and the fix folds into the sealed commit without
/// re-running anything.
///
/// Only submission order changes. Plan indices — and with them the status board,
/// the per-check output streams, and the rendered summary — stay in plan order.
pub(super) fn fixer_first_submission_order(
    keyed: &[(CheckPlan, String)],
    checks: &HashMap<String, CheckCommand>,
    indices: &[usize],
) -> Vec<usize> {
    let mut ordered = indices.to_vec();
    ordered.sort_by_key(|index| {
        let fixes = checks
            .get(&keyed[*index].0.name)
            .is_some_and(|check| check.fixes);
        (!fixes, *index)
    });
    ordered
}

/// Whether a folded fix is fully explained by the declared fixers that ran.
///
/// The wave's verdicts describe the post-fix tree only because the fixers ran
/// first. A fixed path that no declared fixer's INPUT SET covers means some other
/// check rewrote the tree mid-wave, and whatever ran before it validated a tree
/// that never landed — so those verdicts cannot be keyed to the landed tree and
/// must be re-verified. Uncertainty (an uncompilable glob, or no declared fixer
/// at all) resolves to NOT attributed: that costs one extra verification batch,
/// never a verdict recorded against a tree it never saw.
///
/// The combined delta is the only mutation evidence a folding batch produces —
/// the executor attributes dirt per item only on the pure-verdict lane, which
/// reverts after each item and so cannot fold. Attribution therefore rests on
/// the same declaration the result cache already rests on: an undeclared check
/// that rewrites paths INSIDE a declared fixer's input set is indistinguishable
/// from the fixer's own output, and the wave would trust it. Closing that would
/// take observe-only per-item dirt reporting in the executor (CAIRN-3155);
/// until then the declared inputs and `fixes` are the trust boundary, as
/// everywhere else.
pub(super) fn fix_is_attributed_to_declared_fixers(
    paths: &[String],
    fixers: &[&InputSelector],
) -> bool {
    !fixers.is_empty()
        && paths
            .iter()
            .all(|path| fixers.iter().any(|fixer| fixer.matches(path)))
}

/// Whether a check's batch verdict still describes the tree a fix landed.
///
/// Two independent proofs, either of which suffices:
///
/// - the fix left the check's impact-filtered inputs untouched, so its cache key
///   is unchanged — the same argument the result cache itself rests on; or
/// - the check ran after every mutation the wave folded. Ordering supplies that:
///   [`fixer_first_submission_order`] puts the declared fixers first, so a
///   NON-fixer observes all of them, and the LAST declared fixer observes all
///   the fixers before it plus its own output.
///
/// The last qualifier is the whole reason `superseded_by_later_fixer` exists. A
/// wave may declare several fixers, and they run in plan order among themselves,
/// so an EARLIER fixer never sees a later one's output — Prettier passes, then
/// `eslint --fix` rewrites the same file, and Prettier's green verdict would be
/// keyed to a tree it never checked. Impact globs cannot separate the two: their
/// paths overlap by construction, which is exactly why they are both fixers. So
/// every fixer but the last is re-verified whenever the fold moved its key.
///
/// A fixer's own output is not an invalidation of its own verdict: producing
/// this tree is what it just did, and re-checking it against that is precisely
/// the redundant second wave this rule exists to delete.
///
/// `fix_landed_on_tested_base` is false when the branch advanced while the wave
/// ran and the fix was merged onto the newer head. The fixed commit then carries
/// code no check in the wave executed, so only an unchanged input key carries.
///
/// The ordering proof covers the TREE only. A base-relative check ran after the
/// fixers but was handed the change of the commit the wave was planned at, so
/// it carries across a fold only when its key, which names that change, did
/// not move. A base-subtracting check is the same: it was handed the base
/// finding set the wave was planned with, which its key names.
pub(super) fn verdict_survives_fix(
    fix_landed_on_tested_base: bool,
    executed: bool,
    fix_attributed: bool,
    superseded_by_later_fixer: bool,
    key_before: &str,
    key_after: &str,
    base_relative: bool,
) -> bool {
    key_before == key_after
        || (!base_relative
            && fix_landed_on_tested_base
            && fix_attributed
            && executed
            && !superseded_by_later_fixer)
}

/// The submitted plan indices whose declared fix a LATER declared fixer could
/// have overwritten — every fixer in the wave except the last one to run.
///
/// `order` is the wave's submission order, so the fixers form its prefix and the
/// last of them is the one that observed all the others. A non-fixer is never
/// here: it runs after the whole fixer prefix.
pub(super) fn fixers_superseded_by_a_later_fixer(
    keyed: &[(CheckPlan, String)],
    checks: &HashMap<String, CheckCommand>,
    order: &[usize],
) -> BTreeSet<usize> {
    let submitted_fixers: Vec<usize> = order
        .iter()
        .copied()
        .filter(|index| {
            checks
                .get(&keyed[*index].0.name)
                .is_some_and(|check| check.fixes)
        })
        .collect();
    submitted_fixers
        .iter()
        .rev()
        .skip(1)
        .copied()
        .collect::<BTreeSet<usize>>()
}

pub(super) fn split_write_check_batch_outcome(
    outcome: CellOutcome,
    account: &crate::fleet::CellAccount,
    expected: usize,
) -> (
    Vec<Result<CheckExecResult, CheckExecutionFailure>>,
    Option<crate::fleet::MutationDelta>,
) {
    match outcome {
        CellOutcome::Completed {
            output,
            metadata,
            mutation_delta,
            ..
        } => {
            let decoded = serde_json::from_str::<
                Vec<cairn_common::executor_protocol::ProcessBatchItemOutcome>,
            >(&output);
            match decoded {
                Ok(items) if items.len() == expected => {
                    let results = items
                        .into_iter()
                        .map(|item| {
                            let provenance = metadata.for_batch_item(&item);
                            if let Some(stop) = &item.stop {
                                return Err(stopped_command_failure(
                                    stop, &item.body, provenance, account,
                                ));
                            }
                            let mut output = item.body;
                            append_sandbox_denial_evidence(&mut output, &item.sandbox_denials);
                            append_tracked_modification_evidence(
                                &mut output,
                                item.tracked_modifications.as_ref(),
                            );
                            Ok(CheckExecResult {
                                exit_code: item.exit_code,
                                output,
                                timed_out: item.timed_out,
                                duration_ms: Some(
                                    i64::try_from(item.duration_ms).unwrap_or(i64::MAX),
                                ),
                                provenance: Some(provenance),
                                publication: None,
                            })
                        })
                        .collect();
                    (results, mutation_delta.map(|delta| *delta))
                }
                Ok(items) => batch_failure_results(
                    expected,
                    CheckExecutionFailure::substrate(
                        SubstrateFailureShape::Result,
                        format!(
                            "executor returned {} item outcomes for {expected} checks",
                            items.len()
                        ),
                    ),
                ),
                Err(error) => batch_failure_results(
                    expected,
                    CheckExecutionFailure::substrate(
                        SubstrateFailureShape::Result,
                        format!("decode typed write-check batch outcomes: {error}"),
                    ),
                ),
            }
        }
        // The composed failure travels whole: flattening it to text here would
        // discard the operator half before it ever reaches the log.
        other => {
            let failure = check_result_from_cell_outcome(other, account, None)
                .err()
                .unwrap_or_else(|| {
                    CheckExecutionFailure::substrate(
                        SubstrateFailureShape::Result,
                        "write-check batch produced no result",
                    )
                });
            batch_failure_results(expected, failure)
        }
    }
}

pub(super) fn batch_failure_results(
    expected: usize,
    failure: CheckExecutionFailure,
) -> (
    Vec<Result<CheckExecResult, CheckExecutionFailure>>,
    Option<crate::fleet::MutationDelta>,
) {
    ((0..expected).map(|_| Err(failure.clone())).collect(), None)
}

/// Everything a write wave's synchronous planning unit reads, resolved before it
/// runs.
pub(super) struct WriteWavePlanning {
    pub(super) jj: JjEnv,
    pub(super) repository: PathBuf,
    /// The object database contracts are read from, the base's included.
    pub(super) object_repository: PathBuf,
    /// The commit the wave evaluates: the one the triggering call sealed.
    pub(super) head: String,
    pub(super) base: String,
    /// The contract `head` declares.
    pub(super) contract: ChecksContract,
    pub(super) db: Arc<LocalDb>,
    pub(super) project_id: String,
    pub(super) job_id: String,
}

/// A write wave's plan, every part of it derived from one commit.
pub(super) struct PlannedWriteWave {
    /// The applicable `when:write` checks, each paired with its cache key on
    /// `sealed_commit`'s tree.
    pub(super) keyed: Vec<(CheckPlan, String)>,
    pub(super) tree_hash: String,
    /// The commit the slot materializes and the observations are recorded under.
    pub(super) sealed_commit: String,
    pub(super) transport: Vec<(String, String)>,
}

/// Plan a write wave against `planning.head`: its changed files since `base`,
/// the checks they select, and each check's cache key on that tree. `None` when
/// nothing applies (or the change set is unreadable); `Some(Err)` when a
/// selected check's transport cannot be built.
///
/// Synchronous: VCS inspection and the cache bridge wait on subprocesses and
/// joined threads, so callers run it off the async runtime's workers.
pub(super) fn plan_write_wave(
    planning: WriteWavePlanning,
) -> Option<Result<PlannedWriteWave, String>> {
    let WriteWavePlanning {
        jj,
        repository,
        object_repository,
        head,
        base,
        // Unsupported checks are never planned, so planning has no use for them.
        contract:
            ChecksContract {
                checks,
                extra_inputs,
                ..
            },
        db,
        project_id,
        job_id,
    } = planning;
    let changed = logical_changed_files(&jj, &repository, &base, &head)
        .inspect_err(|error| {
            log::warn!(
                "write-cadence checks: changed files of {base}..{head} unreadable; nothing \
                 planned: {error}"
            );
        })
        .ok()?;
    if changed.is_empty() {
        return None;
    }
    // The sealed tree's entry listing feeds BOTH halves of input resolution:
    // the manifest blobs the dependency closures are derived from, and the
    // per-check filtered-tree cache component. One read serves both, and it
    // is skipped entirely when no check declares inputs at all.
    let entries = if any_check_declares_inputs(checks.values()) {
        tree_entries(&jj, &repository, &head).ok()
    } else {
        None
    };
    let blobs = TreeBlobs {
        jj: &jj,
        repository: &repository,
    };
    let snapshot = TreeSnapshot::new(entries.as_deref(), &blobs);
    let inputs = ResolvedInputs::resolve(&checks, &extra_inputs, &snapshot);
    let plans = applicable_write_checks(&checks, &inputs, &changed, &repository);
    if plans.is_empty() {
        return None;
    }
    let tree_hash = logical_tree_hash(&jj, &repository, &head).ok()?;
    let wave_change = WaveChange {
        jj: &jj,
        repository: &repository,
        object_repository: &object_repository,
        base: &base,
        head: &head,
        changed: &changed,
    };
    let planned: Vec<&str> = plans.iter().map(|plan| plan.name.as_str()).collect();
    let base_findings = wave_base_findings(&wave_change, &planned, db.clone(), &project_id);
    let transport = match write_wave_transport(&wave_change, &planned, base_findings.as_ref()) {
        Ok(transport) => transport,
        Err(error) => return Some(Err(error)),
    };
    let base_entries = key_base_entries(&checks, &jj, &repository, &base, &head, &changed);
    let basis = ChangeBasis::Against {
        changed: &changed,
        base_entries: base_entries.as_deref(),
        base_findings: base_findings.as_ref(),
    };
    // The narrowing baseline is this JOB's last green run of each check, not the
    // project's most recent row of any status. Both qualifiers carry weight: a
    // sibling branch's tree is not an anchor on this branch's lineage, and a red
    // run must not displace the green one it superseded.
    let baseline_by_check: HashMap<String, CheckResultCacheEntry> =
        list_latest_passing_check_results_for_job(db.clone(), &job_id)
            .unwrap_or_default()
            .into_iter()
            .map(|row| (row.check_name.clone(), row))
            .collect();
    let keyed: Vec<(CheckPlan, String)> = plans
        .into_iter()
        .map(|plan| {
            let check = checks
                .get(&plan.name)
                .expect("planned check must retain its configured definition");
            let selector = inputs.for_check(&plan.name);
            let input_hash = check_input_hash(
                check,
                selector,
                entries.as_deref(),
                &tree_hash,
                CheckChange {
                    name: &plan.name,
                    basis,
                },
            );
            let should_reselect =
                get_check_result(db.clone(), &project_id, &plan.name, &input_hash)
                    .ok()
                    .flatten()
                    .is_none();
            let selected_plan = if should_reselect {
                let selected_changed = selected_changed_files_for_miss(
                    baseline_by_check.get(&plan.name),
                    entries.as_deref(),
                    check,
                    &inputs,
                    selector,
                    &changed,
                    &blobs,
                );
                replan_one_check(&plan.name, check, &inputs, &selected_changed, &repository)
                    .unwrap_or(plan)
            } else {
                plan
            };
            (selected_plan, input_hash)
        })
        .collect();
    Some(Ok(PlannedWriteWave {
        keyed,
        tree_hash,
        sealed_commit: head,
        transport,
    }))
}

/// Where a write wave's change is read: `changed` is everything `head` changed
/// since `base`, both resolved in the logical `repository`.
pub(super) struct WaveChange<'a> {
    pub(super) jj: &'a JjEnv,
    pub(super) repository: &'a Path,
    /// The object database the base's checks contract is read from.
    pub(super) object_repository: &'a Path,
    pub(super) base: &'a str,
    pub(super) head: &'a str,
    pub(super) changed: &'a [GraphFileChange],
}

/// The base finding set the write-cadence checks of `change.head` ship and key
/// by, when one of them subtracts its base.
pub(super) fn wave_base_findings(
    change: &WaveChange<'_>,
    planned: &[&str],
    db: Arc<LocalDb>,
    project_id: &str,
) -> Option<crate::execution::lint_baseline::LintBaselineTransport> {
    crate::execution::lint_baseline::lint_baseline_for(
        &crate::execution::lint_baseline::LintBaselineSource {
            jj: change.jj,
            object_repository: change.object_repository,
            repository: change.repository,
            base: change.base,
            head: change.head,
            base_label: None,
        },
        db,
        project_id,
        planned.iter().copied(),
    )
}

/// The transport the write-cadence checks of `change.head` receive: its
/// changed files, the executor-protocol base bundle when a planned check is
/// shipped one, and the lint baseline ([`wave_base_findings`]) when one
/// subtracts its base.
///
/// It describes the commit the checks run at and no other. A wave whose fix
/// lands on a head that moved while it ran verifies at that newer head, so it
/// builds this again there: the change of the commit it was planned at omits
/// whatever the newer head added, and a check judging additions would read
/// those as files the change never touched.
pub(super) fn write_wave_transport(
    change: &WaveChange<'_>,
    planned: &[&str],
    base_findings: Option<&crate::execution::lint_baseline::LintBaselineTransport>,
) -> Result<Vec<(String, String)>, String> {
    let base_content = if planned.iter().any(|name| needs_executor_base_content(name)) {
        Some(executor_base_content_bundle(
            change.jj,
            change.repository,
            change.base,
            change.head,
            change.changed,
        )?)
    } else {
        None
    };
    let mut transport = check_transport_env(change.changed, base_content);
    transport.extend(base_findings.map(crate::execution::lint_baseline::lint_baseline_env));
    Ok(transport)
}

/// Give every substrate failure in a settled batch the time its request was
/// actually outstanding. A completed check carries its executor's own measured
/// duration and is left alone.
pub(super) fn stamp_substrate_elapsed(
    results: &mut HashMap<usize, Result<CheckExecResult, CheckExecutionFailure>>,
    elapsed: std::time::Duration,
) {
    for result in results.values_mut() {
        if let Err(CheckExecutionFailure::Substrate(failure)) = result {
            failure.lasted(elapsed);
        }
    }
}

/// Run the affected `when:write` checks after a source-touching commit has been
/// sealed, streaming their output live and returning a compact inline pass/fail
/// summary to append to the originating tool result.
///
/// Returns `None` whenever nothing applied: no run context (so no streaming
/// target), no `checks` contract, no resolvable changed-file set, an empty
/// change set, or no `when:write` check whose impact the change set matches (a
/// doc-only / non-source commit). A cache hit returns the stored verdict without
/// re-running.
///
/// The wave runs as its own task and the caller waits for it only while a
/// verdict is plausibly imminent (see [`await_write_check_wave`]). The commit
/// is already sealed when this is called, so the caller's result must not depend on the checks settling:
/// a wave that outlives the bound keeps running, keeps `/checks` reporting
/// it as in flight, and records its verdicts (and publishes any fix commit)
/// on its own, while the caller returns a pending line in its place.
pub(crate) async fn run_write_checks_after_seal(
    orch: &Orchestrator,
    run_context: Option<&RunContext>,
    cwd: &str,
    tool_use_id: &str,
    target: WriteCheckTarget<'_>,
) -> Option<String> {
    // No run context ⇒ no run id to stream against and no job to anchor the diff.
    let run_context = run_context?.clone();
    let guard = WriteChecksInFlightGuard::new(orch, &run_context.job_id);
    // Opened before the wave is spawned, so a fold that finds the branch moved
    // to this commit sees the wave planning from the moment the commit landed.
    let claims = orch.lane_claims.open_wave(&run_context.job_id);
    let late_orch = orch.clone();
    let late_run_id = run_context.run_id.clone();
    let late_job_id = run_context.job_id.clone();
    let wave_orch = orch.clone();
    let cwd = cwd.to_string();
    let tool_use_id = tool_use_id.to_string();
    let sealed_resolution = target.sealed_resolution();
    let (progress, progress_seen) = tokio::sync::watch::channel(WaveProgress::default());
    await_write_check_wave(
        guard,
        WRITE_CHECK_INLINE_WAIT_BOUNDS,
        progress_seen,
        async move {
            run_write_checks_after_seal_inner(
                &wave_orch,
                Some(&run_context),
                &cwd,
                &tool_use_id,
                sealed_resolution.as_ref(),
                progress,
                claims,
            )
            .await
        },
        move |summary| {
            deliver_late_write_check_summary(late_orch, late_run_id, late_job_id, summary)
        },
    )
    .await
}

/// The commit a write-cadence wave evaluates.
pub(crate) enum WriteCheckTarget<'a> {
    /// The call sealed `commit` on `branch`. `branch` is the resolution the
    /// call prepared against, so its `commit_id` is the branch's PRE-write tip:
    /// only its coordinates are taken, and the wave evaluates `commit`.
    Sealed {
        branch: &'a crate::mcp::handlers::branch::BranchResolution,
        commit: &'a str,
    },
    /// The calling session's own branch head, resolved when the wave starts.
    CurrentHead,
}

impl WriteCheckTarget<'_> {
    /// The branch resolved AT the sealed commit, or `None` when the wave is to
    /// resolve the session's head itself.
    pub(super) fn sealed_resolution(
        &self,
    ) -> Option<crate::mcp::handlers::branch::BranchResolution> {
        match self {
            Self::Sealed { branch, commit } => {
                Some(crate::mcp::handlers::branch::BranchResolution {
                    commit_id: (*commit).to_string(),
                    ..(*branch).clone()
                })
            }
            Self::CurrentHead => None,
        }
    }
}

/// The longest a committing `write` or `run` waits inline for its when:write
/// checks. Stated in the write time contract so the callback layers wrapping
/// the host can size themselves above it.
pub(super) const WRITE_CHECK_INLINE_WAIT: std::time::Duration =
    std::time::Duration::from_millis(cairn_common::write_contract::WRITE_CHECK_INLINE_WAIT_MS);

/// The two bounds a committing call's inline wait for its write checks keeps,
/// both counted from the call.
#[derive(Clone, Copy, Debug)]
pub(super) struct InlineWaitBounds {
    /// How long to wait while no lane of the wave has been admitted.
    pub(super) start: std::time::Duration,
    /// How long to wait in all once one has.
    pub(super) total: std::time::Duration,
}

pub(super) const WRITE_CHECK_INLINE_WAIT_BOUNDS: InlineWaitBounds = InlineWaitBounds {
    start: std::time::Duration::from_millis(
        cairn_common::write_contract::WRITE_CHECK_START_WAIT_MS,
    ),
    total: WRITE_CHECK_INLINE_WAIT,
};

/// How a committing call's inline wait for its write checks ended, by the
/// names the `write_check_inline_wait` profiler span records.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum InlineWaitOutcome {
    /// The wave's summary came back inside the wait.
    Served,
    /// No lane was admitted within the start bound.
    PendingNotStarted,
    /// A lane was running when the total bound expired.
    PendingRunning,
    /// A lane had been admitted, but none was running when the total bound
    /// expired: the wave was back in the queue, or preparing the batch that
    /// re-verifies its fix.
    PendingAtBound,
}

impl InlineWaitOutcome {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Served => "served",
            Self::PendingNotStarted => "pending_not_started",
            Self::PendingRunning => "pending_running",
            Self::PendingAtBound => "pending_at_bound",
        }
    }
}

/// How a wait that expired ended, from whether any lane was ever admitted and
/// where the wave stood at expiry. Admission is latched: a wave that ran and
/// then went back to provisioning had already earned the full wait.
pub(super) fn classify_expired_wait(admitted: bool, stage: WaveStage) -> InlineWaitOutcome {
    match (admitted, stage) {
        (false, _) => InlineWaitOutcome::PendingNotStarted,
        (true, WaveStage::Running) => InlineWaitOutcome::PendingRunning,
        (true, _) => InlineWaitOutcome::PendingAtBound,
    }
}

/// A wait that expired before its wave settled.
pub(super) struct PendingWave {
    pub(super) outcome: InlineWaitOutcome,
    pub(super) waited: std::time::Duration,
    pub(super) progress: WaveProgress,
}

/// Detach `wave` and wait for it only while its verdict is plausibly
/// imminent. Until `progress` reports a lane admitted (claimed by an
/// executor), the wait lasts at most `bounds.start`; once one has been, it
/// lasts up to `bounds.total`, counted from the call. A wave answered from the
/// cache settles at once and is returned at once.
///
/// The in-flight guard moves into the detached task, so the job stays "write
/// checks in flight" for exactly as long as the wave really runs, whether or
/// not anyone is still waiting on it. Dropping the caller (a transport
/// timeout, an interrupt) therefore no longer cancels checks for a commit that
/// has already landed.
///
/// Some outcomes live only in the wave's return value (a fold conflict and its
/// recovery patch, a non-convergent fix), so ownership of that value never
/// rests with the caller. A supervisor owns the wave from the start and offers
/// its result over a channel; if the caller is no longer receiving (it answered
/// pending, or was itself dropped before the bound), the supervisor hands the
/// result to `deliver_late` instead. Exactly one of the two receives it.
pub(super) async fn await_write_check_wave<F, D, DF>(
    guard: WriteChecksInFlightGuard,
    bounds: InlineWaitBounds,
    mut progress: tokio::sync::watch::Receiver<WaveProgress>,
    wave: F,
    deliver_late: D,
) -> Option<String>
where
    F: std::future::Future<Output = Option<String>> + Send + 'static,
    D: FnOnce(String) -> DF + Send + 'static,
    DF: std::future::Future<Output = ()> + Send + 'static,
{
    // Its duration is the wait's. A caller dropped mid-wait closes it still
    // reading `caller_dropped`.
    let span = tracing::info_span!(
        target: "profiler",
        "write_check_inline_wait",
        outcome = "caller_dropped",
        stage = tracing::field::Empty,
        executor = tracing::field::Empty,
        admitted = tracing::field::Empty,
        start_bound_ms = whole_ms(bounds.start),
        total_bound_ms = whole_ms(bounds.total),
    );
    let (offer, mut inline) = tokio::sync::oneshot::channel::<OfferedOutcome>();
    tokio::spawn(async move {
        let outcome = tokio::spawn(async move {
            let _guard = guard;
            wave.await
        })
        .await;
        hand_off_wave_outcome(offer, settled_wave_summary(outcome), deliver_late).await;
    });
    let started = tokio::time::Instant::now();
    let mut admitted = progress.borrow_and_update().admitted;
    let mut watching = true;
    let settled = loop {
        let bound = if admitted { bounds.total } else { bounds.start };
        tokio::select! {
            biased;
            offered = &mut inline => break Some(offered),
            changed = progress.changed(), if watching => {
                // The wave drops its sender as it finishes; its outcome still
                // arrives on `inline`.
                watching = changed.is_ok();
                admitted = progress.borrow_and_update().admitted;
            }
            () = tokio::time::sleep_until(started + bound) => break None,
        }
    };
    let last = progress.borrow().clone();
    match settled {
        Some(Ok(offered)) => {
            record_inline_wait(&span, InlineWaitOutcome::Served.as_str(), &last, admitted);
            log::debug!(
                "when:write checks answered inline after {}ms",
                started.elapsed().as_millis()
            );
            claim_offered_outcome(offered)
        }
        // The supervisor always sends unless the runtime is shutting down.
        Some(Err(_)) => {
            record_inline_wait(&span, "wave_lost", &last, admitted);
            None
        }
        None => {
            // Close before deciding: a result offered in the instant the bound
            // elapsed is still ours to return, and anything after the close is
            // refused and goes to late delivery instead.
            inline.close();
            if let Ok(offered) = inline.try_recv() {
                record_inline_wait(&span, InlineWaitOutcome::Served.as_str(), &last, admitted);
                return claim_offered_outcome(offered);
            }
            let pending = PendingWave {
                outcome: classify_expired_wait(admitted, last.stage),
                waited: started.elapsed(),
                progress: last,
            };
            record_inline_wait(&span, pending.outcome.as_str(), &pending.progress, admitted);
            let why = pending_wave_reason(&pending, unix_time_ms_for_checks());
            log::info!(
                "when:write checks pending after {}s ({}): {why}",
                pending.waited.as_secs(),
                pending.outcome.as_str()
            );
            Some(pending_write_checks_summary(&why))
        }
    }
}

/// Exercise the real timeout path without depending on fleet load or waiting
/// for the production bound. The held wave is released and joined before return.
#[cfg(feature = "test-utils")]
pub async fn expire_write_check_wait_for_test(orch: &Orchestrator) -> String {
    let guard = WriteChecksInFlightGuard::new(orch, "test-expired-write");
    let (_progress, seen) = tokio::sync::watch::channel(WaveProgress::default());
    let (release, released) = tokio::sync::oneshot::channel::<()>();
    let (finished, joined) = tokio::sync::oneshot::channel();
    let summary = await_write_check_wave(
        guard,
        InlineWaitBounds {
            start: std::time::Duration::ZERO,
            total: std::time::Duration::ZERO,
        },
        seen,
        async move {
            let _ = released.await;
            Some("Checks: ✓ held test wave".to_string())
        },
        move |_| async move {
            let _ = finished.send(());
        },
    )
    .await
    .expect("a held wave must outlive the zero inline bound");
    release.send(()).expect("the wave is still held");
    joined.await.expect("the detached wave must finish");
    assert!(!orch.write_checks_in_flight("test-expired-write"));
    summary
}

pub(super) fn record_inline_wait(
    span: &tracing::Span,
    outcome: &str,
    progress: &WaveProgress,
    admitted: bool,
) {
    span.record("outcome", outcome);
    span.record("stage", progress.stage.as_str());
    if let Some(executor) = progress.executor.as_deref() {
        span.record("executor", executor);
    }
    span.record("admitted", admitted);
}

pub(super) fn whole_ms(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// A wave's final summary as offered to its caller, with the receipt the caller
/// must return for the offer to count as delivered.
pub(super) type OfferedOutcome = (Option<String>, tokio::sync::oneshot::Sender<()>);

/// Offer a wave's outcome to its caller and keep ownership of it until the
/// caller acknowledges receipt. A successful `send` only means the value was
/// buffered: a caller cancelled before it polls the receiver drops the value
/// unread. Only an explicit receipt proves the caller holds it; anything else
/// (a refused send, a receiver dropped with the value inside) is late delivery.
pub(super) async fn hand_off_wave_outcome<D, DF>(
    offer: tokio::sync::oneshot::Sender<OfferedOutcome>,
    summary: Option<String>,
    deliver_late: D,
) where
    D: FnOnce(String) -> DF,
    DF: std::future::Future<Output = ()>,
{
    let (receipt, received) = tokio::sync::oneshot::channel::<()>();
    let claimed = match offer.send((summary.clone(), receipt)) {
        Ok(()) => received.await.is_ok(),
        Err(_) => false,
    };
    if !claimed {
        if let Some(summary) = summary {
            deliver_late(summary).await;
        }
    }
}

/// Take an offered outcome and acknowledge it in the same synchronous step, so
/// no cancellation point separates holding the value from the supervisor
/// learning that it is held.
pub(super) fn claim_offered_outcome((summary, receipt): OfferedOutcome) -> Option<String> {
    let _ = receipt.send(());
    summary
}

pub(super) fn settled_wave_summary(
    outcome: Result<Option<String>, tokio::task::JoinError>,
) -> Option<String> {
    match outcome {
        Ok(summary) => summary,
        Err(error) => {
            log::error!("when:write check wave ended abnormally: {error}");
            Some(format!(
                "Checks: \u{2717} the write-check wave ended abnormally ({error}). The commit is \
                 unaffected; no verdict was recorded for it. Read cairn:~/checks for the branch's current verdicts."
            ))
        }
    }
}

/// Hand a wave's final summary to the agent whose commit it checked, after that
/// agent's call already returned pending. A failure wakes an idle agent at its
/// next turn boundary; a pass rides along passively. A superseded node is never
/// woken (the turn-end cadence's resurrection fence, CAIRN-4665): its message is
/// still written, passively, so the outcome stays readable.
pub(super) async fn deliver_late_write_check_summary(
    orch: Orchestrator,
    run_id: String,
    job_id: String,
    summary: String,
) {
    use crate::messages::queued::DeliveryUrgency;
    let mut urgency = if summary.contains('\u{2717}') {
        DeliveryUrgency::Queue
    } else {
        DeliveryUrgency::Passive
    };
    if urgency != DeliveryUrgency::Passive {
        let live = match crate::execution::routing::owning_db_for_job(&orch.db, &job_id).await {
            Ok(db) => crate::execution::canonical::fence_for_job(&db, &job_id)
                .await
                .is_ok_and(|fence| !fence.is_superseded()),
            Err(_) => false,
        };
        if !live {
            urgency = DeliveryUrgency::Passive;
        }
    }
    let content = late_write_check_message(&summary);
    let delivered = tokio::task::spawn_blocking(move || {
        crate::messages::delivery::queue_system_direct(&orch, &run_id, &content, urgency)
    })
    .await;
    match delivered {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            log::warn!("late write-check summary for job {job_id} undeliverable: {error}")
        }
        Err(error) => {
            log::warn!("late write-check summary for job {job_id} delivery task failed: {error}")
        }
    }
}

pub(super) fn late_write_check_message(summary: &str) -> String {
    format!(
        "[Write checks] The when:write checks that were still pending when an earlier commit of yours \
         returned have finished.\n\n{summary}\n\nCurrent verdicts for the branch: cairn:~/checks"
    )
}

/// The line a committing call carries when its write checks outlive the inline
/// wait. It says why they are pending (`why`, from [`pending_wave_reason`]) so
/// a held lane reads differently from a slow suite, then leads with the fact
/// the agent most needs: the commit landed.
pub(super) fn pending_write_checks_summary(why: &str) -> String {
    format!(
        "Checks: \u{22EF} pending ({why}) \u{2014} the commit above landed; its when:write checks continue \
         in the background. Their verdicts (and any formatter fix, which lands as its own commit) appear \
         at cairn:~/checks, and a [Write checks] message follows when they finish."
    )
}

/// Why a wave is still pending when its wait expired: what holds it, or where
/// it is running and for how long.
pub(super) fn pending_wave_reason(wave: &PendingWave, now_unix_ms: u64) -> String {
    let waited = describe_ms(whole_ms(wave.waited));
    let progress = &wave.progress;
    let held = || {
        progress
            .reason
            .clone()
            .unwrap_or_else(|| stage_words(progress.stage).to_string())
    };
    match wave.outcome {
        InlineWaitOutcome::PendingRunning => {
            let on = progress
                .executor
                .as_deref()
                .map(|executor| format!(" on {executor}"))
                .unwrap_or_default();
            match progress.since_unix_ms {
                Some(since) => format!(
                    "running{on} since {}, {} in",
                    clock_utc(since),
                    describe_ms(now_unix_ms.saturating_sub(since))
                ),
                None => format!("running{on}, still running after {waited}"),
            }
        }
        InlineWaitOutcome::PendingAtBound => {
            format!("admitted but not running after {waited}: {}", held())
        }
        InlineWaitOutcome::PendingNotStarted | InlineWaitOutcome::Served => {
            format!("not admitted after {waited}: {}", held())
        }
    }
}

pub(super) fn stage_words(stage: WaveStage) -> &'static str {
    match stage {
        WaveStage::Planning => "still planning",
        WaveStage::Preparing => "still preparing the check's inputs",
        WaveStage::Queued => "queued, and no machine has claimed it",
        WaveStage::Running => "running",
    }
}

pub(super) struct WriteChecksInFlightGuard {
    pub(super) orch: Orchestrator,
    pub(super) job_id: String,
}

impl WriteChecksInFlightGuard {
    pub(super) fn new(orch: &Orchestrator, job_id: &str) -> Self {
        orch.begin_write_checks(job_id);
        let _ = orch.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "check_result_cache", "action": "update"}),
        );
        Self {
            orch: orch.clone(),
            job_id: job_id.to_string(),
        }
    }
}

impl Drop for WriteChecksInFlightGuard {
    fn drop(&mut self) {
        self.orch.end_write_checks(&self.job_id);
        let _ = self.orch.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "check_result_cache", "action": "update"}),
        );
    }
}

pub(super) async fn run_write_checks_after_seal_inner(
    orch: &Orchestrator,
    run_context: Option<&RunContext>,
    cwd: &str,
    tool_use_id: &str,
    sealed_resolution: Option<&crate::mcp::handlers::branch::BranchResolution>,
    progress: tokio::sync::watch::Sender<WaveProgress>,
    // Held until this function returns, which is after every verdict the wave
    // records has been recorded.
    mut claims: WaveClaims,
) -> Option<String> {
    // No run context ⇒ no run id to stream against and no job to anchor the diff.
    let run_context = run_context?;
    let owning_db = crate::execution::routing::owning_db_for_job(&orch.db, &run_context.job_id)
        .await
        .ok()?;
    let job_id = run_context.job_id.clone();
    let repo_path = owning_db
        .query_text(
            "SELECT p.repo_path FROM jobs j JOIN projects p ON p.id = j.project_id WHERE j.id = ?1",
            (job_id,),
        )
        .await
        .ok()??;
    let project_path = std::path::PathBuf::from(repo_path);
    let repo_root = project_path.as_path();

    // 1. Resolve the sealed commit this cadence is about, BEFORE anything reads a
    // check definition. VCS inspection, cargo-metadata planning, and the
    // synchronous cache bridge all wait on subprocesses or joined threads, so
    // gather the DB anchors asynchronously and keep the complete synchronous
    // planning unit off Tokio runtime workers.
    let live_base = live_node_base(orch, &run_context.job_id).await;
    let logical = match sealed_resolution {
        Some(resolution) => resolution.clone(),
        None => crate::mcp::handlers::branch::resolve_current_for_read(
            orch,
            &crate::mcp::types::McpCallbackRequest {
                run_id: Some(run_context.run_id.clone()),
                cwd: cwd.to_string(),
                ..Default::default()
            },
        )
        .await
        .ok()?,
    };

    // 2. Load the checks contract DECLARED BY that exact commit. The definition
    // and the content it evaluates are the same tree, so a `.cairn/config.yaml`
    // edit governs the commits that carry it and nothing else — a branch
    // experimenting with check config cannot reach a sibling job's cadence
    // (CAIRN-3333). A project-level Settings edit takes effect for a job on the
    // first commit that contains it.
    let CommitChecksContract {
        contract:
            ChecksContract {
                checks,
                extra_inputs,
                ..
            },
        defined_by_commit,
    } = load_checks_contract_at_commit(&logical.object_repository_path, &logical.commit_id).await?;
    if checks.is_empty() {
        return None;
    }
    let object_repository = logical.object_repository_path.clone();
    // Structural guard, deliberately a hard assert rather than a log: planning
    // selects, keys, and submits checks that this exact commit declared. If a
    // future edit reintroduces a second contract source, this fails visibly here
    // rather than silently recording a sibling's definition.
    assert_eq!(
        defined_by_commit, logical.commit_id,
        "write-cadence checks must be defined by the commit they evaluate"
    );

    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
    let planning = WriteWavePlanning {
        jj: jj.clone(),
        repository: logical.repository_path,
        object_repository: object_repository.clone(),
        head: logical.commit_id,
        base: live_base.unwrap_or(logical.default_commit_id),
        contract: ChecksContract {
            checks: checks.clone(),
            extra_inputs: extra_inputs.clone(),
            ..Default::default()
        },
        db: orch.db.local.clone(),
        project_id: run_context.project_id.clone(),
        job_id: run_context.job_id.clone(),
    };
    let planning_started = std::time::Instant::now();
    let planned = tokio::task::spawn_blocking(move || plan_write_wave(planning))
        .await
        .ok()??;
    let PlannedWriteWave {
        keyed,
        tree_hash,
        sealed_commit,
        transport,
    } = match planned {
        Ok(planned) => planned,
        Err(error) => {
            return Some(write_check_planning_infrastructure_failure(&error));
        }
    };

    // The planning unit above is synchronous and sits on the critical path of
    // every source-touching commit, between the sealed commit and the agent
    // getting its tool result. Its elapsed time is logged because that is exactly
    // where CAIRN-3108 hid: a quadratic cache read inside it pinned a
    // blocking-pool thread for ~59 SECONDS per commit while every subsystem that
    // does log stayed silent, so the stall was invisible in the logs for days.
    // Expect tens of milliseconds; seconds here means something in planning has
    // started scaling with history again.
    //
    // (The batch these plans feed submits under `MutationPolicy::AllowDelta`, not
    // a pure-verdict lease — the write cadence is the cadence that folds
    // formatter fixes back into the commit. The old wording of this line claimed
    // otherwise and sent this investigation's first pass chasing slot admission.)
    log::info!(
        "when:write checks: planned {} check(s) in {}ms; cache-filtering before submission",
        keyed.len(),
        planning_started.elapsed().as_millis()
    );

    // The live status-line emitter. `run_planned_checks` calls this with a full
    // checklist snapshot on every state transition; we forward each snapshot to
    // the frontend as a `check-status` event keyed by the committing call id.
    // Follows the `db-change` emit idiom below.
    let emitter = orch.services.emitter.clone();
    let notify_run_id = run_context.run_id.clone();
    let notify_tool_use_id = tool_use_id.to_string();
    // Per-check effective timeout, aligned to plan index (the `execute` closure
    // is indexed the same way). A check's schema `timeout` overrides the write
    // cadence default.
    let timeouts: Vec<u32> = keyed
        .iter()
        .map(|(plan, _)| {
            resolve_check_timeout_ms(checks.get(&plan.name), DEFAULT_WRITE_CHECK_TIMEOUT_MS)
        })
        .collect();
    let cell_env = slot_check_env(jj.shell_env());
    let mut process_env = write_check_transport_env(&cell_env, transport);
    // Reuse follows the suite's declared trust set, not the environment of the
    // executor that a miss may eventually select. Filter hits before placement so
    // a stored verdict consumes no build-slot capacity.
    let miss_indices: Vec<usize> = keyed
        .iter()
        .enumerate()
        .filter_map(|(index, (plan, input_hash))| {
            needs_execution(
                orch.db.local.clone(),
                &run_context.project_id,
                plan,
                input_hash,
                &process_env,
            )
            .then_some(index)
        })
        .collect();
    claims.claim(
        miss_indices
            .iter()
            .map(|index| lane_key(&run_context.project_id, &keyed[*index])),
        &sealed_commit,
    );
    let status_notify = wave_status_notify(
        move |checks, phase_status| {
            let _ = emitter.emit(
                "check-status",
                serde_json::to_value(CheckStatusPayload {
                    run_id: notify_run_id.clone(),
                    tool_use_id: notify_tool_use_id.clone(),
                    checks,
                    phase: phase_status.phase,
                    phase_label: phase_status.label,
                    phase_diagnostic: phase_status.diagnostic,
                })
                .unwrap_or(serde_json::Value::Null),
            );
        },
        progress,
    );
    let status_board = CheckStatusBoard::new(&keyed, status_notify);
    status_board.set_phase(
        (!miss_indices.is_empty()).then_some("dispatching"),
        (!miss_indices.is_empty()).then_some("dispatching checks"),
        (!miss_indices.is_empty()).then(|| "preparing check request".to_string()),
    );
    for (index, (plan, input_hash)) in keyed.iter().enumerate() {
        if miss_indices.contains(&index) {
            continue;
        }
        if let Ok(Some(hit)) = get_check_result(
            orch.db.local.clone(),
            &run_context.project_id,
            &plan.name,
            input_hash,
        ) {
            status_board.transition_cached(
                index,
                if hit.passed { "passed" } else { "failed" },
                Some("cached".into()),
            );
        }
    }

    // The write cell runs on this runner's colocated machine; a lane whose
    // platforms exclude it leaves the cell rather than waiting on a machine
    // that can never answer it.
    let split = split_write_wave(
        &keyed,
        &checks,
        &miss_indices,
        crate::execution::check_identity::gating_platform(),
        &orch
            .fleet
            .executor_name_for_text(crate::fleet::COLOCATED_EXECUTOR_ID),
    );
    let batch_outcome = if split.cell.is_empty() {
        None
    } else {
        Some(
            submit_write_check_batch_for(
                orch,
                WriteCheckBatchRequest {
                    run_context,
                    repo_root,
                    checks: &checks,
                    extra_inputs: &extra_inputs,
                    keyed: &keyed,
                    order: &fixer_first_submission_order(&keyed, &checks, &split.cell),
                    timeouts: &timeouts,
                    sealed_commit: &sealed_commit,
                    tree_hash: &tree_hash,
                    defined_by_commit: &defined_by_commit,
                    cell_env: &cell_env,
                    process_env: &process_env,
                    tool_use_id,
                    status_board: Some(status_board.clone()),
                },
                MutationPolicy::AllowDelta,
            )
            .await,
        )
    };
    let (mut batched_results, delta, request, store_dir) = match batch_outcome {
        Some(outcome) => (
            outcome.results,
            outcome.delta,
            outcome.request,
            outcome.store_dir,
        ),
        None => (HashMap::new(), None, None, None),
    };

    // A fix lands as its own commit on the branch, and every verdict this wave
    // records has to describe the tree that landed. The wave does not re-run to
    // get there. The declared fixers ran FIRST inside the shared slot, so the
    // checks after them already validated the fixed tree; re-keying carries
    // their verdicts onto it. Only a check the fix demonstrably invalidated runs
    // again, in one bounded verification batch that never folds.
    let mut keyed = keyed;
    let mut tree_hash = tree_hash;
    let mut fixed = None;
    let mut answered_elsewhere: Vec<ClaimedElsewhere> = Vec::new();
    if let Some(delta) = delta {
        let (Some(request), Some(store_dir)) = (request.as_ref(), store_dir.as_ref()) else {
            return Some(
                "Checks: \u{2717} write-check fold (the slot publication context was lost)".into(),
            );
        };
        let author = PublicationIdentity::new("Cairn checks", "checks@cairn.local");
        let branch = {
            let db =
                match crate::execution::routing::owning_db_for_job(&orch.db, &run_context.job_id)
                    .await
                {
                    Ok(db) => db,
                    Err(error) => {
                        return Some(format!(
                            "Checks: \u{2717} write-check fold (resolve owning database: {error})"
                        ))
                    }
                };
            let job_id = run_context.job_id.clone();
            match db
                .query_text("SELECT branch FROM jobs WHERE id = ?1", (job_id,))
                .await
            {
                Ok(Some(branch)) => branch,
                Ok(None) => {
                    return Some(
                        "Checks: \u{2717} write-check fold (logical branch is absent)".into(),
                    )
                }
                Err(error) => {
                    return Some(format!(
                        "Checks: \u{2717} write-check fold (resolve logical branch: {error})"
                    ))
                }
            }
        };
        let published = crate::mcp::handlers::run::publish_and_seal_slot_delta(
            orch,
            store_dir,
            request,
            &delta,
            &branch,
            "fix: apply write-check changes",
            Some(&author),
        )
        .await;
        let published = match published {
            Ok(Some(published)) => published,
            Ok(None) => {
                return Some(
                    "Checks: \u{22EF} superseded — the branch advanced before this wave's fix landed. \
                     The stale fix was discarded without changing the branch or recording verdicts. \
                     Read cairn:~/checks for the branch's current verdicts."
                        .into(),
                );
            }
            Err(error) => {
                let patch = delta_patch_excerpt(repo_root, &delta);
                return Some(format!(
                    "Checks: \u{2717} write-check fold ({error})\n```diff\n{patch}\n```"
                ));
            }
        };
        log::info!(
            "write checks published fix commit {} for {} path(s), patch_bytes={}",
            published.commit,
            published.paths.len(),
            published.patch.len()
        );
        let _ = orch.services.emitter.emit(
            "worktree-changed",
            serde_json::json!({"path": cwd, "source": "write-check-fix"}),
        );
        let _ = orch.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "check_result_cache", "action": "invalidate"}),
        );
        debug_assert!(published.landed_on_tested_base);
        let rekeyed = rekey_wave_onto_fix(
            orch,
            WriteCheckBatchRequest {
                run_context,
                repo_root,
                checks: &checks,
                extra_inputs: &extra_inputs,
                keyed: &keyed,
                order: &split.cell,
                timeouts: &timeouts,
                sealed_commit: &sealed_commit,
                tree_hash: &tree_hash,
                defined_by_commit: &defined_by_commit,
                cell_env: &cell_env,
                process_env: &process_env,
                tool_use_id,
                status_board: Some(status_board.clone()),
            },
            cwd,
            &published,
            &split.spilled,
            &mut claims,
        )
        .await;
        let rekeyed = match rekeyed {
            Ok(rekeyed) => rekeyed,
            Err(error) => {
                let patch = delta_patch_excerpt(repo_root, &delta);
                return Some(format!(
                    "Checks: \u{2717} write-check fold ({error})\n```diff\n{patch}\n```"
                ));
            }
        };
        // Verification that dirties the tree AGAIN is never folded. With the
        // recursion gone this is what bounds the fix loop: a wave publishes at
        // most one fix commit and reports the second mutation instead of chasing
        // it.
        if let Some(delta) = rekeyed.non_convergent {
            let patch = delta_patch_excerpt(repo_root, &delta);
            return Some(format!(
                "Checks: \u{2717} write-check batch (non-convergent: verification mutated again)\n```diff\n{patch}\n```"
            ));
        }
        keyed = rekeyed.keyed;
        tree_hash = rekeyed.tree_hash;
        // Every lane from here on (the spilled ones, an uncached retry) runs at
        // the fixed commit, so it carries that commit's change.
        process_env = rekeyed.process_env;
        batched_results.extend(rekeyed.results);
        answered_elsewhere.extend(rekeyed.answered_elsewhere);
        fixed = Some(published);
    }
    let observation_commit = fixed
        .as_ref()
        .map(|published| published.commit.as_str())
        .unwrap_or(sealed_commit.as_str());
    // Spilled lanes run only now, after the cell and any fold, at the commit
    // this wave lands and keyed on that tree. Running them beside the cell would
    // key a verdict to a tree a fix then replaced; ordering them after it makes
    // that impossible by construction rather than by timing.
    //
    // After a fold they are keyed to the fixed commit, which the planning-time
    // cache filter never saw, and on a moved head the head's own wave is
    // usually verifying them already: they get the fold's hand-off too.
    let spilled = if fixed.is_some() && !split.spilled.is_empty() {
        let (left, answered) = lanes_left_to_run(
            orch.db.local.clone(),
            &mut claims,
            &run_context.project_id,
            &keyed,
            &split.spilled,
            &process_env,
            FOLD_HANDOFF_WAIT,
        )
        .await;
        answered_elsewhere.extend(answered);
        claims.claim(
            left.iter()
                .map(|index| lane_key(&run_context.project_id, &keyed[*index])),
            observation_commit,
        );
        left
    } else {
        split.spilled.clone()
    };
    if !spilled.is_empty() {
        log::info!(
            "when:write checks: {} lane(s) declare platforms the write cell is not on; running them verdict-only at {observation_commit}: {:?}",
            spilled.len(),
            spilled
                .iter()
                .map(|index| keyed[*index].0.name.as_str())
                .collect::<Vec<_>>()
        );
        let spilled = submit_write_check_batch_for(
            orch,
            WriteCheckBatchRequest {
                run_context,
                repo_root,
                checks: &checks,
                extra_inputs: &extra_inputs,
                keyed: &keyed,
                order: &spilled,
                timeouts: &timeouts,
                sealed_commit: observation_commit,
                tree_hash: &tree_hash,
                defined_by_commit: &defined_by_commit,
                cell_env: &cell_env,
                process_env: &process_env,
                tool_use_id,
                status_board: Some(status_board.clone()),
            },
            MutationPolicy::PureVerdict,
        )
        .await;
        batched_results.extend(spilled.results);
    }
    // A fixer the write cell cannot run is a lane-configuration error: nothing
    // ran, and the sentence names the config line to change.
    for (index, refusal) in &split.refused {
        batched_results.insert(
            *index,
            Err(CheckExecutionFailure::NotPlaceable(refusal.clone())),
        );
    }
    let batched_results = Arc::new(std::sync::Mutex::new(batched_results));
    let retry_orch = orch.clone();
    let retry_context = run_context.clone();
    let retry_keyed = keyed.clone();
    let retry_checks = checks.clone();
    let retry_timeouts = timeouts.clone();
    let retry_commit = observation_commit.to_string();
    let retry_tree_hash = tree_hash.clone();
    let retry_defined_by = defined_by_commit.clone();
    let retry_cell_env = cell_env.clone();
    let retry_process_env = process_env.clone();
    let retry_repository = resolve_check_repository(
        orch,
        &run_context.project_id,
        &run_context.job_id,
        repo_root,
    )
    .await
    .ok();
    let sealed_coordinates = sealed_coordinates(
        CheckRunCommit {
            evaluated: observation_commit,
            defined_by: &defined_by_commit,
        },
        &tree_hash,
        &keyed,
    );
    let results = run_planned_checks_with_board(
        orch.db.local.clone(),
        &run_context.project_id,
        CheckRunCommit {
            evaluated: observation_commit,
            defined_by: &defined_by_commit,
        },
        &tree_hash,
        run_context.job_id.as_str(),
        &keyed,
        &sealed_coordinates,
        &head_infra_scopes(keyed.len(), run_context.job_id.as_str(), observation_commit),
        tool_use_id,
        CheckExecMode::Shared,
        Some(orch),
        Some(status_board),
        &process_env,
        false,
        move |index, command, stream_id| {
            let batched_results = batched_results.clone();
            let orch = retry_orch.clone();
            let context = retry_context.clone();
            let keyed = retry_keyed.clone();
            let checks = retry_checks.clone();
            let timeouts = retry_timeouts.clone();
            let sealed_commit = retry_commit.clone();
            let tree_hash = retry_tree_hash.clone();
            let defined_by_commit = retry_defined_by.clone();
            let cell_env = retry_cell_env.clone();
            let process_env = retry_process_env.clone();
            let repository = retry_repository.clone();
            async move {
                if !command.starts_with(UNCACHED_RETRY_PREFIX) {
                    return batched_results
                        .lock()
                        .unwrap()
                        .remove(&index)
                        .unwrap_or_else(|| {
                            Err(CheckExecutionFailure::substrate(
                                SubstrateFailureShape::Result,
                                format!("missing batched outcome for plan index {index}"),
                            ))
                        });
                }
                let Some((repository, store_dir)) = repository else {
                    return Err(CheckExecutionFailure::substrate(
                        SubstrateFailureShape::Dispatch,
                        "could not resolve repository for uncached check retry",
                    ));
                };
                let plan = &keyed[index].0;
                let configured = checks
                    .get(&plan.name)
                    .expect("planned check must retain its configured definition");
                submit_planned_check(
                    &orch,
                    PlannedCheckRequest {
                        project_id: context.project_id.clone(),
                        repository,
                        store_dir,
                        sealed_commit: sealed_commit.clone(),
                        requesting_job_id: context.job_id.clone(),
                        owner: cairn_common::executor_protocol::CellOwnerRef {
                            project_id: context.project_id.clone(),
                            project_key: Some(context.project_key.clone()),
                            issue_number: context.issue_number,
                            job_id: Some(context.job_id.clone()),
                            execution_seq: context.exec_seq,
                            node_kind: context.job_name.clone(),
                        },
                        affinity_key: Some(check_affinity_key(&context.job_id, &plan.name)),
                        priority: CellPriority::WriteCheck,
                        env: cell_env,
                        item: PlannedCheckBatchItem {
                            recording_plan: Some(CheckAdoptionRecordingPlan::new(
                                plan,
                                &crate::execution::cache::SealedCheckCoordinate::new(
                                    &sealed_commit,
                                    &defined_by_commit,
                                    &tree_hash,
                                    &keyed[index].1,
                                ),
                                &context.job_id,
                                &stream_id,
                                &process_env,
                            )),
                            index,
                            name: plan.name.clone(),
                            input_hash: keyed[index].1.clone(),
                            resource_identity: CheckResourceIdentityInput::Configured {
                                name: plan.name.clone(),
                                check: configured.clone(),
                            },
                            command,
                            stream_id,
                            env: process_env,
                            verdict_environment_names: plan.verdict_environment_names.clone(),
                            timeout_ms: timeouts[index].into(),
                            executor: configured.executor.clone(),
                            verdict_platforms: crate::execution::check_identity::verdict_platforms(
                                configured,
                            ),
                            // A write check runs in the job's own build slot,
                            // which is one environment reached by one machine:
                            // there is no fleet of candidates to bound it by.
                            infra_scopes: InfraScopes::head_only(&context.job_id, &sealed_commit),
                            avoid_executor_ids: Vec::new(),
                            resource_class: plan.resource_class,
                        },
                        run_context: Some(context),
                        status_board: None,
                        on_complete: None,
                        purpose: None,
                        publication: None,
                    },
                )
                .await
                .map_err(|error| {
                    CheckExecutionFailure::substrate(SubstrateFailureShape::Dispatch, error)
                })?
                .results
                .remove(&index)
                .ok_or_else(|| {
                    CheckExecutionFailure::substrate(
                        SubstrateFailureShape::Result,
                        format!("missing uncached retry outcome for plan index {index}"),
                    )
                })?
            }
        },
        move |_checks| {},
    )
    .await;

    // Nudge any open Checks settings view (and other `check_result_cache`
    // consumers) to re-read the freshly stored verdicts. The turn-end cadence
    // emits the same signal; the write cadence must too, or per-commit results
    // never surface live in the settings editor.
    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "check_result_cache", "action": "update"}),
    );

    if results.is_empty() {
        return None;
    }
    let attributions = write_check_attributions(orch, run_context.job_id.as_str(), &results).await;
    let summary = format!("Checks: {}", format_check_summary(&results, &attributions));
    let summary = match format_answered_elsewhere(&answered_elsewhere) {
        Some(line) => format!("{summary}\n{line}"),
        None => summary,
    };
    // The fix is attributed ONCE, at batch level: it is the wave's combined
    // delta, not any single check's, so no per-check annotation can honestly
    // claim it.
    Some(match fixed {
        Some(published) => {
            format_fixed_batch_summary(&summary, &published.commit, &published.paths)
        }
        None => summary,
    })
}

/// Everything one build-slot submission of write-cadence checks needs. The
/// fields are the wave's shared identity; `order` is the only per-submission
/// part, and it carries the plan indices to run in the order to run them.
pub(super) struct WriteCheckBatchRequest<'a> {
    pub(super) run_context: &'a RunContext,
    pub(super) repo_root: &'a Path,
    pub(super) checks: &'a HashMap<String, CheckCommand>,
    /// Node-level extra inputs, carried so the post-fix re-key can resolve every
    /// selector afresh against the tree the fix actually landed.
    pub(super) extra_inputs: &'a HashMap<String, Vec<String>>,
    pub(super) keyed: &'a [(CheckPlan, String)],
    pub(super) order: &'a [usize],
    pub(super) timeouts: &'a [u32],
    pub(super) sealed_commit: &'a str,
    pub(super) tree_hash: &'a str,
    pub(super) defined_by_commit: &'a str,
    /// The build slot's environment: the cell request's, without the transport.
    pub(super) cell_env: &'a [(String, String)],
    /// Each check process's environment: `cell_env` plus the change transport,
    /// built by [`write_check_transport_env`].
    pub(super) process_env: &'a [(String, String)],
    pub(super) tool_use_id: &'a str,
    pub(super) status_board: Option<CheckStatusBoard>,
}

/// How a write-cadence wave's cache misses split by where each may run.
///
/// The write cell is the job's colocated checkout, the one place a fix can fold
/// back into the commit, so its mobility is pinned. A lane whose
/// `verdictPlatforms` exclude the write cell's platform can never be answered
/// there, and a request naming a platform is answered only by a run on that
/// platform. Such a lane leaves the cell as a verdict-only lane placed on a
/// matching machine, unless it is a declared fixer, whose delta can fold only
/// from the write cell.
#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct WriteWaveSplit {
    /// Run in the write cell, which may fold a fix.
    pub(super) cell: Vec<usize>,
    /// Run verdict-only on a machine their own platforms admit, at the commit
    /// the wave lands, after the cell (and any fold) is done.
    pub(super) spilled: Vec<usize>,
    /// Declared fixers the write cell cannot run, with the sentence saying why.
    pub(super) refused: Vec<(usize, String)>,
}

pub(super) fn split_write_wave(
    keyed: &[(CheckPlan, String)],
    checks: &HashMap<String, CheckCommand>,
    misses: &[usize],
    write_cell_platform: &str,
    write_cell_machine: &str,
) -> WriteWaveSplit {
    let mut split = WriteWaveSplit::default();
    for &index in misses {
        let name = &keyed[index].0.name;
        let Some(check) = checks.get(name) else {
            split.cell.push(index);
            continue;
        };
        let platforms = crate::execution::check_identity::verdict_platforms(check);
        if platforms
            .iter()
            .any(|platform| platform.eq_ignore_ascii_case(write_cell_platform))
        {
            split.cell.push(index);
        } else if check.fixes {
            split.refused.push((
                index,
                format!(
                    "check `{name}` fixes files but its verdictPlatforms [{}] exclude the write cell on {write_cell_machine} ({write_cell_platform}); a fix folds only from the write cell. Widen verdictPlatforms or drop `fixes` in .cairn/config.yaml",
                    platforms.join(", ")
                ),
            ));
        } else {
            split.spilled.push(index);
        }
    }
    split
}

/// Submit one build-slot batch for the given cache-MISS plan indices, in the
/// given execution order. Every failure mode resolves to per-index failures, so
/// the caller always holds exactly one outcome per submitted index.
///
/// `mutation_policy` is `AllowDelta` for the write cell, whose fix folds, and
/// `PureVerdict` for the lanes a [`WriteWaveSplit`] spilled to other platforms,
/// which are verdicts and so are each submitted as a request of their own.
pub(super) async fn submit_write_check_batch_for(
    orch: &Orchestrator,
    batch: WriteCheckBatchRequest<'_>,
    mutation_policy: MutationPolicy,
) -> PlannedCheckBatchOutcome {
    let WriteCheckBatchRequest {
        run_context,
        repo_root,
        checks,
        extra_inputs: _,
        keyed,
        order,
        timeouts,
        sealed_commit,
        tree_hash,
        defined_by_commit,
        cell_env,
        process_env,
        tool_use_id,
        status_board,
    } = batch;
    let items: Vec<PlannedCheckBatchItem> = order
        .iter()
        .map(|index| PlannedCheckBatchItem {
            recording_plan: Some(CheckAdoptionRecordingPlan::new(
                &keyed[*index].0,
                &crate::execution::cache::SealedCheckCoordinate::new(
                    sealed_commit,
                    defined_by_commit,
                    tree_hash,
                    &keyed[*index].1,
                ),
                &run_context.job_id,
                tool_use_id,
                process_env,
            )),
            index: *index,
            name: keyed[*index].0.name.clone(),
            input_hash: keyed[*index].1.clone(),
            resource_identity: CheckResourceIdentityInput::Configured {
                name: keyed[*index].0.name.clone(),
                check: checks
                    .get(&keyed[*index].0.name)
                    .expect("planned check must retain its configured definition")
                    .clone(),
            },
            command: keyed[*index].0.command.clone(),
            stream_id: crate::mcp::handlers::run::check_stream_id(tool_use_id, *index),
            env: process_env.to_vec(),
            verdict_environment_names: keyed[*index].0.verdict_environment_names.clone(),
            timeout_ms: timeouts[*index].into(),
            executor: checks
                .get(&keyed[*index].0.name)
                .and_then(|check| check.executor.clone()),
            verdict_platforms: checks
                .get(&keyed[*index].0.name)
                .map(crate::execution::check_identity::verdict_platforms)
                .unwrap_or_default(),
            resource_class: keyed[*index].0.resource_class,
            // A write check runs in the job's own build slot, which is one
            // environment reached by one machine: there is no fleet of
            // candidates to bound it by.
            infra_scopes: InfraScopes::head_only(&run_context.job_id, sealed_commit),
            avoid_executor_ids: Vec::new(),
        })
        .collect();
    if let Some(board) = status_board.as_ref() {
        board.set_phase(
            Some("provisioning"),
            Some("preparing build slot"),
            Some("resolving build slot".into()),
        );
    }
    let repository = resolve_check_repository(
        orch,
        &run_context.project_id,
        &run_context.job_id,
        repo_root,
    )
    .await;
    let submitted = match repository {
        Ok((repository, store_dir)) => {
            let owner = cairn_common::executor_protocol::CellOwnerRef {
                project_id: run_context.project_id.clone(),
                project_key: Some(run_context.project_key.clone()),
                issue_number: run_context.issue_number,
                job_id: Some(run_context.job_id.clone()),
                execution_seq: run_context.exec_seq,
                node_kind: run_context.job_name.clone(),
            };
            if mutation_policy == MutationPolicy::PureVerdict {
                let requests = items.into_iter().map(|item| {
                    let index = item.index;
                    let request = PlannedCheckRequest {
                        project_id: run_context.project_id.clone(),
                        repository: repository.clone(),
                        store_dir: store_dir.clone(),
                        sealed_commit: sealed_commit.to_string(),
                        requesting_job_id: run_context.job_id.clone(),
                        owner: owner.clone(),
                        affinity_key: Some(check_affinity_key(&run_context.job_id, &item.name)),
                        priority: CellPriority::WriteCheck,
                        env: cell_env.to_vec(),
                        item,
                        run_context: Some(run_context.clone()),
                        status_board: status_board.as_ref().map(|board| board.for_lane(index)),
                        on_complete: None,
                        purpose: None,
                        publication: None,
                    };
                    Box::pin(async move { (index, submit_planned_check(orch, request).await) })
                });
                let mut combined = PlannedCheckBatchOutcome {
                    results: HashMap::new(),
                    request: None,
                    delta: None,
                    store_dir: Some(store_dir.clone()),
                };
                for (index, submitted) in futures_util::future::join_all(requests).await {
                    match submitted {
                        Ok(outcome) => combined.results.extend(outcome.results),
                        Err(error) => {
                            combined.results.insert(
                                index,
                                Err(CheckExecutionFailure::Substrate(SubstrateFailure::new(
                                    SubstrateFailureShape::Dispatch,
                                    error,
                                ))),
                            );
                        }
                    }
                }
                Ok(combined)
            } else {
                off_frame(|| {
                    submit_check_lease(
                        orch,
                        PlannedCheckBatchRequest {
                            project_id: run_context.project_id.clone(),
                            repository,
                            store_dir,
                            sealed_commit: sealed_commit.to_string(),
                            requesting_job_id: run_context.job_id.clone(),
                            owner,
                            affinity_key: Some(run_context.job_id.clone()),
                            priority: CellPriority::WriteCheck,
                            env: cell_env.to_vec(),
                            items,
                            run_context: Some(run_context.clone()),
                            mutation_policy,
                            status_board,
                            on_group_complete: None,
                            purpose: None,
                            publication: None,
                        },
                    )
                })
                .await
            }
        }
        Err(error) => Ok(PlannedCheckBatchOutcome::failed(
            order.to_vec(),
            SubstrateFailure::new(SubstrateFailureShape::Dispatch, error),
        )),
    };
    match submitted {
        Ok(outcome) => outcome,
        Err(error) => PlannedCheckBatchOutcome::failed(
            order.to_vec(),
            SubstrateFailure::new(SubstrateFailureShape::Dispatch, error),
        ),
    }
}

/// A write-check wave re-keyed onto the tree its fix actually landed.
pub(super) struct FixedWave {
    /// The wave's plans paired with their cache key on the POST-fix tree. A
    /// re-verified check also carries its re-planned command.
    pub(super) keyed: Vec<(CheckPlan, String)>,
    /// Whole-tree hash of the post-fix commit, re-stamped onto every row.
    pub(super) tree_hash: String,
    /// Outcomes from the re-verification batch, by plan index. They replace the
    /// pre-fix outcome for those checks; every other index keeps its original.
    pub(super) results: HashMap<usize, Result<CheckExecResult, CheckExecutionFailure>>,
    /// Set when re-verification dirtied the tree AGAIN. Never folded.
    pub(super) non_convergent: Option<crate::fleet::MutationDelta>,
    /// Each check process's environment at the fixed commit: the slot's, plus
    /// the transport of the fixed commit's own change.
    pub(super) process_env: Vec<(String, String)>,
    /// Invalidated lanes another wave verified at the same input key, whose
    /// verdicts the fold reuses instead of running them again.
    pub(super) answered_elsewhere: Vec<ClaimedElsewhere>,
}

/// How long a fold waits for another write wave to record a lane it is
/// verifying at the same input key. Longer than one lane's default budget plus
/// its queueing, since the claimant's whole wave has to finish; past it, the
/// fold runs the lane itself, which costs a duplicate and never a verdict.
pub(super) const FOLD_HANDOFF_WAIT: std::time::Duration =
    std::time::Duration::from_millis(2 * DEFAULT_WRITE_CHECK_TIMEOUT_MS as u64);

pub(super) fn lane_key(project_id: &str, (plan, input_hash): &(CheckPlan, String)) -> LaneKey {
    LaneKey {
        project_id: project_id.to_string(),
        check_name: plan.name.clone(),
        input_hash: input_hash.clone(),
    }
}

/// Of `candidates`, the lanes a fold must still run itself, and the lanes
/// another write wave answered.
///
/// A fold onto a head the branch moved to re-keys its lanes to that head, whose
/// own wave is usually verifying the same lanes at the same keys. So the fold
/// first waits for any other wave holding one of these keys to record, and only
/// then asks the result cache what is still a miss. The cache, not the claim,
/// decides: a claimant that recorded leaves a reusable verdict the fold's
/// recording reuses and names, and one that left none (its lane died, or its own
/// fold re-keyed the lane away) leaves the lane a miss the fold runs. Nothing is
/// verified twice, and nothing goes unverified.
pub(super) async fn lanes_left_to_run(
    db: Arc<LocalDb>,
    claims: &mut WaveClaims,
    project_id: &str,
    keyed: &[(CheckPlan, String)],
    candidates: &[usize],
    effective_env: &[(String, String)],
    bound: std::time::Duration,
) -> (Vec<usize>, Vec<ClaimedElsewhere>) {
    if candidates.is_empty() {
        return (Vec::new(), Vec::new());
    }
    let keys: Vec<LaneKey> = candidates
        .iter()
        .map(|index| lane_key(project_id, &keyed[*index]))
        .collect();
    let elsewhere = claims.await_other_waves(&keys, bound).await;
    let left: Vec<usize> = candidates
        .iter()
        .copied()
        .filter(|index| {
            needs_execution(
                db.clone(),
                project_id,
                &keyed[*index].0,
                &keyed[*index].1,
                effective_env,
            )
        })
        .collect();
    let answered: Vec<ClaimedElsewhere> = elsewhere
        .into_iter()
        .filter(|lane| {
            !left
                .iter()
                .any(|index| keyed[*index].0.name == lane.check_name)
        })
        .collect();
    if !answered.is_empty() {
        log::info!(
            "when:write checks: fold reuses {} lane(s) another wave verified at the same key: {:?}",
            answered.len(),
            answered
                .iter()
                .map(|lane| format!("{} @ {}", lane.check_name, lane.commit))
                .collect::<Vec<_>>()
        );
    }
    (left, answered)
}

/// The line a wave's summary carries for the lanes its fold reused from another
/// wave, naming that wave's commit, or `None` when it reused none.
pub(super) fn format_answered_elsewhere(answered: &[ClaimedElsewhere]) -> Option<String> {
    if answered.is_empty() {
        return None;
    }
    let mut by_commit: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for lane in answered {
        by_commit
            .entry(lane.commit.as_str())
            .or_default()
            .push(lane.check_name.as_str());
    }
    let named = by_commit
        .iter()
        .map(|(commit, names)| {
            format!(
                "{} from the wave at {}",
                names.join(", "),
                commit.get(..12).unwrap_or(commit)
            )
        })
        .collect::<Vec<_>>()
        .join("; ");
    Some(format!(
        "Reused, not run twice: {named}, which was already verifying the same inputs."
    ))
}

/// What re-planning a wave against the fixed commit yields: every plan paired
/// with its re-derived result key, the indices whose verdict the fix invalidated,
/// the fixed commit's tree hash, and its transport.
pub(super) type Replan = (
    Vec<(CheckPlan, String)>,
    Vec<usize>,
    String,
    Vec<(String, String)>,
);

/// Everything re-planning a wave onto the commit its fix landed reads.
pub(super) struct FixReplanning {
    pub(super) jj: JjEnv,
    pub(super) repository: PathBuf,
    pub(super) object_repository: PathBuf,
    pub(super) base: String,
    /// The commit the fix was published as.
    pub(super) head: String,
    pub(super) checks: HashMap<String, CheckCommand>,
    pub(super) extra_inputs: HashMap<String, Vec<String>>,
    pub(super) plans: Vec<CheckPlan>,
    pub(super) keys_before: Vec<String>,
    pub(super) executed: BTreeSet<usize>,
    pub(super) superseded: BTreeSet<usize>,
    pub(super) fixer_names: Vec<String>,
    pub(super) fixed_paths: Vec<String>,
    pub(super) fix_landed_on_tested_base: bool,
    pub(super) db: Arc<LocalDb>,
    pub(super) project_id: String,
}

/// Re-key every plan of a wave onto its fixed commit, name the ones the fix
/// invalidated, and build the transport that commit's lanes carry.
///
/// Synchronous: VCS inspection and the store wait on subprocesses and joined
/// threads, so the caller runs it on the blocking pool.
pub(super) fn replan_onto_fix(planning: FixReplanning) -> Result<Replan, String> {
    let FixReplanning {
        jj,
        repository,
        object_repository,
        base,
        head,
        checks,
        extra_inputs,
        plans,
        keys_before,
        executed,
        superseded,
        fixer_names,
        fixed_paths,
        fix_landed_on_tested_base,
        db,
        project_id,
    } = planning;
    let tree_hash = logical_tree_hash(&jj, &repository, &head)?;
    let entries = if any_check_declares_inputs(checks.values()) {
        tree_entries(&jj, &repository, &head).ok()
    } else {
        None
    };
    let blobs = TreeBlobs {
        jj: &jj,
        repository: &repository,
    };
    let snapshot = TreeSnapshot::new(entries.as_deref(), &blobs);
    let inputs = ResolvedInputs::resolve(&checks, &extra_inputs, &snapshot);
    // Attribution is a property of the whole wave: the fix is the slot's
    // combined delta, so the question is whether the declared fixers that ran
    // can account for every path in it. It is asked against the FIXED tree's
    // inputs, which is the tree the verdicts are being keyed to.
    let fixers: Vec<&InputSelector> = fixer_names
        .iter()
        .map(|name| inputs.for_check(name))
        .collect();
    let attributed = fix_is_attributed_to_declared_fixers(&fixed_paths, &fixers);
    // The fixed commit's own change. When the fix landed on a head that moved
    // while the wave ran, it differs from the change the wave was planned with,
    // and every key and lane from here on is about it.
    let changed = logical_changed_files(&jj, &repository, &base, &head)
        .map_err(|error| format!("the changed files of {base}..{head} are unreadable: {error}"))?;
    let planned: Vec<&str> = plans.iter().map(|plan| plan.name.as_str()).collect();
    let wave_change = WaveChange {
        jj: &jj,
        repository: &repository,
        object_repository: &object_repository,
        base: &base,
        head: &head,
        changed: &changed,
    };
    let base_findings = wave_base_findings(&wave_change, &planned, db, &project_id);
    let transport = write_wave_transport(&wave_change, &planned, base_findings.as_ref())?;
    let base_entries = key_base_entries(&checks, &jj, &repository, &base, &head, &changed);
    let basis = ChangeBasis::Against {
        changed: &changed,
        base_entries: base_entries.as_deref(),
        base_findings: base_findings.as_ref(),
    };
    let mut rekeyed = Vec::with_capacity(plans.len());
    let mut invalidated = Vec::new();
    for (index, plan) in plans.into_iter().enumerate() {
        let check = checks
            .get(&plan.name)
            .expect("planned check must retain its configured definition");
        let key_after = check_input_hash(
            check,
            inputs.for_check(&plan.name),
            entries.as_deref(),
            &tree_hash,
            CheckChange {
                name: &plan.name,
                basis,
            },
        );
        if verdict_survives_fix(
            fix_landed_on_tested_base,
            executed.contains(&index),
            attributed,
            superseded.contains(&index),
            &keys_before[index],
            &key_after,
            is_base_relative(&plan.name, check)
                || crate::execution::lint_baseline::subtracts_base_findings(&plan.name),
        ) {
            rekeyed.push((plan, key_after));
            continue;
        }
        // The fix changed this check's inputs and nothing proves it saw them.
        // Re-plan against the fixed commit so a `{changedFiles}` selector
        // covers the fixed paths too, then run it once.
        let replanned = replan_one_check(&plan.name, check, &inputs, &changed, &repository)
            .unwrap_or_else(|| plan.clone());
        invalidated.push(index);
        rekeyed.push((replanned, key_after));
    }
    Ok((rekeyed, invalidated, tree_hash, transport))
}

/// Re-key a wave onto the commit its fix landed, re-verifying exactly the checks
/// whose verdict the fix invalidated.
///
/// Survival is decided per check by [`verdict_survives_fix`]. In the ordinary
/// case — a declared formatter rewriting files inside its own `impact` — every
/// verdict survives and no second batch is submitted at all; the wave costs one
/// slot admission and one execution per check. A fix that no declared fixer
/// explains, or a check that was answered from the cache before the fix touched
/// its inputs, falls back to running that check once against the fixed commit.
///
/// Planning is bound to `fix.commit`, the commit the fix was published as,
/// never to whatever the branch head is by the time this runs: a detached wave
/// can outlive several later commits.
pub(super) async fn rekey_wave_onto_fix(
    orch: &Orchestrator,
    wave: WriteCheckBatchRequest<'_>,
    cwd: &str,
    fix: &crate::mcp::handlers::run::PublishedSlotDelta,
    deferred: &[usize],
    claims: &mut WaveClaims,
) -> Result<FixedWave, String> {
    let fixed_paths = fix.paths.as_slice();
    let fixed_commit = fix.commit.as_str();
    let fix_landed_on_tested_base = fix.landed_on_tested_base;
    let logical = crate::mcp::handlers::branch::resolve_current_for_read(
        orch,
        &crate::mcp::types::McpCallbackRequest {
            run_id: Some(wave.run_context.run_id.clone()),
            cwd: cwd.to_string(),
            ..Default::default()
        },
    )
    .await
    .map_err(|error| format!("resolve the fixed commit: {error}"))?;
    let live_base = live_node_base(orch, &wave.run_context.job_id).await;

    // Fixers run in plan order among themselves, so only the last one observed
    // the whole fold. Every earlier fixer whose key the fold moved is re-verified.
    let superseded = fixers_superseded_by_a_later_fixer(wave.keyed, wave.checks, wave.order);

    let executed: BTreeSet<usize> = wave.order.iter().copied().collect();
    let plans: Vec<CheckPlan> = wave.keyed.iter().map(|(plan, _)| plan.clone()).collect();
    let keys_before: Vec<String> = wave.keyed.iter().map(|(_, key)| key.clone()).collect();
    let checks = wave.checks.clone();
    let extra_inputs = wave.extra_inputs.clone();
    let fixer_names: Vec<String> = wave
        .order
        .iter()
        .map(|index| wave.keyed[*index].0.name.clone())
        .filter(|name| wave.checks.get(name).is_some_and(|check| check.fixes))
        .collect();
    let head = fixed_commit.to_string();
    let planning = FixReplanning {
        jj: crate::jj::environment(&orch.jj_binary_path, &orch.config_dir),
        repository: logical.repository_path.clone(),
        object_repository: logical.object_repository_path.clone(),
        base: live_base.unwrap_or(logical.default_commit_id),
        head: head.clone(),
        checks,
        extra_inputs,
        plans,
        keys_before,
        executed,
        superseded,
        fixer_names,
        fixed_paths: fixed_paths.to_vec(),
        fix_landed_on_tested_base,
        db: orch.db.local.clone(),
        project_id: wave.run_context.project_id.clone(),
    };
    let (rekeyed, invalidated, tree_hash, transport) =
        tokio::task::spawn_blocking(move || replan_onto_fix(planning))
            .await
            .map_err(|error| format!("join the post-fix planning unit: {error}"))??;
    let process_env = write_check_transport_env(wave.cell_env, transport);

    let project_id = wave.run_context.project_id.as_str();
    // The keys these lanes were planned under will never be recorded now, so no
    // other fold should wait on them.
    claims.release(
        invalidated
            .iter()
            .map(|index| lane_key(project_id, &wave.keyed[*index])),
    );
    // An invalidated verdict may still be answerable from the cache on the FIXED
    // tree, including from another wave verifying the same key right now (the
    // moved head's own wave, usually); only a genuine miss needs the slot. A
    // deferred lane is never re-verified here: it has not run yet, it runs at
    // the fixed commit once this returns, and the write cell is the one place it
    // cannot run.
    let candidates: Vec<usize> = invalidated
        .into_iter()
        .filter(|index| !deferred.contains(index))
        .collect();
    let (misses, answered_elsewhere) = lanes_left_to_run(
        orch.db.local.clone(),
        claims,
        project_id,
        &rekeyed,
        &candidates,
        &process_env,
        FOLD_HANDOFF_WAIT,
    )
    .await;
    claims.claim(
        misses
            .iter()
            .map(|index| lane_key(project_id, &rekeyed[*index])),
        &head,
    );
    if misses.is_empty() {
        return Ok(FixedWave {
            keyed: rekeyed,
            tree_hash,
            results: HashMap::new(),
            non_convergent: None,
            process_env,
            answered_elsewhere,
        });
    }
    log::info!(
        "when:write checks: fix invalidated {} verdict(s), re-verifying {:?} against {head}",
        misses.len(),
        misses
            .iter()
            .map(|index| rekeyed[*index].0.name.as_str())
            .collect::<Vec<_>>()
    );
    let outcome = submit_write_check_batch_for(
        orch,
        WriteCheckBatchRequest {
            keyed: &rekeyed,
            order: &fixer_first_submission_order(&rekeyed, wave.checks, &misses),
            sealed_commit: &head,
            tree_hash: &tree_hash,
            process_env: &process_env,
            ..wave
        },
        MutationPolicy::AllowDelta,
    )
    .await;
    Ok(FixedWave {
        keyed: rekeyed,
        tree_hash,
        results: outcome.results,
        non_convergent: outcome.delta,
        process_env,
        answered_elsewhere,
    })
}
