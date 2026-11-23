mod durable;

pub(super) async fn rearm_at_boot(orch: &Orchestrator) {
    durable::rearm(orch).await;
}

use super::*;

#[cfg(test)]
static CRASH_AFTER_FOLD: std::sync::LazyLock<std::sync::Mutex<std::collections::HashSet<String>>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(std::collections::HashSet::new()));

#[cfg(test)]
pub(super) fn crash_after_fold_for_test(id: &str) {
    CRASH_AFTER_FOLD.lock().unwrap().insert(id.to_string());
}

#[cfg(test)]
static CRASH_AFTER_RECORD: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashSet<String>>,
> = std::sync::LazyLock::new(|| std::sync::Mutex::new(std::collections::HashSet::new()));

#[cfg(test)]
pub(super) fn crash_after_record_for_test(id: &str) {
    CRASH_AFTER_RECORD.lock().unwrap().insert(id.to_string());
}

#[cfg(test)]
static CRASH_AFTER_RAW: std::sync::LazyLock<std::sync::Mutex<std::collections::HashSet<String>>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(std::collections::HashSet::new()));

#[cfg(test)]
pub(super) fn crash_after_raw_for_test(id: &str) {
    CRASH_AFTER_RAW.lock().unwrap().insert(id.to_string());
}

/// A wave's summary with its folds attributed once, at wave level: each
/// fixer's fold is its own commit, and the files are every fold's together.
pub(super) fn format_fixed_batch_summary(
    summary: &str,
    commits: &[String],
    paths: &[String],
) -> String {
    let short = commits
        .iter()
        .map(|commit| commit.get(..12).unwrap_or(commit))
        .collect::<Vec<_>>()
        .join(", ");
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

/// Whether a fold is fully explained by the declared fixer that produced it.
///
/// A fold carries its fixer's verdict onto the folded tree only because that
/// fixer produced the tree. A folded path outside every given fixer's INPUT
/// SET means the fixer rewrote something it never declared, so its verdict
/// cannot be keyed to the landed tree and must be re-verified. Uncertainty (an
/// uncompilable glob, or no fixer at all) resolves to NOT attributed: that
/// costs one extra verification run, never a verdict recorded against a tree
/// it never saw.
///
/// The fold is the only mutation evidence a fixer's request produces, so
/// attribution rests on the same declaration the result cache already rests
/// on: the declared inputs and `fixes` are the trust boundary, as everywhere
/// else. A check that does not declare `fixes` is a pure verdict: whatever it
/// writes is discarded and reported (`tracked_modifications`), never folded.
pub(super) fn fix_is_attributed_to_declared_fixers(
    paths: &[String],
    fixers: &[&InputSelector],
) -> bool {
    !fixers.is_empty()
        && paths
            .iter()
            .all(|path| fixers.iter().any(|fixer| fixer.matches(path)))
}

/// Whether a lane's verdict still describes the tree one fixer's fold landed.
///
/// Two independent proofs, either of which suffices:
///
/// - the fold left the lane's impact-filtered inputs untouched, so its cache
///   key is unchanged — the same argument the result cache itself rests on; or
/// - the lane is the fixer whose output this fold is (`produced_fold`), and the
///   fold is attributed to it. Producing this tree is what it just did, and
///   re-checking it against that is precisely the redundant second run this
///   rule exists to delete.
///
/// Every other verdict in hand, an earlier fixer's included, was about the
/// tree before this fold, so it carries only on an unchanged key. That is why
/// an earlier fixer never carries a later one's rewrite: Prettier passes, then
/// `eslint --fix` rewrites the same file and folds, and Prettier's key moved.
///
/// `fix_landed_on_tested_base` is false when the branch advanced while the
/// fixer ran and the fold merged onto the newer head. The folded commit then
/// carries code the fixer never executed, so only an unchanged key carries.
///
/// The fixer proof covers the TREE only. A base-relative check was handed the
/// change of the commit it was planned at, so it carries across a fold only
/// when its key, which names that change, did not move. A base-subtracting
/// check is the same: it was handed the base finding set its key names.
pub(super) fn verdict_survives_fix(
    fix_landed_on_tested_base: bool,
    produced_fold: bool,
    fix_attributed: bool,
    key_before: &str,
    key_after: &str,
    base_relative: bool,
) -> bool {
    key_before == key_after
        || (!base_relative && fix_landed_on_tested_base && fix_attributed && produced_fold)
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
#[derive(Clone, serde::Serialize, serde::Deserialize)]
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
    run_write_checks_after_seal_for_suite(orch, run_context, cwd, tool_use_id, target, None).await
}

async fn run_write_checks_after_seal_for_suite(
    orch: &Orchestrator,
    run_context: Option<&RunContext>,
    cwd: &str,
    tool_use_id: &str,
    target: WriteCheckTarget<'_>,
    only_suite: Option<String>,
) -> Option<String> {
    // No run context ⇒ no run id to stream against and no job to anchor the diff.
    let run_context = run_context?.clone();
    let guard = WriteChecksInFlightGuard::new(orch, &run_context.job_id);
    // Opened before the wave is spawned, so a fold that finds the branch moved
    // to this commit sees the wave planning from the moment the commit landed.
    let claims = orch.lane_claims.open_wave(&run_context.job_id);
    let ticket = Arc::new(
        orch.late_write_checks
            .open(&run_context.job_id, tool_use_id),
    );
    let inline_ticket = ticket.clone();
    let inline_orch = orch.clone();
    let wave_ticket = ticket.clone();
    let late_orch = orch.clone();
    let late_run_id = run_context.run_id.clone();
    let wave_orch = orch.clone();
    let cwd = cwd.to_string();
    let sealed_resolution = target.sealed_resolution();
    let (progress, progress_seen) = tokio::sync::watch::channel(WaveProgress::default());
    let answer = await_write_check_wave(
        guard,
        WRITE_CHECK_INLINE_WAIT_BOUNDS,
        progress_seen,
        async move {
            run_write_checks_after_seal_filtered(
                &wave_orch,
                Some(&run_context),
                &cwd,
                sealed_resolution.as_ref(),
                progress,
                claims,
                &wave_ticket,
                only_suite.as_deref(),
            )
            .await
        },
        move |summary| deliver_late_write_check_summary(late_orch, late_run_id, ticket, summary),
    )
    .await;
    if let Some(summary) = &answer {
        if let Ok(db) =
            crate::execution::routing::owning_db_for_job(&inline_orch.db, inline_ticket.job_id())
                .await
        {
            // A pending response cannot acknowledge a future summary.
            let id =
                durable::resolve_key(&db, inline_ticket.job_id(), inline_ticket.wave_id()).await;
            let acknowledged = db.execute("UPDATE write_check_waves SET delivered=1 WHERE id=?1 AND completed=1 AND summary=?2",
                cairn_db::turso::params![id.as_str(), summary.as_str()]).await;
            if matches!(acknowledged, Ok(1)) {
                durable::retry("inline summary receipt", || durable::delivered(&db, &id)).await;
            }
        }
    }
    answer
}

/// Upgrade continuation for a pre-durable fixer whose phase was unknowable.
/// The imported lane records its old evidence but never publishes its delta;
/// this ordinary current-head wave owns any new execution and mutation.
pub(super) fn resume_legacy_write<'a>(
    orch: &'a Orchestrator,
    job_id: &'a str,
    tool_use_id: &'a str,
    suite: &'a str,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<String, String>> + Send + 'a>> {
    Box::pin(async move {
        let db = crate::execution::routing::owning_db_for_job(&orch.db, job_id)
            .await
            .map_err(|error| error.to_string())?;
        let existing = db
            .query_text(
                "SELECT id FROM write_check_waves WHERE job_id=?1 AND tool_use_id=?2",
                (job_id.to_string(), tool_use_id.to_string()),
            )
            .await
            .map_err(|e| e.to_string())?;
        if let Some(id) = existing {
            return Ok(id);
        }
        let active = db.query_opt(
        "SELECT j.status,j.branch,p.repo_path,i.status FROM jobs j JOIN projects p ON p.id=j.project_id LEFT JOIN issues i ON i.id=j.issue_id WHERE j.id=?1",
        (job_id.to_string(),), |row| Ok((row.get::<String>(0)?,row.get::<Option<String>>(1)?,
            row.get::<String>(2)?,row.get::<Option<String>>(3)?))).await.map_err(|e|e.to_string())?
        .ok_or("superseded: legacy write job is gone")?;
        if matches!(active.0.as_str(), "complete" | "failed" | "cancelled")
            || active.1.as_deref().is_none_or(str::is_empty)
            || active
                .3
                .as_deref()
                .is_some_and(|status| matches!(status, "complete" | "failed" | "merged" | "closed"))
        {
            return Err("superseded: legacy write job, issue or branch has resolved".into());
        }
        if let Some(reason) = crate::execution::canonical::fence_for_job(&db, job_id)
            .await
            .map_err(|e| e.to_string())?
            .refusal()
        {
            return Err(format!("superseded: {reason}"));
        }
        let run_id = db
            .query_text(
                "SELECT id FROM runs WHERE job_id=?1 ORDER BY created_at DESC,id DESC LIMIT 1",
                (job_id.to_string(),),
            )
            .await
            .map_err(|e| e.to_string())?
            .ok_or("superseded: legacy write job has no surviving run")?;
        let run = crate::mcp::handlers::run_context::lookup_run_by_id(&db, &run_id).await?;
        let answer = run_write_checks_after_seal_for_suite(
            orch,
            Some(&run),
            &active.2,
            tool_use_id,
            WriteCheckTarget::CurrentHead,
            Some(suite.to_string()),
        )
        .await;
        let id = db
            .query_text(
                "SELECT id FROM write_check_waves WHERE job_id=?1 AND tool_use_id=?2",
                (job_id.to_string(), tool_use_id.to_string()),
            )
            .await
            .map_err(|e| e.to_string())?;
        match id {
        Some(id) => Ok(id),
        None if answer.is_none() && !durable::DriverGuard::active(&db,&durable::key(job_id,tool_use_id)) =>
            Err("superseded: the current sealed head no longer selects the imported write suite or its branch is gone".into()),
        None => Err("legacy write successor is still planning; its durable trigger retains custody".into()),
    }
    })
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
    cairn_common::task::spawn(async move {
        let outcome = cairn_common::task::spawn(async move {
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
///
/// A wave a newer wave of the same job has superseded sends nothing; the newer
/// wave's summary names it instead ([`crate::execution::late_write_checks`]).
/// Each wave is delivered at most once, keyed by its recipient and the call
/// that started it, so a repeated hand-off cannot write a second copy.
pub(super) async fn deliver_late_write_check_summary(
    orch: Orchestrator,
    run_id: String,
    ticket: Arc<crate::execution::late_write_checks::WaveTicket>,
    summary: String,
) {
    use crate::execution::late_write_checks::LateDisposition;
    use crate::messages::queued::DeliveryUrgency;
    let job_id = ticket.job_id().to_string();
    let owning_db = crate::execution::routing::owning_db_for_job(&orch.db, &job_id)
        .await
        .ok();
    let durable_id = match &owning_db {
        Some(db) => durable::resolve_key(db, &job_id, ticket.wave_id()).await,
        None => durable::key(&job_id, ticket.wave_id()),
    };
    let superseded = match ticket.settle_late() {
        LateDisposition::Superseded => {
            log::info!(
                "late write-check summary for job {job_id}, wave {}, commit {}: a newer wave superseded it; folded into that wave's summary",
                ticket.wave_id(),
                ticket.commit().unwrap_or("unplanned"),
            );
            if let Some(db) = &owning_db {
                durable::retry("superseded summary custody", || {
                    durable::superseded(db, &durable_id)
                })
                .await;
            }
            return;
        }
        LateDisposition::Deliver(superseded) => superseded,
    };
    let folded_failure = superseded
        .as_ref()
        .is_some_and(|superseded| !superseded.failures.is_empty());
    let mut urgency = if summary.contains('\u{2717}') || folded_failure {
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
    let content = late_write_check_message(
        &LateWave {
            commit: ticket.commit(),
            wave_id: ticket.wave_id(),
            superseded: superseded.as_ref(),
        },
        &summary,
    );
    let key = late_write_check_key(&job_id, ticket.wave_id());
    let delivered = tokio::task::spawn_blocking(move || {
        crate::messages::delivery::queue_system_direct_once(&orch, &run_id, &content, urgency, &key)
    })
    .await;
    match delivered {
        Ok(Ok(())) => {
            if let Some(db) = &owning_db {
                if let Err(error) = durable::delivered(db, &durable_id).await {
                    log::error!("write-wave {durable_id} delivery checkpoint failed: {error}");
                }
            }
        }
        Ok(Err(error)) => {
            log::warn!("late write-check summary for job {job_id} undeliverable: {error}")
        }
        Err(error) => {
            log::warn!("late write-check summary for job {job_id} delivery task failed: {error}")
        }
    }
}

/// The wave a late summary reports on.
pub(super) struct LateWave<'a> {
    /// The commit it evaluated, or `None` when it ended before planning.
    pub(super) commit: Option<&'a str>,
    /// The committing call that started it.
    pub(super) wave_id: &'a str,
    /// The earlier waves this summary stands in for.
    pub(super) superseded: Option<&'a crate::execution::late_write_checks::SupersededWaves>,
}

/// The one delivery a wave's late summary may make. The recipient is part of
/// the key, so no other job's delivery can ever answer for it.
pub(super) fn late_write_check_key(job_id: &str, wave_id: &str) -> String {
    format!("write-checks:{job_id}:{wave_id}")
}

pub(super) fn late_write_check_message(wave: &LateWave<'_>, summary: &str) -> String {
    let subject = match wave.commit {
        Some(commit) => format!(
            "commit {}",
            crate::execution::late_write_checks::short_commit(commit)
        ),
        None => "an earlier commit".to_string(),
    };
    let folded = wave
        .superseded
        .map(|superseded| {
            let unnamed = superseded.total - superseded.commits.len();
            let mut commits = superseded.commits.join(", ");
            if unnamed > 0 {
                if !commits.is_empty() {
                    commits.push_str(", ");
                }
                commits.push_str(&format!("{unnamed} more"));
            }
            let waves = if superseded.total == 1 {
                "1 earlier pending wave".to_string()
            } else {
                format!("{} earlier pending waves", superseded.total)
            };
            let failures = if superseded.failures.is_empty() {
                String::new()
            } else {
                let named = superseded
                    .failures
                    .iter()
                    .map(|failure| {
                        format!(
                            "{}: {} failed",
                            failure.commit.as_deref().unwrap_or("unplanned"),
                            failure.lane
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("; ");
                format!(
                    " No newer wave has recorded a verdict on these failures from them: {named}."
                )
            };
            format!(
                "\n\nThis summary supersedes {waves} (commits {commits}); their own summaries are not sent.{failures}"
            )
        })
        .unwrap_or_default();
    format!(
        "[Write checks] The when:write checks for {subject} (wave {}) have finished; that commit's \
         call had returned pending.\n\n{summary}{folded}\n\nCurrent verdicts for the branch: cairn:~/checks",
        wave.wave_id
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

#[cfg(test)]
pub(super) async fn run_write_checks_after_seal_inner(
    orch: &Orchestrator,
    run_context: Option<&RunContext>,
    cwd: &str,
    sealed_resolution: Option<&crate::mcp::handlers::branch::BranchResolution>,
    progress: tokio::sync::watch::Sender<WaveProgress>,
    // Held until this function returns, which is after every verdict the wave
    // records has been recorded.
    claims: WaveClaims,
    // Its wave id is the committing call's tool_use_id.
    ticket: &crate::execution::late_write_checks::WaveTicket,
) -> Option<String> {
    run_write_checks_after_seal_filtered(
        orch,
        run_context,
        cwd,
        sealed_resolution,
        progress,
        claims,
        ticket,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn run_write_checks_after_seal_filtered(
    orch: &Orchestrator,
    run_context: Option<&RunContext>,
    cwd: &str,
    sealed_resolution: Option<&crate::mcp::handlers::branch::BranchResolution>,
    progress: tokio::sync::watch::Sender<WaveProgress>,
    // Held until this function returns, which is after every verdict the wave
    // records has been recorded.
    claims: WaveClaims,
    // Its wave id is the committing call's tool_use_id.
    ticket: &crate::execution::late_write_checks::WaveTicket,
    only_suite: Option<&str>,
) -> Option<String> {
    let run = run_context?;
    let db = crate::execution::routing::owning_db_for_job(&orch.db, &run.job_id)
        .await
        .ok()?;
    let id = durable::resolve_key(&db, &run.job_id, ticket.wave_id()).await;
    let _driver = durable::DriverGuard::acquire(&db, &id)?;
    run_owned_write_wave(
        orch,
        run_context,
        cwd,
        sealed_resolution,
        progress,
        claims,
        ticket,
        only_suite,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn run_owned_write_wave(
    orch: &Orchestrator,
    run_context: Option<&RunContext>,
    cwd: &str,
    sealed_resolution: Option<&crate::mcp::handlers::branch::BranchResolution>,
    progress: tokio::sync::watch::Sender<WaveProgress>,
    // Held until this function returns, which is after every verdict the wave
    // records has been recorded.
    claims: WaveClaims,
    // Its wave id is the committing call's tool_use_id.
    ticket: &crate::execution::late_write_checks::WaveTicket,
    only_suite: Option<&str>,
) -> Option<String> {
    // Keep the phase driver's large state out of every supervising future.
    let summary = Box::pin(drive_write_wave(
        orch,
        run_context,
        cwd,
        sealed_resolution,
        progress,
        claims,
        ticket,
        only_suite,
    ))
    .await;
    if let Some(run) = run_context {
        if let Ok(db) = crate::execution::routing::owning_db_for_job(&orch.db, &run.job_id).await {
            let id = durable::resolve_key(&db, &run.job_id, ticket.wave_id()).await;
            if let Ok(Some(snapshot)) = durable::load(&db, &id).await {
                for index in snapshot.split.fixers {
                    for phase in ["fixer", "convergence"] {
                        let key = format!("{id}:{index}:{phase}");
                        let reason = summary.as_deref().unwrap_or(
                            "the write wave has completed; this raw phase has no final-tree observation obligation");
                        durable::retry("terminal fixer custody", || {
                            requests::supersede_planned_check_request(
                                orch,
                                &run.project_id,
                                &key,
                                reason,
                            )
                        })
                        .await;
                    }
                }
            }
            loop {
                match durable::complete(&db, &id, summary.as_deref()).await {
                    Ok(()) => break,
                    Err(error) => {
                        log::error!("write-wave {id} retains summary pending checkpoint: {error}");
                        tokio::time::sleep(crate::fleet::RETAINED_RESULT_RETRY).await;
                    }
                }
            }
            if summary.is_none() {
                let _ = durable::delivered(&db, &id).await;
            }
        }
    }
    summary
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn drive_write_wave(
    orch: &Orchestrator,
    run_context: Option<&RunContext>,
    cwd: &str,
    sealed_resolution: Option<&crate::mcp::handlers::branch::BranchResolution>,
    progress: tokio::sync::watch::Sender<WaveProgress>,
    // Held until this function returns, which is after every verdict the wave
    // records has been recorded.
    mut claims: WaveClaims,
    // Its wave id is the committing call's tool_use_id.
    ticket: &crate::execution::late_write_checks::WaveTicket,
    only_suite: Option<&str>,
) -> Option<String> {
    let tool_use_id = ticket.wave_id();
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
    let wave_id = durable::resolve_key(&owning_db, &run_context.job_id, tool_use_id).await;
    let recovered = durable::retry("snapshot", || durable::load(&owning_db, &wave_id)).await;
    let live_base = match &recovered {
        Some(snapshot) => Some(snapshot.base.clone()),
        None => live_node_base(orch, &run_context.job_id).await,
    };
    let logical = match recovered
        .as_ref()
        .map(|snapshot| &snapshot.logical)
        .or(sealed_resolution)
    {
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

    // Kept for re-keying onto each fixer's fold: the fold lands in the same
    // repository, and its change is read against the same base.
    let repository_path = logical.repository_path.clone();
    let planning_base = live_base.unwrap_or_else(|| logical.default_commit_id.clone());
    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
    let planning = WriteWavePlanning {
        jj: jj.clone(),
        repository: repository_path.clone(),
        object_repository: object_repository.clone(),
        head: logical.commit_id.clone(),
        base: planning_base.clone(),
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
    let planned = match &recovered {
        Some(snapshot) => Ok(snapshot.planned.clone()),
        None => tokio::task::spawn_blocking(move || plan_write_wave(planning))
            .await
            .ok()??,
    };
    let PlannedWriteWave {
        mut keyed,
        tree_hash,
        sealed_commit,
        transport,
    } = match planned {
        Ok(planned) => planned,
        Err(error) => {
            return Some(write_check_planning_infrastructure_failure(&error));
        }
    };
    let only_suite = recovered
        .as_ref()
        .and_then(|snapshot| snapshot.only_suite.as_deref())
        .or(only_suite);
    if let Some(suite) = only_suite {
        keyed.retain(|(plan, _)| plan.name == suite);
        if keyed.is_empty() {
            return None;
        }
    }
    ticket.planned(&sealed_commit);

    // The planning unit above is synchronous and sits on the critical path of
    // every source-touching commit, between the sealed commit and the agent
    // getting its tool result. Its elapsed time is logged because that is exactly
    // where CAIRN-3108 hid: a quadratic cache read inside it pinned a
    // blocking-pool thread for ~59 SECONDS per commit while every subsystem that
    // does log stayed silent, so the stall was invisible in the logs for days.
    // Expect tens of milliseconds; seconds here means something in planning has
    // started scaling with history again.
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
    let saved_transport = transport.clone();
    let mut process_env = write_check_transport_env(&cell_env, transport);
    // Reuse follows the suite's declared trust set, not the environment of the
    // executor that a miss may eventually select. Filter hits before placement so
    // a stored verdict consumes no build-slot capacity.
    let miss_indices: Vec<usize> = recovered
        .as_ref()
        .map(|snapshot| snapshot.misses.clone())
        .unwrap_or_else(|| {
            keyed
                .iter()
                .enumerate()
                .filter_map(|(index, (plan, input_hash))| {
                    (only_suite.is_some()
                        || needs_execution(
                            orch.db.local.clone(),
                            &run_context.project_id,
                            plan,
                            input_hash,
                            &process_env,
                        ))
                    .then_some(index)
                })
                .collect()
        });
    if recovered.is_none() {
        let snapshot = durable::Snapshot {
            run: run_context.clone(),
            cwd: cwd.to_string(),
            logical: logical.clone(),
            base: planning_base.clone(),
            planned: PlannedWriteWave {
                keyed: keyed.clone(),
                tree_hash: tree_hash.clone(),
                sealed_commit: sealed_commit.clone(),
                transport: saved_transport,
            },
            misses: miss_indices.clone(),
            only_suite: only_suite.map(str::to_string),
            opened_at_ms: ticket.opened_at_ms(),
            split: split_write_wave(
                &keyed,
                &checks,
                &miss_indices,
                crate::execution::check_identity::gating_platform(),
                &orch
                    .fleet
                    .executor_name_for_text(crate::fleet::COLOCATED_EXECUTOR_ID),
            ),
        };
        durable::retry("snapshot", || {
            durable::save(&owning_db, &wave_id, &snapshot)
        })
        .await;
    }
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
    let status_board = CheckStatusBoard::for_write_wave(&keyed, status_notify);
    // `/checks` reads each write lane from its own request while the wave runs.
    let _published_board = orch
        .write_wave_boards
        .publish(&run_context.job_id, &status_board);
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

    let split = recovered
        .as_ref()
        .map(|snapshot| snapshot.split.clone())
        .unwrap_or_else(|| {
            split_write_wave(
                &keyed,
                &checks,
                &miss_indices,
                crate::execution::check_identity::gating_platform(),
                &orch
                    .fleet
                    .executor_name_for_text(crate::fleet::COLOCATED_EXECUTOR_ID),
            )
        });
    // What each lane already has, by plan index, before the verdicts run: a
    // fixer's own run, and a fixer the write cell cannot run, which is a
    // lane-configuration error whose sentence names the config line to change.
    let mut ran: HashMap<usize, Result<CheckExecResult, CheckExecutionFailure>> = split
        .refused
        .iter()
        .map(|(index, refusal)| {
            (
                *index,
                Err(CheckExecutionFailure::NotPlaceable(refusal.clone())),
            )
        })
        .collect();
    let requests = match resolve_check_repository(
        orch,
        &run_context.project_id,
        &run_context.job_id,
        repo_root,
    )
    .await
    {
        Ok((repository, store_dir)) => Some(Arc::new(WriteWaveRequests {
            run_context: run_context.clone(),
            repository,
            store_dir,
            checks: checks.clone(),
            timeouts: timeouts.clone(),
            defined_by_commit: defined_by_commit.clone(),
            cell_env: cell_env.clone(),
            tool_use_id: tool_use_id.to_string(),
            board: status_board.clone(),
        })),
        Err(error) => {
            for index in split.fixers.iter().chain(&split.verifiers) {
                ran.insert(
                    *index,
                    Err(CheckExecutionFailure::substrate(
                        SubstrateFailureShape::Dispatch,
                        error.clone(),
                    )),
                );
            }
            None
        }
    };

    // 1. The declared fixers, chained. Each is its own change-returning
    // request on the commit the previous fixer's fold landed, so it sees every
    // rewrite before it, and its own rewrite lands as a commit of its own
    // before the next fixer runs. The verdicts wait for the chain, so every one
    // of them is about the tree that lands, and each check runs once.
    let mut tree_hash = tree_hash;
    let mut commit = sealed_commit.clone();
    let mut folds: Vec<crate::mcp::handlers::run::PublishedSlotDelta> = Vec::new();
    // Lanes whose answer in hand describes a tree a later fold replaced.
    let mut stale: BTreeSet<usize> = BTreeSet::new();
    let mut answered_elsewhere: Vec<ClaimedElsewhere> = Vec::new();
    if let Some(requests) = requests.as_ref() {
        for (position, &index) in split.fixers.iter().enumerate() {
            let fixer = keyed[index].0.name.clone();
            status_board.set_phase(
                Some("dispatching"),
                Some("running fixers"),
                Some(format!(
                    "waiting for {fixer} to fold its fix before the verdicts run"
                )),
            );
            let request = requests.request(
                index,
                &LaneAt {
                    commit: &commit,
                    tree_hash: &tree_hash,
                    keyed: &keyed,
                    process_env: &process_env,
                },
                keyed[index].0.command.clone(),
                crate::mcp::handlers::run::check_stream_id(tool_use_id, index),
            );
            let phase_key = format!("{wave_id}:{index}:fixer");
            let FixerOutcome {
                result,
                delta,
                cell,
            } = fixer_outcome(
                index,
                requests::file_planned_check_request(
                    orch,
                    &phase_key,
                    request,
                    requests::WriteLanePhase::Fixer,
                )
                .await,
            );
            ran.insert(index, result);
            let Some(delta) = delta else {
                continue;
            };
            #[cfg(test)]
            if CRASH_AFTER_RAW.lock().unwrap().remove(&wave_id) {
                panic!("simulated runner loss after durable raw delta, before publication");
            }
            let Some((request, store_dir)) = cell else {
                return Some(
                    "Checks: \u{2717} write-check fold (the slot publication context was lost)"
                        .into(),
                );
            };
            let prior = durable::retry("fold checkpoint", || {
                requests::load_planned_fold(orch, &run_context.project_id, &phase_key)
            })
            .await;
            let published = match prior {
                Some(outcome) => outcome,
                None => {
                    let (_, since) = durable::retry("filing timestamp", || {
                        requests::planned_filing(orch, &run_context.project_id, &phase_key)
                    })
                    .await;
                    let outcome = durable::retry("keyed fold publication", || async {
                        match fold_fixer_delta(
                            orch,
                            FixerFold {
                                run_context,
                                repo_root,
                                cwd,
                                request: &request,
                                store_dir: &store_dir,
                                delta: &delta,
                                fixer: &fixer,
                            },
                            since,
                        )
                        .await
                        {
                            Ok(published) => Ok(Ok(published)),
                            Err(FixerFoldFailure::Refused(summary)) => Ok(Err(summary)),
                            Err(FixerFoldFailure::Retryable(error)) => Err(error),
                        }
                    })
                    .await;
                    loop {
                        match requests::record_planned_fold(
                            orch,
                            &run_context.project_id,
                            &phase_key,
                            &outcome,
                        )
                        .await
                        {
                            Ok(()) => break,
                            Err(error) => {
                                log::error!(
                                    "write-wave {wave_id} retains fold pending checkpoint: {error}"
                                );
                                tokio::time::sleep(crate::fleet::RETAINED_RESULT_RETRY).await;
                            }
                        }
                    }
                    outcome
                }
            };
            let published = match published {
                Ok(published) => published,
                Err(summary) => return Some(summary),
            };
            #[cfg(test)]
            if CRASH_AFTER_FOLD.lock().unwrap().remove(&wave_id) {
                panic!("simulated runner loss after a durable fold, before successor planning");
            }
            let cached_rekey = durable::retry("post-fold plan", || {
                durable::rekey(&owning_db, &wave_id, index)
            })
            .await;
            let rekeyed = if let Some(cached) = cached_rekey {
                cached.restore(&cell_env)
            } else {
                let rekeyed = rekey_wave_onto_fix(
                    orch,
                    FoldRekey {
                        run_context,
                        checks: &checks,
                        extra_inputs: &extra_inputs,
                        keyed: &keyed,
                        cell_env: &cell_env,
                        repository: &repository_path,
                        object_repository: &object_repository,
                        base: &planning_base,
                        folded_by: index,
                    },
                    &published,
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
                durable::retry("post-fold plan", || {
                    durable::save_rekey(&owning_db, &wave_id, index, &rekeyed)
                })
                .await;
                rekeyed
            };
            claims.release(
                rekeyed
                    .invalidated
                    .iter()
                    .map(|lane| lane_key(&run_context.project_id, &keyed[*lane])),
            );
            // The fixers after this one and every verdict run on the fold, and
            // a refusal is about no tree, so only an answer already in hand can
            // have gone stale.
            let unanswered: BTreeSet<usize> = split.fixers[position + 1..]
                .iter()
                .chain(&split.verifiers)
                .chain(split.refused.iter().map(|(lane, _)| lane))
                .copied()
                .collect();
            stale.extend(
                rekeyed
                    .invalidated
                    .into_iter()
                    .filter(|lane| !unanswered.contains(lane)),
            );
            keyed = rekeyed.keyed;
            tree_hash = rekeyed.tree_hash;
            process_env = rekeyed.process_env;
            commit = published.commit.clone();
            claims.claim(
                split.fixers[position + 1..]
                    .iter()
                    .map(|lane| lane_key(&run_context.project_id, &keyed[*lane])),
                &commit,
            );
            folds.push(published);
        }
    }
    let observation_commit = commit;

    // 2. After a fold every lane still to answer is keyed to the commit the
    // chain ended at, which the planning-time cache filter never saw, and on a
    // moved head that head's own wave is usually verifying the same lanes at
    // the same keys: wait for those, and run only what is still a miss.
    let saved_convergence = durable::retry("convergence checkpoint", || {
        durable::convergence(&owning_db, &wave_id)
    })
    .await;
    let mut rerun_fixers = saved_convergence.clone().unwrap_or_default();
    if !folds.is_empty() {
        if saved_convergence.is_none() {
            for lane in &stale {
                ran.remove(lane);
            }
            let candidates: Vec<usize> = split
                .verifiers
                .iter()
                .chain(&stale)
                .copied()
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect();
            let (left, answered) = lanes_left_to_run(
                orch.db.local.clone(),
                &mut claims,
                &run_context.project_id,
                &keyed,
                &candidates,
                &process_env,
                FOLD_HANDOFF_WAIT,
            )
            .await;
            answered_elsewhere.extend(answered);
            claims.claim(
                left.iter()
                    .map(|lane| lane_key(&run_context.project_id, &keyed[*lane])),
                &observation_commit,
            );
            rerun_fixers = left
                .into_iter()
                .filter(|lane| {
                    stale.contains(lane)
                        && checks
                            .get(&keyed[*lane].0.name)
                            .is_some_and(|check| check.fixes)
                })
                .collect();
        }
        for lane in &stale {
            ran.remove(lane);
        }
    }
    loop {
        match durable::save_convergence(&owning_db, &wave_id, &rerun_fixers).await {
            Ok(()) => break,
            Err(error) => {
                log::error!("write-wave {wave_id} retains convergence plan: {error}");
                tokio::time::sleep(crate::fleet::RETAINED_RESULT_RETRY).await;
            }
        }
    }

    // 3. A fixer whose answer a later fold made stale runs once more, on the
    // tree that landed. A rewrite then is never folded: a wave folds each
    // fixer at most once, which is what bounds the fix loop, and a second
    // rewrite is reported as non-convergent instead of chased.
    if let (false, Some(requests)) = (rerun_fixers.is_empty(), requests.as_ref()) {
        log::info!(
            "when:write checks: a later fold invalidated {:?}; re-running them against {observation_commit}",
            rerun_fixers
                .iter()
                .map(|lane| keyed[*lane].0.name.as_str())
                .collect::<Vec<_>>()
        );
        let at = LaneAt {
            commit: &observation_commit,
            tree_hash: &tree_hash,
            keyed: &keyed,
            process_env: &process_env,
        };
        let reruns = futures_util::future::join_all(rerun_fixers.iter().map(|&index| {
            let request = requests.request(
                index,
                &at,
                keyed[index].0.command.clone(),
                crate::mcp::handlers::run::check_stream_id(tool_use_id, index),
            );
            let phase_key = format!("{wave_id}:{index}:convergence");
            async move {
                (
                    index,
                    fixer_outcome(
                        index,
                        requests::file_planned_check_request(
                            orch,
                            &phase_key,
                            request,
                            requests::WriteLanePhase::Convergence,
                        )
                        .await,
                    ),
                )
            }
        }))
        .await;
        let mut rewrote = Vec::new();
        for (index, FixerOutcome { result, delta, .. }) in reruns {
            ran.insert(index, result);
            if let Some(delta) = delta {
                rewrote.push((index, delta));
            }
        }
        if !rewrote.is_empty() {
            let names = rewrote
                .iter()
                .map(|(index, _)| keyed[*index].0.name.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            let patches = rewrote
                .iter()
                .map(|(_, delta)| delta_patch_excerpt(repo_root, delta))
                .collect::<Vec<_>>()
                .join("\n");
            return Some(format!(
                "Checks: \u{2717} write-check fix (non-convergent: {names} rewrote the tree again after the fold)\n```diff\n{patches}\n```"
            ));
        }
    }
    if !split.fixers.is_empty() {
        status_board.set_phase(
            Some("dispatching"),
            Some("dispatching checks"),
            Some("preparing check requests".to_string()),
        );
    }

    // 4. The verdicts, all at once: each lane is its own pure-verdict request
    // placed by its own selector and platforms, and each is recorded the moment
    // its own request settles. A verdict is reproducible from the commit
    // alone, so what a check that does not declare `fixes` writes is discarded
    // and reported with its result, never folded.
    if let Some(requests) = requests.as_ref() {
        let at = LaneAt {
            commit: &observation_commit,
            tree_hash: &tree_hash,
            keyed: &keyed,
            process_env: &process_env,
        };
        for &index in &split.fixers {
            let original = format!("{wave_id}:{index}:fixer");
            if stale.contains(&index) {
                durable::retry("stale fixer custody", || requests::supersede_planned_check_request(
                    orch, &run_context.project_id, &original,
                    "a later landed fixer invalidated this result; the convergence phase or a reusable verdict answers the final tree")).await;
                if !rerun_fixers.contains(&index) {
                    continue;
                }
            }
            let key = if rerun_fixers.contains(&index) {
                format!("{wave_id}:{index}:convergence")
            } else {
                original
            };
            let request = requests.request(
                index,
                &at,
                keyed[index].0.command.clone(),
                crate::mcp::handlers::run::check_stream_id(tool_use_id, index),
            );
            let mut finalized = durable::retry("final fixer recording", || {
                requests::finalize_planned_check_request(orch, &key, &request)
            })
            .await;
            if let Some(result) = finalized.results.remove(&index) {
                ran.insert(index, result);
            }
        }
    }
    #[cfg(test)]
    if CRASH_AFTER_RECORD.lock().unwrap().remove(&wave_id) {
        panic!("simulated runner loss after final fixer observations, before verifier successors");
    }
    let ran = Arc::new(std::sync::Mutex::new(ran));
    let lane_wave_id = wave_id.clone();
    let lane_orch = orch.clone();
    let lane_commit = observation_commit.clone();
    let lane_tree = tree_hash.clone();
    let lane_keyed = Arc::new(keyed.clone());
    let lane_env = Arc::new(process_env.clone());
    let sealed_coordinates = sealed_coordinates(
        CheckRunCommit {
            evaluated: &observation_commit,
            defined_by: &defined_by_commit,
        },
        &tree_hash,
        &keyed,
    );
    let results = run_planned_checks_with_board(
        orch.db.local.clone(),
        &run_context.project_id,
        CheckRunCommit {
            evaluated: &observation_commit,
            defined_by: &defined_by_commit,
        },
        &tree_hash,
        run_context.job_id.as_str(),
        &keyed,
        &sealed_coordinates,
        &head_infra_scopes(
            keyed.len(),
            run_context.job_id.as_str(),
            &observation_commit,
        ),
        tool_use_id,
        &CheckProducer::new(CheckConsumer::Write, tool_use_id),
        CheckExecMode::Isolated,
        Some(orch),
        Some(status_board),
        &process_env,
        only_suite.is_some(),
        move |index, command, stream_id| {
            let wave_id = lane_wave_id.clone();
            let ran = ran.clone();
            let orch = lane_orch.clone();
            let requests = requests.clone();
            let commit = lane_commit.clone();
            let tree_hash = lane_tree.clone();
            let keyed = lane_keyed.clone();
            let process_env = lane_env.clone();
            async move {
                // A fixer's answer (or a refusal) is already in hand. An
                // uncached retry always runs afresh, as a verdict.
                if !command.starts_with(UNCACHED_RETRY_PREFIX) {
                    if let Some(result) = ran.lock().unwrap().remove(&index) {
                        return result;
                    }
                }
                let Some(requests) = requests else {
                    return Err(CheckExecutionFailure::substrate(
                        SubstrateFailureShape::Dispatch,
                        "could not resolve repository for the write check",
                    ));
                };
                let is_retry = command.starts_with(UNCACHED_RETRY_PREFIX);
                let request = requests.request(
                    index,
                    &LaneAt {
                        commit: &commit,
                        tree_hash: &tree_hash,
                        keyed: &keyed,
                        process_env: &process_env,
                    },
                    command,
                    stream_id,
                );
                let phase = if is_retry { "retry" } else { "verifier" };
                requests::file_planned_check_request(
                    &orch,
                    &format!("{wave_id}:{index}:{phase}"),
                    request,
                    requests::WriteLanePhase::Verifier,
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
                        format!("missing write-check outcome for plan index {index}"),
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

    let verdicts: Vec<(String, bool)> = results
        .iter()
        .filter(|outcome| outcome.passed || outcome.is_genuine_failure())
        .map(|outcome| (outcome.name.clone(), outcome.passed))
        .collect();
    loop {
        match durable::save_verdicts(&owning_db, &wave_id, &verdicts).await {
            Ok(()) => break,
            Err(error) => {
                log::error!("write-wave {wave_id} retains verdict aggregation: {error}");
                tokio::time::sleep(crate::fleet::RETAINED_RESULT_RETRY).await;
            }
        }
    }
    ticket.recorded(results.iter().filter_map(|outcome| {
        use crate::execution::late_write_checks::LaneVerdict;
        let verdict = if outcome.passed {
            LaneVerdict::Passed
        } else if outcome.is_genuine_failure() {
            LaneVerdict::Failed
        } else {
            return None;
        };
        Some((outcome.name.as_str(), verdict))
    }));
    if results.is_empty() {
        return None;
    }
    let attributions = write_check_attributions(orch, run_context.job_id.as_str(), &results).await;
    let summary = format!("Checks: {}", format_check_summary(&results, &attributions));
    let summary = match format_answered_elsewhere(&answered_elsewhere) {
        Some(line) => format!("{summary}\n{line}"),
        None => summary,
    };
    // The fixes are attributed once, at wave level: each fold is its own
    // commit, named there, and the per-check verdicts stay verdicts.
    if folds.is_empty() {
        return Some(summary);
    }
    let commits: Vec<String> = folds.iter().map(|fold| fold.commit.clone()).collect();
    let paths: Vec<String> = folds
        .iter()
        .flat_map(|fold| fold.paths.iter().cloned())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    Some(format_fixed_batch_summary(&summary, &commits, &paths))
}

/// What every request one write wave submits shares. Each lane is its own
/// request for one suite, keyed by job and suite so it returns to the cell its
/// own last run left, and reporting on its own lane of the wave's board.
pub(super) struct WriteWaveRequests {
    pub(super) run_context: RunContext,
    pub(super) repository: String,
    pub(super) store_dir: PathBuf,
    pub(super) checks: HashMap<String, CheckCommand>,
    pub(super) timeouts: Vec<u32>,
    pub(super) defined_by_commit: String,
    /// The build slot's environment: the cell request's, without the transport.
    pub(super) cell_env: Vec<(String, String)>,
    pub(super) tool_use_id: String,
    pub(super) board: CheckStatusBoard,
}

/// The commit one lane's request runs at: that commit's tree, every plan's key
/// on it, and each process's environment there, which is the slot's plus the
/// transport of that commit's own change ([`write_check_transport_env`]).
pub(super) struct LaneAt<'a> {
    pub(super) commit: &'a str,
    pub(super) tree_hash: &'a str,
    pub(super) keyed: &'a [(CheckPlan, String)],
    pub(super) process_env: &'a [(String, String)],
}

impl WriteWaveRequests {
    /// Lane `index`'s request at `at`. Whether it asks for a verdict or runs a
    /// fixer is the submission's choice ([`submit_planned_check`] or
    /// [`submit_planned_fix`]), never the request's shape.
    pub(super) fn request(
        &self,
        index: usize,
        at: &LaneAt<'_>,
        command: String,
        stream_id: String,
    ) -> PlannedCheckRequest {
        let (plan, input_hash) = &at.keyed[index];
        let configured = self
            .checks
            .get(&plan.name)
            .expect("planned check must retain its configured definition");
        let context = &self.run_context;
        PlannedCheckRequest {
            producer: CheckProducer::new(CheckConsumer::Write, &self.tool_use_id),
            project_id: context.project_id.clone(),
            repository: self.repository.clone(),
            store_dir: self.store_dir.clone(),
            sealed_commit: at.commit.to_string(),
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
            env: self.cell_env.clone(),
            item: PlannedCheckBatchItem {
                recording_plan: Some(CheckAdoptionRecordingPlan::new(
                    plan,
                    &crate::execution::cache::SealedCheckCoordinate::new(
                        at.commit,
                        &self.defined_by_commit,
                        at.tree_hash,
                        input_hash,
                    ),
                    &context.job_id,
                    &self.tool_use_id,
                    at.process_env,
                    &CheckProducer::new(CheckConsumer::Write, &self.tool_use_id),
                )),
                index,
                name: plan.name.clone(),
                input_hash: input_hash.clone(),
                resource_identity: CheckResourceIdentityInput::Configured {
                    name: plan.name.clone(),
                    check: configured.clone(),
                },
                command,
                stream_id,
                env: at.process_env.to_vec(),
                verdict_environment_names: plan.verdict_environment_names.clone(),
                timeout_ms: self.timeouts[index].into(),
                executor: configured.executor.clone(),
                verdict_platforms: crate::execution::check_identity::verdict_platforms(configured),
                resource_class: plan.resource_class,
                // A write check is bounded by the head it runs on, not by a
                // fleet of candidate environments.
                infra_scopes: InfraScopes::head_only(&context.job_id, at.commit),
                avoid_executor_ids: Vec::new(),
            },
            run_context: Some(context.clone()),
            status_board: Some(self.board.for_lane(index)),
            on_complete: None,
            purpose: None,
            publication: None,
        }
    }
}

/// How a write wave's cache misses divide by what kind of request each is.
///
/// A declared fixer (`fixes: true`) returns its changes to fold into the
/// branch, and a fold publishes only from the write cell, so a fixer whose
/// `verdictPlatforms` exclude the write cell's platform is refused with the
/// config line to change. Every other check is a verdict, reproducible from
/// the commit alone, placed by its own selector and platforms.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(super) struct WriteWaveSplit {
    /// Declared fixers, in plan order: the order they chain in.
    pub(super) fixers: Vec<usize>,
    /// Verdicts, run concurrently once every fixer has folded.
    pub(super) verifiers: Vec<usize>,
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
        let Some(check) = checks.get(name).filter(|check| check.fixes) else {
            split.verifiers.push(index);
            continue;
        };
        let platforms = crate::execution::check_identity::verdict_platforms(check);
        if platforms.is_empty()
            || platforms
                .iter()
                .any(|platform| platform.eq_ignore_ascii_case(write_cell_platform))
        {
            split.fixers.push(index);
        } else {
            split.refused.push((
                index,
                format!(
                    "check `{name}` fixes files but its verdictPlatforms [{}] exclude the write cell on {write_cell_machine} ({write_cell_platform}); a fix folds only from the write cell. Widen verdictPlatforms or drop `fixes` in .cairn/config.yaml",
                    platforms.join(", ")
                ),
            ));
        }
    }
    split
}

/// One fixer request's answer.
pub(super) struct FixerOutcome {
    /// The lane's result.
    pub(super) result: Result<CheckExecResult, CheckExecutionFailure>,
    /// The rewrite it returned, if it changed tracked files.
    pub(super) delta: Option<crate::fleet::MutationDelta>,
    /// The cell request and store that rewrite is published from.
    pub(super) cell: Option<(CellRequest, PathBuf)>,
}

pub(super) fn fixer_outcome(
    index: usize,
    submitted: Result<PlannedCheckBatchOutcome, String>,
) -> FixerOutcome {
    match submitted {
        Ok(mut outcome) => FixerOutcome {
            result: outcome.results.remove(&index).unwrap_or_else(|| {
                Err(CheckExecutionFailure::substrate(
                    SubstrateFailureShape::Result,
                    format!("missing fixer outcome for plan index {index}"),
                ))
            }),
            delta: outcome.delta,
            cell: outcome.request.zip(outcome.store_dir),
        },
        Err(error) => FixerOutcome {
            result: Err(CheckExecutionFailure::substrate(
                SubstrateFailureShape::Dispatch,
                error,
            )),
            delta: None,
            cell: None,
        },
    }
}

/// One fixer's delta, and where it is published from.
pub(super) struct FixerFold<'a> {
    pub(super) run_context: &'a RunContext,
    pub(super) repo_root: &'a Path,
    pub(super) cwd: &'a str,
    pub(super) request: &'a CellRequest,
    pub(super) store_dir: &'a Path,
    pub(super) delta: &'a crate::fleet::MutationDelta,
    pub(super) fixer: &'a str,
}

enum FixerFoldFailure {
    Refused(String),
    Retryable(String),
}

/// Publish one fixer's delta as a commit of its own on the job's branch.
/// `Err` is the summary the wave returns instead, when it cannot land.
async fn fold_fixer_delta(
    orch: &Orchestrator,
    fold: FixerFold<'_>,
    since: i64,
) -> Result<crate::mcp::handlers::run::PublishedSlotDelta, FixerFoldFailure> {
    let author = PublicationIdentity::new("Cairn checks", "checks@cairn.local");
    let db = crate::execution::routing::owning_db_for_job(&orch.db, &fold.run_context.job_id)
        .await
        .map_err(|error| {
            FixerFoldFailure::Retryable(format!("resolve write-check fold database: {error}"))
        })?;
    let job_id = fold.run_context.job_id.clone();
    let branch = match db
        .query_text("SELECT branch FROM jobs WHERE id = ?1", (job_id,))
        .await
    {
        Ok(Some(branch)) => branch,
        Ok(None) => {
            return Err(FixerFoldFailure::Refused(
                "Checks: \u{2717} write-check fold (logical branch is absent)".into(),
            ))
        }
        Err(error) => {
            return Err(FixerFoldFailure::Retryable(format!(
                "resolve write-check logical branch: {error}"
            )))
        }
    };
    // This narrows a retry after a raced retirement; the publisher still makes
    // the authoritative decision under the store lock immediately before sealing.
    match crate::mcp::handlers::branch::publication_authority(
        &orch.db,
        &fold.run_context.job_id,
        Some(&fold.run_context.run_id),
        Some(&branch),
    )
    .await
    {
        crate::mcp::handlers::branch::PublicationAuthority::Authorized => {}
        crate::mcp::handlers::branch::PublicationAuthority::Retired(reason) => {
            return Err(FixerFoldFailure::Refused(format!(
                "Checks: ✗ write-check fold (job retired before the fold's outcome was recorded; not determined. {reason})"
            )))
        }
        crate::mcp::handlers::branch::PublicationAuthority::Unknown(reason) => {
            return Err(FixerFoldFailure::Retryable(reason))
        }
    }
    let published = crate::mcp::handlers::run::publish_and_seal_slot_delta_replayable(
        orch,
        fold.store_dir,
        fold.request,
        fold.delta,
        &branch,
        &format!("fix: apply {} write-check changes", fold.fixer),
        Some(&author),
        since,
    )
    .await;
    let published = match published {
        Ok(Some(published)) => published,
        Ok(None) => {
            return Err(FixerFoldFailure::Refused(
                "Checks: \u{22EF} superseded — the branch advanced before this wave's fix landed. \
                 The stale fix was discarded without changing the branch or recording verdicts. \
                 Read cairn:~/checks for the branch's current verdicts."
                    .into(),
            ));
        }
        Err(error) => {
            let patch = delta_patch_excerpt(fold.repo_root, fold.delta);
            return Err(FixerFoldFailure::Retryable(format!(
                "Checks: \u{2717} write-check fold ({error})\n```diff\n{patch}\n```"
            )));
        }
    };
    log::info!(
        "write checks published {}'s fix commit {} for {} path(s), patch_bytes={}",
        fold.fixer,
        published.commit,
        published.paths.len(),
        published.patch.len()
    );
    let _ = orch.services.emitter.emit(
        "worktree-changed",
        serde_json::json!({"path": fold.cwd, "source": "write-check-fix"}),
    );
    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "check_result_cache", "action": "invalidate"}),
    );
    Ok(published)
}

/// A write wave re-keyed onto the commit one fixer's fold landed.
pub(super) struct FixedWave {
    /// The wave's plans paired with their cache key on the folded tree. A lane
    /// the fold invalidated also carries its re-planned command.
    pub(super) keyed: Vec<(CheckPlan, String)>,
    /// Whole-tree hash of the folded commit, re-stamped onto every row.
    pub(super) tree_hash: String,
    /// The plan indices whose verdict the fold invalidated.
    pub(super) invalidated: Vec<usize>,
    /// Each check process's environment at the folded commit: the slot's, plus
    /// the transport of the folded commit's own change.
    pub(super) process_env: Vec<(String, String)>,
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
    /// The plan index of the fixer whose output the fold is.
    pub(super) folded_by: usize,
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
        folded_by,
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
    // Attribution is a property of the fold: the question is whether the
    // fixer that produced it can account for every path in it. It is asked
    // against the FOLDED tree's inputs, which is the tree the verdicts are
    // being keyed to.
    let attributed = fix_is_attributed_to_declared_fixers(
        &fixed_paths,
        &[inputs.for_check(&plans[folded_by].name)],
    );
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
            index == folded_by,
            attributed,
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

/// The wave as one fixer's fold finds it.
pub(super) struct FoldRekey<'a> {
    pub(super) run_context: &'a RunContext,
    pub(super) checks: &'a HashMap<String, CheckCommand>,
    /// Node-level extra inputs, so every selector resolves afresh against the
    /// tree the fold actually landed.
    pub(super) extra_inputs: &'a HashMap<String, Vec<String>>,
    /// Every plan with its key on the tree before the fold.
    pub(super) keyed: &'a [(CheckPlan, String)],
    pub(super) cell_env: &'a [(String, String)],
    pub(super) repository: &'a Path,
    pub(super) object_repository: &'a Path,
    /// The immutable base the original sealed wave was planned against.
    pub(super) base: &'a str,
    pub(super) folded_by: usize,
}

/// Re-key a wave onto the commit one fixer's fold landed, naming the lanes
/// whose verdict the fold invalidated. Nothing is run here: the fixers still to
/// chain and every verdict run on the folded commit anyway, and the caller
/// re-verifies whatever answer in hand went stale.
///
/// Survival is decided per lane by [`verdict_survives_fix`]. In the ordinary
/// case, a declared formatter rewriting files inside its own `impact`, its own
/// verdict survives and nothing else has answered yet, so the fold costs no
/// further run.
///
/// Planning is bound to `fix.commit`, the commit the fold was published as,
/// never to whatever the branch head is by the time this runs: a detached wave
/// can outlive several later commits.
pub(super) async fn rekey_wave_onto_fix(
    orch: &Orchestrator,
    fold: FoldRekey<'_>,
    fix: &crate::mcp::handlers::run::PublishedSlotDelta,
    claims: &mut WaveClaims,
) -> Result<FixedWave, String> {
    let planning = FixReplanning {
        jj: crate::jj::environment(&orch.jj_binary_path, &orch.config_dir),
        repository: fold.repository.to_path_buf(),
        object_repository: fold.object_repository.to_path_buf(),
        base: fold.base.to_string(),
        head: fix.commit.clone(),
        checks: fold.checks.clone(),
        extra_inputs: fold.extra_inputs.clone(),
        plans: fold.keyed.iter().map(|(plan, _)| plan.clone()).collect(),
        keys_before: fold.keyed.iter().map(|(_, key)| key.clone()).collect(),
        folded_by: fold.folded_by,
        fixed_paths: fix.paths.clone(),
        fix_landed_on_tested_base: fix.landed_on_tested_base,
        db: orch.db.local.clone(),
        project_id: fold.run_context.project_id.clone(),
    };
    let (keyed, invalidated, tree_hash, transport) =
        tokio::task::spawn_blocking(move || replan_onto_fix(planning))
            .await
            .map_err(|error| format!("join the post-fix planning unit: {error}"))??;
    // The keys these lanes were planned under will never be recorded now, so
    // no other fold should wait on them.
    claims.release(
        invalidated
            .iter()
            .map(|index| lane_key(&fold.run_context.project_id, &fold.keyed[*index])),
    );
    Ok(FixedWave {
        keyed,
        tree_hash,
        invalidated,
        process_env: write_check_transport_env(fold.cell_env, transport),
    })
}
