//! Synchronous `when:write` project-check runner.
//!
//! A committing `write`/`run` that seals a source-touching commit calls
//! [`run_write_checks_after_seal`] right after the seal. It loads the project's
//! `checks` contract, computes the node's changed files, selects the affected
//! `when:write` checks, and runs each one to completion against the sealed
//! commit — streaming its output live into the originating tool's transcript and
//! returning a compact inline pass/fail line. A cache hit returns the stored
//! verdict without re-running.
//!
//! ## Contract source: live project config, not the worktree
//!
//! The `checks` contract is read from the project's MAIN-CHECKOUT
//! `.cairn/config.yaml` (located by `project_id`), which is exactly the file the
//! Settings UI edits. It is not read from a mutable executor projection's committed
//! copy: that copy is snapshotted when the branch is cut, so a project-level
//! checks edit made while a session is in flight would never reach it. Sourcing
//! the contract live means a Settings edit takes effect on the very next commit
//! of an in-flight session with no restart. The live project config wins
//! outright over a branch's own committed `.cairn/config.yaml`; the worktree copy
//! is a fallback only when the project repo path cannot be resolved. Everything
//! else — the changed-file set, impact-glob matching, the cache tree hash, and
//! the check commands' working directory — still targets the sealed worktree
//! commit. See [`load_live_project_checks`].
//!
//! ## Scope
//!
//! Only the `when:write` cadence runs here; `when:review` runs at turn-end
//! ([`crate::execution::checks_turn_end`]). A check passes iff its
//! command exits `0` — output parsing ([`crate::execution::check_parsers`]) is
//! pure enrichment (failing test names + excerpt) and never changes a verdict;
//! a spawn error or sandbox denial is a clear failure, never a silent pass.
//! Placeholder selectors narrow to the delta since the check's last PASSING
//! baseline and fall back to the cumulative branch diff on any uncertainty (see
//! `baseline_delta_changed_files`). Checks are invoked through the `run` verb's
//! process machinery directly (not `run_one`), so a sandbox-blocked syscall
//! surfaces as a failed exit rather than an interactive fence prompt.
//!
//! ## Concurrency, ordering, and the fold
//!
//! The affected cache-MISS checks run SEQUENTIALLY inside ONE `AllowDelta` cell
//! lease materialized at the just-sealed commit. Cache hits are resolved before
//! admission, so an all-hit cadence acquires no slot. The committing run's
//! affinity key prefers its warm slot while each check retains its own cache
//! identity, result, timing, output stream, and provenance.
//!
//! The lease takes no per-item snapshot, so the checks share one mutable
//! checkout and each observes what the checks before it wrote. That is what
//! makes the write cadence the cadence that FOLDS a formatter's fix back into
//! the commit that produced it, and it is why submission order is not plan
//! order: checks declared `fixes: true` are submitted FIRST
//! ([`fixer_first_submission_order`]), so every other check in the wave already
//! validates the tree the fix produces. The wave then publishes that fix as one
//! commit, re-keys its verdicts onto the tree that landed, and re-runs nothing —
//! each check executes at most once per commit. Only a verdict the fix
//! demonstrably invalidated ([`verdict_survives_fix`]) is re-verified, in one
//! bounded batch that never folds again. Fixers run in plan order among
//! themselves, so an earlier fixer cannot see a later one's rewrite and only the
//! LAST fixer's own verdict is carried across the fold
//! ([`fixers_superseded_by_a_later_fixer`]).
//!
//! ## Cache key
//!
//! Each check's verdict is keyed by [`check_result_key`]: the impact-filtered
//! sealed-tree content, configured command, platform, and cached toolchain
//! identity. A commit that changed none of a check's inputs — a doc-only commit
//! landing after a source commit — hits the cache even though the whole-tree hash moved, so the
//! check is not re-run. A check with no `impact` globs keeps whole-tree keying
//! ([`crate::jj::sealed_tree_hash`]). The row also stores that whole-tree hash and
//! re-stamps it on every evaluation (run OR hit), so the `/checks` listing — which
//! looks rows up by whole-tree hash — still surfaces every applicable check at the
//! current tree. If the sealed tree can't be read, an impact-scoped check falls
//! back to whole-tree keying: conservative (re-runs on any change), never a false
//! reuse.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::time::Instant;

// Isolated checks carry their configured executor selector into the immutable request.
use crate::config::project_settings::{
    CheckCommand, CheckPolicy, CheckResourceClass, CheckWhen, ChecksContract,
};
use crate::execution::cache::{
    claim_check_execution, claim_infra_escalation, get_check_result,
    get_exact_reusable_check_result, get_reusable_check_observation_id,
    get_suppressed_check_result, list_latest_passing_check_results_for_job,
    record_cached_check_observation, record_fresh_check_observation, store_check_result,
    CachedCheckObservationWrite, CheckExecutionClaim, CheckResultCacheEntry, CheckResultCacheWrite,
    CheckTestResultRow, FreshCheckObservationWrite,
};
use crate::execution::inputs::{
    any_check_declares_inputs, InputSelector, ResolvedInputs, TreeBlobs, TreeSnapshot,
};
use crate::fleet::{
    CellOutcome, CellPriority, CellRequest, CommandResourceIdentity, MutationPolicy,
    PureVerdictBatchItem,
};
use crate::mcp::git::GitAuthor;
use cairn_common::executor_protocol::{
    CellCommandClass, ExecutorSelector, PlacementMobility, ProcessBatchExecution, ProcessBatchItem,
    RepositoryLocator, ResourceReservation, ResourceReservationSource,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CheckExecMode {
    Isolated,
    Shared,
}

const SLOT_CHECK_DEV_DEBUG_ENV: (&str, &str) = ("CARGO_PROFILE_DEV_DEBUG", "line-tables-only");

fn slot_check_env(mut env: Vec<(String, String)>) -> Vec<(String, String)> {
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
    run_planned_checks_at_commit(
        db,
        project_id,
        CheckRunCommit {
            evaluated: tree_hash,
            defined_by: tree_hash,
        },
        tree_hash,
        job_id,
        plans,
        tool_use_id,
        mode,
        diagnostic_orch,
        execute,
        notify,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_planned_checks_at_commit<F, Fut, N, E>(
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
    run_planned_checks_with_board(
        db,
        project_id,
        commit,
        tree_hash,
        job_id,
        plans,
        tool_use_id,
        mode,
        diagnostic_orch,
        None,
        execute,
        notify,
    )
    .await
}

type CheckStatusNotify =
    Arc<dyn Fn(Vec<CheckStatusEntry>, Option<String>, Option<String>) + Send + Sync>;

#[derive(Clone)]
pub(crate) struct CheckStatusBoard {
    entries: Arc<std::sync::Mutex<Vec<CheckStatusEntry>>>,
    phase: Arc<std::sync::Mutex<(Option<String>, Option<String>)>>,
    notify: CheckStatusNotify,
}

impl CheckStatusBoard {
    fn new(plans: &[(CheckPlan, String)], notify: CheckStatusNotify) -> Self {
        Self {
            entries: Arc::new(std::sync::Mutex::new(
                plans
                    .iter()
                    .enumerate()
                    .map(|(index, (plan, _))| CheckStatusEntry {
                        index,
                        name: plan.name.clone(),
                        state: "pending".into(),
                        annotation: None,
                    })
                    .collect(),
            )),
            phase: Arc::new(std::sync::Mutex::new((None, None))),
            notify,
        }
    }

    fn emit(&self) {
        let entries = self.entries.lock().unwrap().clone();
        let (phase, detail) = self.phase.lock().unwrap().clone();
        (self.notify)(entries, phase, detail);
    }

    fn emit_initial(&self) {
        self.emit();
    }

    pub(crate) fn transition(&self, index: usize, state: &str, annotation: Option<String>) {
        {
            let mut entries = self.entries.lock().unwrap();
            if let Some(entry) = entries.get_mut(index) {
                entry.state = state.to_string();
                entry.annotation = annotation;
            }
        }
        if state == "running" {
            *self.phase.lock().unwrap() = (Some("running".into()), None);
        } else if self
            .entries
            .lock()
            .unwrap()
            .iter()
            .all(|entry| entry.state == "passed" || entry.state == "failed")
        {
            *self.phase.lock().unwrap() = (None, None);
        }
        self.emit();
    }

    fn set_phase(&self, phase: Option<&str>, detail: Option<String>) {
        *self.phase.lock().unwrap() = (phase.map(str::to_string), detail);
        self.emit();
    }
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ManualCheckCacheContext {
    pub project_id: String,
    pub job_id: String,
    pub commit_sha: String,
    pub tree_hash: String,
    pub input_hash: String,
    cacheable: bool,
    entry: Option<crate::execution::cache::CheckResultCacheEntry>,
}

impl ManualCheckCacheContext {
    pub fn require_cacheable(&self) -> Result<(), String> {
        self.cacheable.then_some(()).ok_or_else(|| {
            "the run coordinate cannot be stored as sealed-tree evidence".to_string()
        })
    }
}

fn require_snapshot_command(expected: &str, submitted: &str) -> Result<(), CheckExecutionFailure> {
    if submitted == expected {
        Ok(())
    } else {
        Err(CheckExecutionFailure::substrate(
            SubstrateFailureShape::Result,
            "manual check command diverged from its immutable contract snapshot",
        ))
    }
}

#[derive(Debug, Clone)]
struct ManualCheckContractSnapshot {
    context: ManualCheckCacheContext,
    repository_path: String,
    /// The commit whose `.cairn/config.yaml` declared the requested check. An
    /// explicitly requested branch supplies both the definition and the content,
    /// so a manual check on one branch can never run another branch's definition.
    defined_by_commit: String,
    configured_check: CheckCommand,
    plan: CheckPlan,
    timeout_ms: u32,
    resource_identity_key: String,
}

/// Resolve every mutable input to a manual check exactly once. The returned owned
/// values are the contract carried through cache lookup, execution, and recording;
/// callers must not consult the live project config again during this operation.
async fn resolve_manual_check_contract_snapshot(
    orch: &Orchestrator,
    run_id: &str,
    check_name: &str,
    branch: Option<&str>,
) -> Result<ManualCheckContractSnapshot, String> {
    let db = crate::execution::routing::routing_db_for_id(&orch.db, run_id)
        .await
        .map_err(|error| error.to_string())?;
    let run_id = run_id.to_string();
    let (job_id, project_id, repository_path) = db
        .read(move |conn| {
            let run_id = run_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT r.job_id, r.project_id, p.repo_path
                         FROM runs r
                         JOIN projects p ON p.id = r.project_id
                         WHERE r.id = ?1 AND r.job_id IS NOT NULL
                         LIMIT 1",
                        (run_id.as_str(),),
                    )
                    .await?;
                let row = rows.next().await?.ok_or_else(|| {
                    crate::storage::DbError::Row(format!(
                        "run {run_id} has no resolvable job coordinate"
                    ))
                })?;
                Ok((row.text(0)?, row.text(1)?, row.text(2)?))
            })
        })
        .await
        .map_err(|error| error.to_string())?;
    let repository = PathBuf::from(&repository_path);
    // Resolve the coordinate FIRST: the requested branch (or the job's logical
    // head) supplies the content to check AND the definition to check it with.
    let commit = match branch {
        Some(branch) => {
            let store = crate::jj::project_store_dir(&orch.config_dir, &repository);
            let coordinate_repository = if crate::jj::is_jj_dir(&store) {
                store
            } else {
                repository.clone()
            };
            cairn_vcs::resolve_coordinate(&coordinate_repository, branch)
                .await
                .map_err(|error| format!("branch {branch:?} is unresolvable: {error}"))?
        }
        None => crate::execution::cache::resolve_job_logical_head(orch, &job_id).await?,
    };
    let CommitChecksContract {
        contract: ChecksContract {
            checks,
            extra_inputs,
        },
        defined_by_commit,
    } = load_checks_contract_at_commit(&repository, &commit)
        .await
        .ok_or_else(|| format!("commit {commit} declares no configured checks"))?;
    assert_eq!(
        defined_by_commit, commit,
        "a manual check must be defined by the commit it evaluates"
    );
    let check = checks
        .get(check_name)
        .ok_or_else(|| format!("configured check {check_name:?} was not found"))?;
    let jj = JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
    let tree_hash = crate::jj::logical_tree_hash(&jj, &repository, &commit)?;
    let entries = crate::jj::tree_entries(&jj, &repository, &commit).ok();
    let blobs = TreeBlobs {
        jj: &jj,
        repository: &repository,
    };
    let snapshot = TreeSnapshot::new(entries.as_deref(), &blobs);
    let inputs = ResolvedInputs::resolve(&checks, &extra_inputs, &snapshot);
    let input_hash = check_result_key(
        check,
        inputs.for_check(check_name),
        entries.as_deref(),
        &tree_hash,
        &check_platform_identity(),
        check_toolchain_identity(),
    );
    let plan = plan_checks(&checks, &inputs, &[], &repository)
        .into_iter()
        .find(|plan| plan.name == check_name)
        .ok_or_else(|| format!("configured check {check_name:?} could not be planned"))?;
    if let Some(error) = plan.config_error.as_deref() {
        return Err(format!(
            "configured check {check_name:?} is unrunnable: {error}"
        ));
    }
    let configured_check = check.clone();
    let timeout_ms =
        resolve_check_timeout_ms(Some(&configured_check), DEFAULT_REVIEW_CHECK_TIMEOUT_MS);
    let resource_identity_key = check_resource_identity(check_name, &configured_check).key;
    let entry =
        crate::execution::cache::get_check_result(db, &project_id, check_name, &input_hash)?;
    Ok(ManualCheckContractSnapshot {
        context: ManualCheckCacheContext {
            project_id,
            job_id,
            commit_sha: commit,
            tree_hash,
            input_hash,
            cacheable: true,
            entry,
        },
        repository_path,
        defined_by_commit,
        configured_check,
        plan,
        timeout_ms,
        resource_identity_key,
    })
}

/// Resolve manual check evidence from an authenticated run's durable coordinate.
/// The logical head is sealed by construction, so process cwd and loose host files
/// cannot influence cache ownership or the computed tree.
pub async fn manual_check_cache_context(
    orch: &Orchestrator,
    run_id: &str,
    check_name: &str,
    branch: Option<&str>,
) -> Result<ManualCheckCacheContext, String> {
    Ok(
        resolve_manual_check_contract_snapshot(orch, run_id, check_name, branch)
            .await?
            .context,
    )
}

/// Result of a trusted, agent-initiated configured check run.
///
/// Every identity in this response was derived by the runner from the authenticated
/// run coordinate and the project's live checks contract. Callers never supply a
/// command, content hash, environment fingerprint, or verdict.
///
/// The observation fields report what the run RECORDED, carried out of the
/// recorder itself. They are never re-derived from a second lookup: the key a
/// requesting machine computes does not describe a verdict another machine
/// produced, so a lookup-derived reply calls a spilled check unrecorded while its
/// row sits in the database.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ManualConfiguredCheckResult {
    pub check_name: String,
    pub commit_sha: String,
    pub tree_hash: String,
    pub input_hash: String,
    /// The environment identity the recorded observation is keyed by. EMPTY when
    /// a remote executor produced the verdict — Cairn cannot yet identify that
    /// machine's verdict environment, so the row is addressable by id and
    /// readable as diagnosis while nothing reuses it — and empty when this run
    /// recorded nothing at all.
    pub environment_fingerprint: String,
    /// The observation this run recorded, or `None` when it recorded none: a
    /// suppressed check never ran, and a failed write leaves a real verdict
    /// standing without a durable row behind it.
    pub observation_id: Option<String>,
    /// Whether the recorded observation may answer a later run of this check on
    /// these inputs without executing it.
    pub reusable: bool,
    /// `fresh` when this run produced the verdict, `cached` when a stored
    /// observation answered it without running anything.
    pub disposition: String,
    pub passed: bool,
    pub exit_code: Option<i32>,
    /// Set when the run produced NO VERDICT about the tree.
    pub no_verdict: Option<ManualCheckNoVerdict>,
    pub output_tail: String,
}

/// Why a manual run has no verdict to report.
///
/// An infrastructure failure and a suppression are facts about Cairn, not results
/// about the caller's change. Each has to render as itself: reporting either as a
/// red — or as a complaint about recording — hides the only thing the reader can
/// act on.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ManualCheckNoVerdict {
    /// `infrastructure` when Cairn's own machinery failed around the check,
    /// `suppressed` when Cairn declined to run it after repeated failures.
    pub kind: String,
    /// The consecutive infrastructure failures behind a `suppressed` decision.
    pub after_failures: Option<i64>,
    /// The named cause, composed for the reader where the failure happened.
    pub cause: String,
}

/// Build the manual reply from the outcome the run returned.
///
/// Everything here comes from the evaluation itself — its verdict, and the
/// observation its recorder wrote. A second lookup could only re-derive a key the
/// recorder may not have used, which is exactly how a verdict that ran on another
/// machine came back as "not recorded".
fn manual_configured_check_result(
    check_name: &str,
    commit_sha: String,
    tree_hash: String,
    input_hash: String,
    outcome: CheckOutcome,
) -> ManualConfiguredCheckResult {
    let no_verdict = manual_check_no_verdict(&outcome);
    let disposition = if outcome.cached { "cached" } else { "fresh" }.to_string();
    let (observation_id, environment_fingerprint, reusable) = match outcome.recorded {
        Some(recorded) => (
            Some(recorded.id),
            recorded.environment_fingerprint,
            recorded.reusable,
        ),
        None => (None, String::new(), false),
    };
    ManualConfiguredCheckResult {
        check_name: check_name.to_string(),
        commit_sha,
        tree_hash,
        input_hash,
        environment_fingerprint,
        observation_id,
        reusable,
        disposition,
        passed: outcome.passed,
        exit_code: outcome.exit_code,
        no_verdict,
        output_tail: outcome.output_tail,
    }
}

/// Name a run that produced no verdict, using the same predicate the wake path
/// uses to decide whether a red is the agent's business.
fn manual_check_no_verdict(outcome: &CheckOutcome) -> Option<ManualCheckNoVerdict> {
    if outcome.passed || outcome.is_genuine_failure() {
        return None;
    }
    Some(ManualCheckNoVerdict {
        kind: match outcome.suppressed_after {
            Some(_) => "suppressed",
            None => "infrastructure",
        }
        .to_string(),
        after_failures: outcome.suppressed_after,
        cause: outcome.output_tail.clone(),
    })
}

/// Execute one named configured suite at an authenticated agent run's sealed head.
///
/// This is the trusted manual producer. It deliberately accepts no raw command or
/// caller-asserted result metadata: the durable run resolves ownership and commit,
/// the live project contract supplies the command, and the normal planned-check
/// path computes identities, performs conservative reuse, executes misses, parses
/// results, and records immutable observations.
pub async fn run_manual_configured_check(
    orch: &Orchestrator,
    run_id: &str,
    check_name: &str,
    branch: Option<&str>,
) -> Result<ManualConfiguredCheckResult, String> {
    let contract = resolve_manual_check_contract_snapshot(orch, run_id, check_name, branch).await?;
    let context = contract.context;
    let db = crate::execution::routing::owning_db_for_job(&orch.db, &context.job_id)
        .await
        .map_err(|error| error.to_string())?;
    let repository_path = contract.repository_path;
    let repository = PathBuf::from(&repository_path);
    let plan = contract.plan;
    let keyed = vec![(plan.clone(), context.input_hash.clone())];
    let timeout_ms = contract.timeout_ms;
    let store_dir = crate::jj::project_store_dir(&orch.config_dir, &repository);
    let tool_use_id = format!("manual-check:{run_id}:{check_name}");
    let project_id = context.project_id.clone();
    let job_id = context.job_id.clone();
    let commit_sha = context.commit_sha.clone();
    let command_check = contract.configured_check;
    let resource_identity_key = contract.resource_identity_key;
    let submission_orch = orch.clone();
    let submission_repo = repository_path.clone();
    let submission_store = store_dir.clone();
    let submission_project = project_id.clone();
    let submission_job = job_id.clone();
    let submission_commit = commit_sha.clone();
    let submission_tool = tool_use_id.clone();
    let submission_input_hash = context.input_hash.clone();
    let submission_check_name = check_name.to_string();
    let submission_resource_class = plan.resource_class;
    let snapshot_command = plan.command.clone();
    let outcomes = run_planned_checks_at_commit(
        db.clone(),
        &project_id,
        CheckRunCommit {
            evaluated: &commit_sha,
            defined_by: &contract.defined_by_commit,
        },
        &context.tree_hash,
        &job_id,
        &keyed,
        &tool_use_id,
        CheckExecMode::Shared,
        None,
        move |_index, command, stream_id| {
            let orch = submission_orch.clone();
            let repository = submission_repo.clone();
            let store_dir = submission_store.clone();
            let project_id = submission_project.clone();
            let job_id = submission_job.clone();
            let commit_sha = submission_commit.clone();
            let tool_use_id = submission_tool.clone();
            let check = command_check.clone();
            let resource_identity_key = resource_identity_key.clone();
            let input_hash = submission_input_hash.clone();
            let check_name = submission_check_name.clone();
            let snapshot_command = snapshot_command.clone();
            async move {
                require_snapshot_command(&snapshot_command, &command)?;
                submit_planned_check_batch(
                    &orch,
                    PlannedCheckBatchRequest {
                        project_id: project_id.clone(),
                        repository,
                        store_dir,
                        sealed_commit: commit_sha,
                        requesting_job_id: job_id.clone(),
                        owner: cairn_common::executor_protocol::CellOwnerRef {
                            project_id,
                            project_key: None,
                            issue_number: None,
                            job_id: Some(job_id.clone()),
                            execution_seq: None,
                            node_kind: Some("manual-check".to_string()),
                        },
                        affinity_key: Some(job_id),
                        priority: CellPriority::ReviewCheck,
                        env: Vec::new(),
                        items: vec![PlannedCheckBatchItem {
                            index: 0,
                            name: check_name.clone(),
                            input_hash,
                            resource_identity_key,
                            command,
                            stream_id,
                            env: Vec::new(),
                            timeout_ms,
                            executor: check.executor.clone(),
                            resource_class: submission_resource_class,
                        }],
                        run_context: None,
                        mutation_policy: MutationPolicy::PureVerdict,
                        status_board: None,
                    },
                )
                .await?
                .results
                .remove(&0)
                .ok_or_else(|| {
                    CheckExecutionFailure::substrate(
                        SubstrateFailureShape::Result,
                        format!("missing manual batch outcome for {tool_use_id}"),
                    )
                })?
            }
        },
        move |_| {},
    )
    .await;
    let outcome = outcomes.into_iter().next().ok_or_else(|| {
        format!("Cairn lost the result of {check_name}. Run the same command again.")
    })?;
    Ok(manual_configured_check_result(
        check_name,
        commit_sha,
        context.tree_hash,
        context.input_hash,
        outcome,
    ))
}

fn format_fixed_batch_summary(summary: &str, commit: &str, paths: &[String]) -> String {
    let short = commit.get(..12).unwrap_or(commit);
    let verdicts = summary.strip_prefix("Checks: ").unwrap_or(summary);
    format!(
        "Checks: ✓ write-check fixes (fixed, {short}; {} file{}) · {verdicts}",
        paths.len(),
        if paths.len() == 1 { "" } else { "s" }
    )
}

fn delta_patch_excerpt(repo: &Path, delta: &crate::fleet::MutationDelta) -> String {
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
fn fixer_first_submission_order(
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
fn fix_is_attributed_to_declared_fixers(paths: &[String], fixers: &[&InputSelector]) -> bool {
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
fn verdict_survives_fix(
    executed: bool,
    fix_attributed: bool,
    superseded_by_later_fixer: bool,
    key_before: &str,
    key_after: &str,
) -> bool {
    key_before == key_after || (fix_attributed && executed && !superseded_by_later_fixer)
}

/// The submitted plan indices whose declared fix a LATER declared fixer could
/// have overwritten — every fixer in the wave except the last one to run.
///
/// `order` is the wave's submission order, so the fixers form its prefix and the
/// last of them is the one that observed all the others. A non-fixer is never
/// here: it runs after the whole fixer prefix.
fn fixers_superseded_by_a_later_fixer(
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

fn split_write_check_batch_outcome(
    outcome: CellOutcome,
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
                            let mut output = item.body;
                            append_sandbox_denial_evidence(&mut output, &item.sandbox_denials);
                            append_tracked_modification_evidence(
                                &mut output,
                                item.tracked_modifications.as_ref(),
                            );
                            let mut provenance = metadata.clone();
                            provenance.started_at_unix_ms = item.started_at_unix_ms;
                            provenance.finished_at_unix_ms = item.finished_at_unix_ms;
                            provenance.duration_ms = Some(item.duration_ms);
                            provenance.peak_rss_bytes = item.peak_rss_bytes;
                            provenance.disk_delta_bytes = item.disk_delta_bytes;
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
            let failure = check_result_from_cell_outcome(other, None)
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

fn batch_failure_results(
    expected: usize,
    failure: CheckExecutionFailure,
) -> (
    Vec<Result<CheckExecResult, CheckExecutionFailure>>,
    Option<crate::fleet::MutationDelta>,
) {
    ((0..expected).map(|_| Err(failure.clone())).collect(), None)
}

/// Stable cost identity for a configured check. Unlike [`check_result_key`],
/// this deliberately excludes the tree: verdict validity is tree-sensitive,
/// while the resources consumed by the same configured command transcend trees.
pub(crate) fn check_resource_identity(name: &str, check: &CheckCommand) -> CommandResourceIdentity {
    use sha2::{Digest, Sha256};

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
    // Where a check runs is part of what its verdict means, so two checks that
    // differ only in the machine they demand must not share a cost identity.
    if let Some(selector) = check.executor.as_ref() {
        field(&mut hasher, "executor");
        option(&mut hasher, selector.name.as_deref());
        option(&mut hasher, selector.os.as_deref());
        strings(&mut hasher, Some(&selector.required_toolchains));
    } else {
        field(&mut hasher, "no-executor");
    }
    CommandResourceIdentity {
        version: cairn_common::executor_protocol::COMMAND_RESOURCE_IDENTITY_VERSION,
        key: format!("{:x}", hasher.finalize()),
    }
}

pub(crate) async fn resolve_check_repository(
    orch: &Orchestrator,
    project_id: &str,
    job_id: &str,
    _residence: &Path,
) -> Result<(String, std::path::PathBuf), String> {
    let db = crate::execution::routing::owning_db_for_job(&orch.db, job_id)
        .await
        .map_err(|error| error.to_string())?;
    let job_id = job_id.to_string();
    let (resolved_project, repo_path) = db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn.query(
                "SELECT j.project_id, p.repo_path FROM jobs j JOIN projects p ON p.id = j.project_id WHERE j.id = ?1",
                (job_id.as_str(),),
            ).await?;
            let row = rows.next().await?.ok_or_else(|| crate::storage::DbError::Row(format!("check job {job_id} was not found")))?;
            Ok((row.text(0)?, row.text(1)?))
        })
    }).await.map_err(|error| error.to_string())?;
    if resolved_project != project_id {
        return Err(format!("check dispatch project mismatch: request names {project_id}, job names {resolved_project}"));
    }
    let repo = std::path::PathBuf::from(repo_path);
    let store = crate::jj::project_store_dir(&orch.config_dir, &repo);
    Ok((repo.to_string_lossy().into_owned(), store))
}

struct TemporaryCheckRef {
    orch: Orchestrator,
    repository: std::path::PathBuf,
    store_dir: std::path::PathBuf,
    reference: String,
    commit: String,
    armed: bool,
}

impl std::fmt::Debug for TemporaryCheckRef {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TemporaryCheckRef")
            .field("repository", &self.repository)
            .field("store_dir", &self.store_dir)
            .field("reference", &self.reference)
            .field("commit", &self.commit)
            .field("armed", &self.armed)
            .finish_non_exhaustive()
    }
}

impl TemporaryCheckRef {
    fn delete_locked(&mut self) -> Result<(), String> {
        git_check_output(
            &self.repository,
            &["update-ref", "-d", &self.reference, &self.commit],
            "delete sealed check commit reference",
        )?;
        self.armed = false;
        Ok(())
    }
}

impl Drop for TemporaryCheckRef {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.armed = false;
        let orch = self.orch.clone();
        let repository = self.repository.clone();
        let store_dir = self.store_dir.clone();
        let reference = self.reference.clone();
        let commit = self.commit.clone();
        let cleanup = async move {
            loop {
                let _guard = orch
                    .acquire_jj_store_lock(
                        &store_dir,
                        "remove abandoned sealed check commit reference",
                    )
                    .await;
                match git_check_output(
                    &repository,
                    &["update-ref", "-d", &reference, &commit],
                    "delete abandoned sealed check commit reference",
                ) {
                    Ok(_) => return,
                    Err(error) => log::warn!(
                        "temporary check ref cleanup will retry: reference={}, commit={}, error={}",
                        reference,
                        commit,
                        error
                    ),
                }
                drop(_guard);
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        };
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(cleanup);
        } else {
            std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("temporary check ref cleanup runtime");
                runtime.block_on(cleanup);
            });
        }
    }
}

fn git_check_output(repository: &Path, args: &[&str], context: &str) -> Result<String, String> {
    let output = std::process::Command::new("git")
        .args(args)
        .current_dir(repository)
        .output()
        .map_err(|error| format!("{context}: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "{context}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

async fn publish_check_commit_ref(
    orch: &Orchestrator,
    repository: &Path,
    store_dir: &Path,
    commit: &str,
    request_id: &str,
) -> Result<TemporaryCheckRef, String> {
    publish_check_commit_ref_with_verifier(
        orch,
        repository,
        store_dir,
        commit,
        request_id,
        |repository, reference| {
            git_check_output(
                repository,
                &["rev-parse", "--verify", &format!("{reference}^{{commit}}")],
                "verify sealed check commit reference",
            )
        },
    )
    .await
}

async fn publish_check_commit_ref_with_verifier<F>(
    orch: &Orchestrator,
    repository: &Path,
    store_dir: &Path,
    commit: &str,
    request_id: &str,
    verify: F,
) -> Result<TemporaryCheckRef, String>
where
    F: FnOnce(&Path, &str) -> Result<String, String>,
{
    let _guard = orch
        .acquire_jj_store_lock_with_timeout(
            store_dir,
            "publish sealed check commit",
            Some(std::time::Duration::from_secs(600)),
        )
        .await
        .map_err(|_| "timed out acquiring the managed store lock for check dispatch".to_string())?;
    git_check_output(
        repository,
        &["cat-file", "-e", &format!("{commit}^{{commit}}")],
        "verify sealed check commit in the managed Git backend",
    )?;
    let reference = format!("refs/cairn/checks/{request_id}");
    let absent = "0".repeat(commit.len());
    git_check_output(
        repository,
        &["update-ref", &reference, commit, &absent],
        "publish sealed check commit reference",
    )?;
    // Arm the cleanup obligation immediately after publication. Every later
    // error path either deletes under this held store lock or transfers the
    // obligation to Drop's lock-aware retry task.
    let mut temporary_ref = TemporaryCheckRef {
        orch: orch.clone(),
        repository: repository.to_path_buf(),
        store_dir: store_dir.to_path_buf(),
        reference,
        commit: commit.to_string(),
        armed: true,
    };
    let resolved = match verify(repository, &temporary_ref.reference) {
        Ok(resolved) => resolved,
        Err(error) => {
            return match temporary_ref.delete_locked() {
                Ok(()) => Err(error),
                Err(cleanup_error) => Err(format!(
                    "{error}; temporary check ref cleanup failed and was scheduled for retry: {cleanup_error}"
                )),
            };
        }
    };
    if resolved != commit {
        let error =
            format!("sealed check commit reference resolved to {resolved}, expected {commit}");
        return match temporary_ref.delete_locked() {
            Ok(()) => Err(error),
            Err(cleanup_error) => Err(format!(
                "{error}; temporary check ref cleanup failed and was scheduled for retry: {cleanup_error}"
            )),
        };
    }
    Ok(temporary_ref)
}

async fn cleanup_check_commit_ref(
    orch: &Orchestrator,
    store_dir: &Path,
    temporary_ref: &mut TemporaryCheckRef,
) -> Result<(), String> {
    let _guard = orch
        .acquire_jj_store_lock_with_timeout(
            store_dir,
            "remove sealed check commit reference",
            Some(std::time::Duration::from_secs(600)),
        )
        .await
        .map_err(|_| "timed out acquiring the managed store lock for check cleanup".to_string())?;
    temporary_ref.delete_locked()
}

fn merge_batch_executor(
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
    use sha2::{Digest, Sha256};

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
    let store_dir = crate::jj::project_store_dir(&orch.config_dir, &repository_path);
    let coordinate_repository = if crate::jj::is_jj_dir(&store_dir) {
        store_dir.clone()
    } else {
        repository_path.clone()
    };
    let coordinate = cairn_vcs::resolve_coordinate(&coordinate_repository, &branch)
        .await
        .map_err(|error| format!("checkpoint branch '{branch}' is unresolvable: {error}"))?;

    let mut hasher = Sha256::new();
    hasher.update(name.as_bytes());
    hasher.update([0]);
    hasher.update(command.as_bytes());
    let identity = format!("{:x}", hasher.finalize());
    let fleet = crate::config::settings::load_fleet(&orch.config_dir);
    let outcome = submit_planned_check_batch(
        orch,
        PlannedCheckBatchRequest {
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
            affinity_key: Some(job_id.to_string()),
            priority: CellPriority::ReviewCheck,
            // No PATH: a check resolves its tools against the PATH the executor
            // composed on the machine that runs it, the same as every other
            // check batch. Stating this host's PATH here would name directories
            // that exist on the runner and nowhere else.
            env: Vec::new(),
            items: vec![PlannedCheckBatchItem {
                index: 0,
                name: name.to_string(),
                input_hash: coordinate.clone(),
                resource_identity_key: identity,
                command: command.to_string(),
                stream_id: format!("checkpoint:{job_id}"),
                env: Vec::new(),
                timeout_ms: fleet
                    .default_timeout_seconds
                    .saturating_mul(1_000)
                    .min(u32::MAX as u64) as u32,
                executor: None,
                resource_class: CheckResourceClass::Shared,
            }],
            run_context: None,
            mutation_policy: MutationPolicy::PureVerdict,
            status_board: None,
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
            // rather than reporting a verdict the command never produced.
            CheckExecutionFailure::Suppressed => suppressed_check_message(
                crate::execution::cache::OBSERVED_INFRA_FAILURE_BOUND,
                "see the operator log for this check's last substrate diagnostic",
            ),
        })?;
    Ok(JobVerdictResult {
        coordinate,
        exit_code: result.exit_code,
        output: result.output,
        timed_out: result.timed_out,
    })
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
    for item in items {
        match claim_check_execution(db.clone(), project_id, &item.name, &item.input_hash) {
            Ok(CheckExecutionClaim::Suppressed) => {
                refused.insert(item.index, Err(CheckExecutionFailure::Suppressed));
            }
            // Fail open. A reservation that could not be read must not become one
            // more way for a check to go unmeasured: the bound exists to end a
            // loop, not to start a different outage.
            _ => admitted.push(item),
        }
    }
    (admitted, refused)
}

/// Extra immediate attempts a capacity refusal earns before it is allowed to
/// become an agent-visible infrastructure result.
///
/// A verdictless red check costs an agent a wake, a re-read, and a re-run of the
/// whole suite, so a momentary contention spike is worth a second and third ask
/// before it is reported as a failure of anything.
const CAPACITY_RETRY_ATTEMPTS: usize = 2;
/// Pause before each retry, so a retry does not simply re-ask a host that is
/// still finishing whatever displaced it.
const CAPACITY_RETRY_BACKOFF_MS: [u64; CAPACITY_RETRY_ATTEMPTS] = [2_000, 5_000];

/// The least patience any check batch declares, whatever else it knows.
///
/// Below a minute a lane cannot outlast even a short contention spike, which is
/// the one thing every retry policy here exists to absorb.
const CHECK_PATIENCE_FLOOR_MS: u64 = 60_000;

/// The most a write-cadence batch waits on load Cairn cannot account for.
///
/// A write-cadence verdict is appended to the tool result of the commit that
/// triggered it, so an agent's turn is stopped for the whole of this wait. That
/// makes the ceiling a statement about an agent's time: past it, a red
/// infrastructure row that re-runs next cadence costs the session less than
/// continuing to hold its turn open for a machine nothing here can reason about.
const WRITE_CADENCE_FOREIGN_CEILING_MS: u64 = 3 * 60_000;

/// The most a review-cadence batch waits on load Cairn cannot account for.
///
/// Nothing is blocked on a turn-end wave: it runs after the turn, it is
/// cancelled outright when its issue resolves, and a verdict that lands late is
/// still the verdict. So its ceiling is set by how long a result stays worth
/// having rather than by who is waiting, and on a fleet where a whole-workspace
/// suite runs for minutes, ten of them is one suite's worth of queueing.
const REVIEW_CADENCE_FOREIGN_CEILING_MS: u64 = 10 * 60_000;

/// Margin added to a predicted relief time before it becomes a bound.
///
/// The prediction says when the occupant finishes; it does not cover the
/// executor noticing, this request winning the next admission pass, and the
/// cell being handed the command. Without the margin a correct prediction would
/// still surface a refusal moments before the room it predicted opened.
const PREDICTION_MARGIN_MS: u64 = 30_000;

/// The two relations the clamp in [`CheckPatience::declare`] depends on, held
/// where they can be broken rather than where they would be noticed. A floor
/// above a ceiling inverts `clamp` (which panics), and a write ceiling above the
/// review one would mean the cadence that holds an agent's turn open is the
/// patient one — both are facts about these constants, so they are checked when
/// the constants are compiled.
const _: () = assert!(
    CHECK_PATIENCE_FLOOR_MS <= WRITE_CADENCE_FOREIGN_CEILING_MS,
    "the floor must fit inside every ceiling or a wait is over before it begins"
);
const _: () = assert!(
    WRITE_CADENCE_FOREIGN_CEILING_MS < REVIEW_CADENCE_FOREIGN_CEILING_MS,
    "holding an agent's turn open is the more expensive wait"
);

fn foreign_ceiling_ms(priority: &CellPriority) -> u64 {
    match priority {
        // Both of these hold something open while they wait. Interactive work
        // has an agent inside it by definition, and no check batch submits at
        // this priority today -- naming it here keeps a future one from
        // inheriting the ceiling meant for work nobody is sitting on.
        CellPriority::WriteCheck | CellPriority::AgentInteractive => {
            WRITE_CADENCE_FOREIGN_CEILING_MS
        }
        CellPriority::ReviewCheck => REVIEW_CADENCE_FOREIGN_CEILING_MS,
    }
}

/// How long this batch will wait for capacity in total, and why that long.
///
/// One budget, declared once and spent across every presentation of the same
/// request, so the added latency an agent can meet is the budget rather than
/// the budget times the attempt count. Where it comes from is the point:
///
/// - When Cairn's own placed work is what holds the machine, the budget is that
///   work's predicted remaining time. A bound taken from measurement is the
///   only kind that can say "this should have been enough" when it is not, and
///   it is why a lane no longer abandons a machine it could have named the
///   occupant of (CAIRN-3429).
/// - Otherwise the budget is the cadence's ceiling. A machine held by an
///   operator's own build, a dev harness, or work with no measured duration is
///   one nothing here can predict, and the honest answer is to wait a bounded
///   while and then say so — which is the CAIRN-3345 floor, unchanged.
///
/// The fleet-wide default horizon is deliberately not consulted. It documents
/// itself as the answer for a caller with no tighter answer of its own, and a
/// check has one: a four-second formatter and an interactive REPL should not
/// share a number, and the day they did, one stale value took the whole check
/// fabric down at once.
struct CheckPatience {
    started: std::time::Instant,
    foreign_ceiling: std::time::Duration,
    mobility: PlacementMobility,
}

/// The wait ONE selector group declares, against the machines that group can
/// actually land on.
///
/// A batch is not one wait. Pure-verdict items partition by executor selector
/// and each group is presented separately, so a Linux-targeted group and an
/// unconstrained one are queued behind different work and are relieved at
/// different moments. Deriving one basis for the whole batch would hand a
/// targeted group a horizon sized by a machine it can never land on — and then
/// print the wrong occupant's name on the row that went red.
struct GroupWait {
    horizon_ms: u64,
    /// Whether the capacity this group is waiting on is held by Cairn's own
    /// measured work. A wait on Cairn does not expire; only a wait on load Cairn
    /// cannot account for does.
    self_inflicted: bool,
    description: String,
}

impl CheckPatience {
    fn declare(batch: &PlannedCheckBatchRequest) -> Self {
        Self {
            started: std::time::Instant::now(),
            foreign_ceiling: std::time::Duration::from_millis(foreign_ceiling_ms(&batch.priority)),
            mobility: batch_placement_mobility(&batch.mutation_policy),
        }
    }

    /// What remains of the ceiling that governs waiting on load Cairn cannot
    /// account for.
    ///
    /// Never zero. A presentation with no horizon is evicted from the queue the
    /// instant it arrives, which would spend an admission to learn nothing; the
    /// bound that ends that wait is [`Self::foreign_patience_spent`], checked
    /// before a retry.
    fn remaining_foreign_ceiling_ms(&self) -> u64 {
        let elapsed_ms = self.started.elapsed().as_millis().min(u64::MAX as u128) as u64;
        (self.foreign_ceiling.as_millis() as u64)
            .saturating_sub(elapsed_ms)
            .max(CAPACITY_RETRY_BACKOFF_MS[0])
    }

    fn foreign_patience_spent(&self) -> bool {
        self.started.elapsed() >= self.foreign_ceiling
    }

    /// Read what holds this group's eligible machines, and turn it into a
    /// horizon and a sentence.
    ///
    /// Taken fresh at each presentation rather than once at declaration: a retry
    /// exists precisely because the world may have changed, and a forecast from
    /// before the last wait would answer a question about a machine that no
    /// longer looks like that.
    fn for_group(
        &self,
        orch: &Orchestrator,
        batch: &PlannedCheckBatchRequest,
        selector: Option<&ExecutorSelector>,
    ) -> GroupWait {
        // The same repository locator the submission below states. Eligibility
        // turns on it -- a checkout that exists on only one machine cannot be
        // recreated elsewhere -- so a forecast that omitted it would be scoped
        // differently from the placement it is trying to predict.
        let repository = RepositoryLocator::ColocatedPath {
            project_id: batch.project_id.clone(),
            repository_id: batch.project_id.clone(),
            absolute_path: batch.repository.clone(),
        };
        GroupWait::from_occupancy(
            orch.fleet.occupancy_for(crate::fleet::PlacementScope {
                project_id: &batch.project_id,
                repository: &repository,
                selector,
                // Checks never pin: `require_colocated_population` may pin a
                // request later, and does so only for a batch that needs the
                // runner's ignored content -- which is already pinned here by
                // its mutation policy's mobility.
                pinned_executor_id: None,
                mobility: self.mobility,
            }),
            self.remaining_foreign_ceiling_ms(),
        )
    }
}

impl GroupWait {
    /// The wait a reading implies, separated from the fleet so the policy can be
    /// exercised against a stated occupancy rather than a live machine.
    fn from_occupancy(
        occupancy: crate::fleet::occupancy::MachineOccupancy,
        remaining_foreign_ceiling_ms: u64,
    ) -> Self {
        let crate::fleet::occupancy::MachineOccupancy::Predicted(forecast) = occupancy else {
            return Self {
                horizon_ms: remaining_foreign_ceiling_ms,
                self_inflicted: false,
                description: format!(
                    "the machines this check can use are held by work with no measured duration, so there is nothing to queue behind knowingly; waiting up to {}",
                    describe_ms(remaining_foreign_ceiling_ms)
                ),
            };
        };
        let others = match forecast.occupant_count.saturating_sub(1) {
            0 => String::new(),
            1 => ", behind 1 other cell".to_string(),
            more => format!(", behind {more} other cells"),
        };
        Self {
            // No ceiling. This presentation is sized to outlast the occupant it
            // is queued behind, and if that occupant is replaced by another the
            // next presentation is sized to outlast THAT one. The floor still
            // applies, because a horizon shorter than a minute is evicted before
            // the queue can do anything with it.
            horizon_ms: forecast
                .relief_ms
                .saturating_add(PREDICTION_MARGIN_MS)
                .max(CHECK_PATIENCE_FLOOR_MS),
            self_inflicted: true,
            description: format!(
                "queued behind {}, predicted to finish in {}{others}; holding this check's place until it frees",
                forecast.blocking,
                describe_ms(forecast.relief_ms),
            ),
        }
    }
}

fn describe_ms(value_ms: u64) -> String {
    let seconds = value_ms / 1_000;
    match (seconds / 60, seconds % 60) {
        (0, seconds) => format!("{seconds}s"),
        (minutes, 0) => format!("{minutes}m"),
        (minutes, seconds) => format!("{minutes}m{seconds}s"),
    }
}

pub(crate) async fn submit_planned_check_batch(
    orch: &Orchestrator,
    mut batch: PlannedCheckBatchRequest,
) -> Result<PlannedCheckBatchOutcome, String> {
    let (admitted, refused) = reserve_batch_items(
        orch.db.local.clone(),
        &batch.project_id,
        std::mem::take(&mut batch.items),
    );
    batch.items = admitted;
    // Nothing survived the reservation: launch no command and request no cell, so
    // a fully suppressed suite costs no build-slot admission whatsoever.
    if batch.items.is_empty() {
        return Ok(PlannedCheckBatchOutcome {
            results: refused,
            request: None,
            delta: None,
            store_dir: Some(batch.store_dir.clone()),
        });
    }
    // The reservation above is claimed ONCE and the retries below sit inside it,
    // so an immediate retry cannot spend the persisted per-check
    // infrastructure-failure budget. That budget remains the outer circuit
    // breaker across cadences; this loop is the inner one within a single ask.
    let patience = CheckPatience::declare(&batch);
    let names = batch
        .items
        .iter()
        .map(|item| item.name.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    let mut attempt = 0;
    let mut outcome = loop {
        let presented = submit_reserved_check_batch(orch, batch.clone(), &patience).await?;
        let CapacityRetry::Again { backoff } = capacity_retry_decision(attempt, &presented.outcome)
        else {
            break presented.outcome;
        };
        // The room was taken by Cairn's own finite work, so this check is queued
        // rather than failing. It keeps its place: no attempt is spent, no
        // ceiling is consulted, and the next presentation is sized against
        // whatever holds the machine then.
        //
        // Several agents working at once is what this fabric is FOR, so the
        // contention it creates in normal operation must not be able to produce
        // a red row about someone's change. A check that reports failure because
        // it grew tired of waiting for Cairn is stating something false about
        // the diff under test, and the cost lands on a merge.
        //
        // This does not wait forever on a wedged machine, and the reason is in
        // `fleet::occupancy` rather than here: an occupant that outlives its own
        // learned duration stops being explainable, the reading becomes
        // unforecastable, and the next presentation is bounded by the ceiling
        // below. Patience is extended only while the wait keeps being accounted
        // for.
        if presented.self_inflicted {
            log::info!(
                "check batch [{names}] is queued behind Cairn's own work; holding its place — {}",
                describe_capacity_waits(&presented.outcome)
            );
            tokio::time::sleep(backoff).await;
            continue;
        }
        // Nothing here can account for what holds the machine, so the wait is
        // bounded and the refusal is honest when it comes (CAIRN-3345).
        if patience.foreign_patience_spent() {
            log::info!(
                "check batch [{names}] found no capacity within its declared patience — {}",
                describe_capacity_waits(&presented.outcome)
            );
            break presented.outcome;
        }
        log::info!(
            "check batch [{names}] found no capacity; {}; retrying ({} of {CAPACITY_RETRY_ATTEMPTS})",
            describe_capacity_waits(&presented.outcome),
            attempt + 1,
        );
        // Cancellation stops this immediately: the caller holds this future, and
        // a resolved issue or an ended wave drops it mid-sleep rather than
        // waiting out a retry nobody wants any more.
        tokio::time::sleep(backoff).await;
        attempt += 1;
    };
    outcome.results.extend(refused);
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
fn attribute_capacity_wait(outcome: &mut PlannedCheckBatchOutcome, wait: &GroupWait) {
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
fn describe_capacity_waits(outcome: &PlannedCheckBatchOutcome) -> String {
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
enum CapacityRetry {
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
/// command ran are all re-presented unchanged, so none of them retry.
fn capacity_retry_decision(attempt: usize, outcome: &PlannedCheckBatchOutcome) -> CapacityRetry {
    let wholly_transient = !outcome.results.is_empty()
        && outcome.results.values().all(|result| {
            matches!(
                result,
                Err(CheckExecutionFailure::Substrate(failure)) if failure.shape().is_transient()
            )
        });
    match CAPACITY_RETRY_BACKOFF_MS.get(attempt) {
        Some(backoff) if wholly_transient => CapacityRetry::Again {
            backoff: std::time::Duration::from_millis(*backoff),
        },
        _ => CapacityRetry::Surface,
    }
}

/// One presentation of a batch, and whether what refused it was Cairn's own
/// work.
///
/// The second fact decides whether the batch queues or gives up, and it is a
/// property of THIS presentation rather than of the batch: a group refused by a
/// named sibling suite is queued, while the same batch refused by an operator's
/// own build is on a clock. A batch split across selectors is treated as queued
/// only if every group that was refused for capacity was refused by Cairn — one
/// group waiting on something nobody can account for puts the whole batch back
/// on the bounded path, because it is the batch that has to end somewhere.
struct PresentedBatch {
    outcome: PlannedCheckBatchOutcome,
    self_inflicted: bool,
}

/// Whether this group's outcome is a capacity refusal at all.
fn refused_for_capacity(outcome: &PlannedCheckBatchOutcome) -> bool {
    outcome.results.values().any(|result| {
        matches!(
            result,
            Err(CheckExecutionFailure::Substrate(failure))
                if failure.shape() == SubstrateFailureShape::Capacity
        )
    })
}

async fn submit_reserved_check_batch(
    orch: &Orchestrator,
    mut batch: PlannedCheckBatchRequest,
    patience: &CheckPatience,
) -> Result<PresentedBatch, String> {
    if batch.mutation_policy == MutationPolicy::PureVerdict {
        let mut groups = partition_check_items_by_executor(std::mem::take(&mut batch.items));
        if groups.len() > 1 {
            let mut combined = PlannedCheckBatchOutcome {
                results: HashMap::new(),
                request: None,
                delta: None,
                store_dir: Some(batch.store_dir.clone()),
            };
            let mut refused_groups = 0_usize;
            let mut queued_groups = 0_usize;
            for (selector, items) in groups {
                // Each group derives its own wait, from its own eligible
                // machines, at the moment it is presented. Groups also run one
                // after another, so each re-reads what is LEFT of the shared
                // ceiling -- otherwise a batch naming two executors could wait
                // twice as long as it declared, and a ten-machine one, ten
                // times.
                let wait = patience.for_group(orch, &batch, selector.as_ref());
                let mut outcome = submit_single_planned_check_batch(
                    orch,
                    PlannedCheckBatchRequest {
                        items,
                        ..batch.clone()
                    },
                    &wait,
                )
                .await?;
                attribute_capacity_wait(&mut outcome, &wait);
                if refused_for_capacity(&outcome) {
                    refused_groups += 1;
                    queued_groups += usize::from(wait.self_inflicted);
                }
                combined.results.extend(outcome.results);
            }
            return Ok(PresentedBatch {
                outcome: combined,
                self_inflicted: refused_groups > 0 && refused_groups == queued_groups,
            });
        }
        batch.items = groups.pop().map(|(_, items)| items).unwrap_or_default();
    }
    // One group, whose selector is whatever its items agree on. A conflict is a
    // configuration error the submission below reports; there is no forecast to
    // scope by a selector that cannot exist, so it reads as unconstrained here.
    let wait = patience.for_group(
        orch,
        &batch,
        merge_batch_executor(&batch.items).ok().flatten().as_ref(),
    );
    let mut outcome = submit_single_planned_check_batch(orch, batch, &wait).await?;
    attribute_capacity_wait(&mut outcome, &wait);
    Ok(PresentedBatch {
        self_inflicted: wait.self_inflicted && refused_for_capacity(&outcome),
        outcome,
    })
}

/// Whether a planned check batch is free for placement policy to move.
///
/// A pure-verdict batch is the one request class that genuinely is. It is
/// disposable, it publishes a verdict rather than a mutation, and its tree is
/// materialized from managed objects wherever it lands, so nothing about it is
/// tied to the runner's own machine. A write-cadence batch is the opposite on
/// every count: it mutates one shared working tree that later checks in the same
/// batch observe, and its delta has to come back.
///
/// Stated from the mutation policy, never inferred from the absence of a
/// selector. A group that named a platform is just as mobile within that
/// platform as an unconstrained one is across the fleet, and an agent's
/// untargeted `run` batch is not mobile at all -- which is why the two facts are
/// separate fields.
///
/// A batch that needs the runner's ignored project content is pinned back to the
/// colocated executor by `fleet::require_colocated_population`, which overrides
/// this and is the hard boundary rather than a preference.
fn batch_placement_mobility(policy: &MutationPolicy) -> PlacementMobility {
    match policy {
        MutationPolicy::PureVerdict => PlacementMobility::SpillEligible,
        MutationPolicy::AllowDelta => PlacementMobility::PinnedOrColocated,
    }
}

fn partition_check_items_by_executor(
    items: Vec<PlannedCheckBatchItem>,
) -> Vec<(Option<ExecutorSelector>, Vec<PlannedCheckBatchItem>)> {
    let mut groups: Vec<(Option<ExecutorSelector>, Vec<PlannedCheckBatchItem>)> = Vec::new();
    for item in items {
        let selector = item.executor.clone().filter(|value| !value.is_empty());
        if let Some((_, items)) = groups
            .iter_mut()
            .find(|(candidate, _)| candidate == &selector)
        {
            items.push(item);
        } else {
            groups.push((selector, vec![item]));
        }
    }
    groups
}

async fn submit_single_planned_check_batch(
    orch: &Orchestrator,
    batch: PlannedCheckBatchRequest,
    wait: &GroupWait,
) -> Result<PlannedCheckBatchOutcome, String> {
    let wait_horizon_ms = wait.horizon_ms;
    // Configuration conflicts are deterministic caller errors and must surface
    // before any transient infrastructure preflight can obscure them.
    let executor = merge_batch_executor(&batch.items)?;
    if let Some(failure) =
        active_build_service_failure(&orch.build_service_diagnostic_snapshot("sccache"))
    {
        return Ok(PlannedCheckBatchOutcome::failed(
            batch.items.iter().map(|item| item.index).collect(),
            SubstrateFailure::new(SubstrateFailureShape::Dispatch, failure)
                .implicating_build_service(),
        ));
    }
    let timeout_ms = batch
        .items
        .iter()
        .fold(0_u32, |sum, item| sum.saturating_add(item.timeout_ms));
    let request_id = uuid::Uuid::new_v4().to_string();
    let attempt_id = uuid::Uuid::new_v4().to_string();
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
    let command = batch
        .items
        .iter()
        .map(|item| item.name.as_str())
        .collect::<Vec<_>>()
        .join(" · ");
    let request = CellRequest {
        request_id,
        attempt_id,
        project_id: batch.project_id.clone(),
        repository: RepositoryLocator::ColocatedPath {
            project_id: batch.project_id.clone(),
            repository_id: batch.project_id.clone(),
            absolute_path: batch.repository,
        },
        base_commit: batch.sealed_commit,
        command_class: batch_command_class(&batch.items),
        command,
        owner: Some(batch.owner.clone()),
        cwd: String::new(),
        env: batch.env,
        priority: batch.priority,
        // A check yields to interactive work by priority and then waits its
        // turn. The horizon is the batch's OWN, stated by [`CheckPatience`] from
        // what holds the machine, rather than the fleet-wide default meant for
        // callers with no answer of their own. Past it the machine is genuinely
        // saturated, "no room for a check right now" is honest, and the cadence
        // that planned this check will plan it again. It is derived from NOW on
        // every presentation, so a retry re-enters admission with a live wait
        // rather than one that already expired.
        wait_horizon_unix_ms: unix_time_ms_for_checks().saturating_add(wait_horizon_ms),
        waiting_since_unix_ms: unix_time_ms_for_checks(),
        timeout_ms,
        mutation_policy: batch.mutation_policy.clone(),
        requesting_job_id: Some(batch.requesting_job_id),
        affinity_key: batch.affinity_key,
        executor,
        pinned_executor_id: None,
        placement_mobility: batch_placement_mobility(&batch.mutation_policy),
        command_resource_identity: None,
        resource_reservation: declared_batch_reservation(&batch.items),
        learned_estimate: None,
    };
    let indexed: Vec<_> = batch.items.iter().map(|item| item.index).collect();
    let items = batch
        .items
        .into_iter()
        .map(|item| PureVerdictBatchItem {
            result_identity: crate::execution::cache::CheckResultIdentity::new(
                &batch.project_id,
                &item.name,
                &item.input_hash,
            ),
            process: ProcessBatchItem {
                header: item.name,
                stream_id: item.stream_id,
                execution: ProcessBatchExecution::Direct,
                program: "bash".into(),
                args: vec!["-c".into(), item.command],
                env: item.env,
                stdin: None,
                timeout_ms: item.timeout_ms,
                command_resource_identity: Some(CommandResourceIdentity {
                    version: cairn_common::executor_protocol::COMMAND_RESOURCE_IDENTITY_VERSION,
                    key: item.resource_identity_key,
                }),
            },
        })
        .collect();
    let mutation_policy = request.mutation_policy.clone();
    let submitted = if mutation_policy == MutationPolicy::PureVerdict {
        orch.fleet
            .submit_pure_verdict_batch(orch, request.clone(), items, batch.run_context)
            .await
    } else {
        let processes = items.into_iter().map(|item| item.process).collect();
        // The queued row says what it is queued BEHIND. "Waiting for build
        // slot" told an operator watching their own fleet only that Cairn was
        // waiting on Cairn, which is the one thing they could already see.
        if let Some(board) = &batch.status_board {
            board.set_phase(Some("queued"), Some(wait.description.clone()));
        }
        let outcome = orch
            .fleet
            .submit_write_check_batch(
                orch,
                request.clone(),
                processes,
                batch.run_context,
                batch.status_board.clone(),
            )
            .await;
        let (item_outcomes, delta) = split_write_check_batch_outcome(outcome, indexed.len());
        let results = indexed.iter().copied().zip(item_outcomes).collect();
        if let Err(error) =
            cleanup_check_commit_ref(orch, &batch.store_dir, &mut temporary_ref).await
        {
            return Ok(PlannedCheckBatchOutcome::failed(
                indexed,
                SubstrateFailure::new(SubstrateFailureShape::Result, error),
            ));
        }
        return Ok(PlannedCheckBatchOutcome {
            results,
            request: Some(request),
            delta,
            store_dir: Some(batch.store_dir),
        });
    };
    if let Err(error) = cleanup_check_commit_ref(orch, &batch.store_dir, &mut temporary_ref).await {
        return Ok(PlannedCheckBatchOutcome::failed(
            indexed,
            SubstrateFailure::new(SubstrateFailureShape::Result, error),
        ));
    }
    let results = indexed
        .into_iter()
        .zip(submitted)
        .map(|(index, submitted)| {
            let result = match submitted {
                Ok(submitted) => {
                    check_result_from_cell_outcome(submitted.outcome, Some(submitted.publication))
                }
                Err(outcome) => check_result_from_cell_outcome(outcome, None),
            };
            (index, result)
        })
        .collect();
    Ok(PlannedCheckBatchOutcome {
        results,
        request: Some(request),
        delta: None,
        store_dir: Some(batch.store_dir),
    })
}

#[derive(Clone)]
pub(crate) struct PlannedCheckBatchItem {
    pub index: usize,
    pub name: String,
    pub input_hash: String,
    pub resource_identity_key: String,
    pub command: String,
    pub stream_id: String,
    pub env: Vec<(String, String)>,
    pub timeout_ms: u32,
    pub executor: Option<ExecutorSelector>,
    /// The project's declaration of whether this check can co-run with others.
    /// Carried onto the submission so the scheduler's budget sees an exclusive
    /// lane as the whole-machine work it is.
    pub resource_class: CheckResourceClass,
}

/// What a check batch's cell will actually fan out to.
///
/// This is DEMAND, not enforcement. The check system is the only party that
/// knows what it is asking for; capping an admitted cell's internal parallelism
/// and yielding to interactive work belong to the executor (CAIRN-3248). A batch
/// that declares honestly cannot overrun a budget by itself — it just lets the
/// scheduler plan against the work rather than against the request count.
///
/// Concurrency is STATED by the project, never inferred from the command text.
/// A tool that parallelizes internally — cargo across the crate graph, vitest
/// across workers, a bundler across modules — uses the cores that happen to be
/// free when it runs; it does not require them. Charging that opportunistic
/// parallelism as an admission reservation makes every ordinary build claim the
/// whole host, which is exactly how a 16-core machine came to report `17 of 16`
/// concurrency units reserved at ~31% utilization while five-second checks died
/// at their acquisition deadlines (CAIRN-3345). Only
/// [`CheckResourceClass::Exclusive`] — the project's own statement that a check
/// needs a quiet machine — reserves the whole executor.
///
/// The command still classifies the work ([`batch_command_class`]) for the
/// per-class memory, disk, and duration profiles, which is a different question:
/// how much a run costs, not how many lanes it must be handed.
///
/// Memory and disk are deliberately left at zero: those are learned per command
/// identity from observed runs, and a declaration here would suppress a better
/// estimate than any submitter could write down.
pub(crate) fn declared_check_reservation(
    resource_class: CheckResourceClass,
) -> ResourceReservation {
    ResourceReservation {
        memory_bytes: 0,
        disk_growth_bytes: 0,
        concurrency_units: match resource_class {
            CheckResourceClass::Exclusive => ResourceReservation::WHOLE_MACHINE_CONCURRENCY,
            CheckResourceClass::Shared => 1,
        },
        source: ResourceReservationSource::Declared,
    }
}

/// Demand for a batch that runs several checks in one cell: as heavy as the
/// heaviest DECLARED class among its items, since they share the cell.
pub(crate) fn declared_batch_reservation(items: &[PlannedCheckBatchItem]) -> ResourceReservation {
    let heaviest = items.iter().map(|item| item.resource_class).fold(
        CheckResourceClass::Shared,
        |heaviest, class| match (heaviest, class) {
            (CheckResourceClass::Exclusive, _) | (_, CheckResourceClass::Exclusive) => {
                CheckResourceClass::Exclusive
            }
            _ => CheckResourceClass::Shared,
        },
    );
    declared_check_reservation(heaviest)
}

/// The command class of a batch: the heaviest class among its items.
///
/// A batch runs every item in one cell, so the cell is as heavy as the heaviest
/// thing in it. This used to classify the batch's DISPLAY string — a join of
/// check *names* like `rust-lint · rust-full` — which matches none of the
/// command patterns, so every batch reported `other` and the learned resource
/// profiles keyed off a class that never described the work.
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

#[derive(Clone)]
pub(crate) struct PlannedCheckBatchRequest {
    pub project_id: String,
    pub repository: String,
    pub store_dir: std::path::PathBuf,
    pub sealed_commit: String,
    pub requesting_job_id: String,
    pub owner: cairn_common::executor_protocol::CellOwnerRef,
    pub affinity_key: Option<String>,
    pub priority: CellPriority,
    pub env: Vec<(String, String)>,
    pub items: Vec<PlannedCheckBatchItem>,
    pub run_context: Option<RunContext>,
    pub mutation_policy: MutationPolicy,
    pub status_board: Option<CheckStatusBoard>,
}

pub(crate) struct PlannedCheckBatchOutcome {
    pub results: HashMap<usize, Result<CheckExecResult, CheckExecutionFailure>>,
    pub request: Option<CellRequest>,
    pub delta: Option<crate::fleet::MutationDelta>,
    pub store_dir: Option<std::path::PathBuf>,
}

impl PlannedCheckBatchOutcome {
    fn failed(indices: Vec<usize>, failure: SubstrateFailure) -> Self {
        Self {
            results: indices
                .into_iter()
                .map(|index| {
                    (
                        index,
                        Err(CheckExecutionFailure::Substrate(failure.clone())),
                    )
                })
                .collect(),
            request: None,
            delta: None,
            store_dir: None,
        }
    }
}

/// What Cairn's own machinery failed to do, in the only terms an agent can act
/// on. Every arm answers one question — why is there no verdict? — without
/// naming a slot, a cell, a scratch path, or an executor outcome variant. Those
/// coordinates are real and they matter, but they address a substrate the agent
/// has no standing inside: the operator log is where they belong.
/// The no-start conditions an agent can meet, kept apart because they call for
/// different responses. "It did not start" is not one condition: a machine that
/// is momentarily full will take the same check in a minute, a fleet with no
/// machine for its toolchain never will, and work whose own environment was torn
/// down is not a fault at all. Collapsing them into one lead sentence forced
/// every reader to the operator log to learn which had happened (CAIRN-3345).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SubstrateFailureShape {
    /// Cairn never got the check started, for a reason that is Cairn's own.
    Dispatch,
    /// Execution capacity never became available before the deadline. The one
    /// condition time alone relieves, and so the only one worth retrying.
    Capacity,
    /// Contact with the machine that would have run the check was lost.
    MachineUnreachable,
    /// The machine is being taken out of service and accepts no new work.
    Draining,
    /// Nothing in the fleet matches what this check requires.
    NoMachine,
    /// A working environment for the check could not be prepared.
    Preparation,
    /// The working environment the check was given could not be made fit for
    /// it, and was taken out of service for that. Distinct from `Preparation`
    /// because the next attempt gets a different environment rather than the
    /// same broken one.
    EnvironmentRetired,
    /// The command ran, but Cairn could not obtain or record its result.
    Result,
    /// Cairn's own storage for the check failed.
    Storage,
    /// Cairn cancelled the check before it produced a verdict.
    Cancelled,
}

impl SubstrateFailureShape {
    fn lead(self) -> &'static str {
        match self {
            Self::Dispatch => "Cairn could not start this check.",
            Self::Capacity => {
                "Cairn could not obtain the capacity to run this check before its deadline."
            }
            Self::MachineUnreachable => {
                "Cairn lost contact with the machine that would have run this check."
            }
            Self::Draining => {
                "The machine that runs checks is being taken out of service, so this check did not start."
            }
            Self::NoMachine => {
                "No machine available to Cairn can run this check's platform or toolchain."
            }
            Self::Preparation => {
                "Cairn could not prepare a working environment for this check."
            }
            Self::EnvironmentRetired => {
                "The working environment this check was given could not be made ready, so Cairn took it out of service."
            }
            Self::Result => "This check ran, but Cairn could not record its result.",
            Self::Storage => "Cairn's own storage for this check failed.",
            Self::Cancelled => "Cairn cancelled this check before it produced a verdict.",
        }
    }

    /// Whether presenting this work again could plausibly change the answer.
    ///
    /// Two conditions qualify, and both because something outside the check
    /// changes between attempts. Capacity is relieved by time passing. A retired
    /// environment is relieved by the retirement itself: the environment that
    /// could not take the check is gone, so the next attempt is handed a
    /// different one. A structural refusal (no matching machine, a toolchain
    /// fault, a draining host), a cancellation, and anything that happened after
    /// the command ran are all re-presented unchanged, so retrying them would
    /// only spend the machine twice for the same answer.
    fn is_transient(self) -> bool {
        matches!(self, Self::Capacity | Self::EnvironmentRetired)
    }
}

/// The condition class behind an executor's refusal to start a cell.
///
/// Exhaustive over [`CellUnavailableReason`] on purpose: a new reason cannot be
/// added upstream without landing here, which is what keeps a novel refusal from
/// silently arriving at an agent as the generic "could not start this check".
/// The sibling classifier in [`crate::fleet::placement`] answers a different
/// question about the same input — whether the fleet should keep waiting — and
/// the two are deliberately separate: what the fleet does next and what the
/// agent is told are not the same decision.
fn no_start_shape(
    reason: &cairn_common::executor_protocol::CellUnavailableReason,
) -> SubstrateFailureShape {
    use cairn_common::executor_protocol::{AdmissionRejectionReason, CellUnavailableReason};
    match reason {
        CellUnavailableReason::Deadline {
            host_pressure,
            substrate,
        } => deadline_shape(host_pressure.as_ref(), substrate.as_ref()),
        CellUnavailableReason::AdmissionRejected { reason } => match reason {
            AdmissionRejectionReason::QueueFull => SubstrateFailureShape::Capacity,
            AdmissionRejectionReason::Draining => SubstrateFailureShape::Draining,
            AdmissionRejectionReason::StorageCleanupFailed => SubstrateFailureShape::Storage,
            // A reservation larger than the machine's whole budget is not a
            // wait: no amount of idling makes the host bigger.
            AdmissionRejectionReason::RequestTooLarge => SubstrateFailureShape::NoMachine,
        },
        CellUnavailableReason::ExecutorUnavailable => SubstrateFailureShape::MachineUnreachable,
        CellUnavailableReason::NoMatchingExecutor => SubstrateFailureShape::NoMachine,
        CellUnavailableReason::Provisioning
        | CellUnavailableReason::Checkout
        | CellUnavailableReason::Preparation => SubstrateFailureShape::Preparation,
        CellUnavailableReason::SlotUnhealthy => SubstrateFailureShape::EnvironmentRetired,
        // The environment was ready and the command still could not be launched,
        // which is Cairn's own machinery failing rather than the machine's state.
        CellUnavailableReason::Spawn | CellUnavailableReason::ObjectInfrastructure(_) => {
            SubstrateFailureShape::Dispatch
        }
    }
}

/// What an elapsed acquisition deadline actually says happened.
///
/// A deadline is not a condition of its own: it is the moment a wait ended, and
/// what the wait was ON is in the executor's evidence. `CapacityBusy` behind a
/// queue is a machine doing its job; `ConnectedStalled`, a draining host, a disk
/// below its floor, or no evidence at all are machines that were never going to
/// get to this request. Telling an agent "could not obtain capacity" for the
/// second group — and, worse, asking twice more before saying it — is the
/// condition-class collapse this whole mapping exists to remove.
///
/// The wait/refuse split itself is NOT re-decided here: it is
/// [`crate::fleet::placement`]'s, read through the same two predicates that
/// module uses, so retry eligibility here and the fleet's own waiting decision
/// cannot drift apart. This function only names which condition a refusal was.
fn deadline_shape(
    host_pressure: Option<&cairn_common::executor_protocol::HostPressureEvidence>,
    substrate: Option<&cairn_common::executor_protocol::ExecutorSubstrateEvidence>,
) -> SubstrateFailureShape {
    use crate::fleet::placement::{pressure_relieves_itself, substrate_is_working};
    use cairn_common::executor_protocol::HostPressureCondition;
    let working = substrate.is_some_and(|evidence| substrate_is_working(evidence.state));
    let relieving = host_pressure.is_some_and(pressure_relieves_itself);
    if working || relieving {
        return SubstrateFailureShape::Capacity;
    }
    // The executor's own statement about itself outranks inferred pressure.
    if let Some(evidence) = substrate {
        return stalled_substrate_shape(evidence.state);
    }
    match host_pressure {
        // Disk below its floor is the one hold nothing running will relieve: the
        // bytes come back from an operator, not from the queue draining.
        Some(evidence)
            if evidence
                .conditions
                .iter()
                .any(|condition| matches!(condition, HostPressureCondition::DiskFree { .. })) =>
        {
            SubstrateFailureShape::Storage
        }
        // Silence. The machine said nothing about why the wait ended, which is
        // the same reading placement takes: one that stopped answering.
        _ => SubstrateFailureShape::MachineUnreachable,
    }
}

/// The condition a substrate state describes when it is not one the fleet waits
/// on. Exhaustive so a new executor state must be decided here rather than
/// inheriting whichever answer happens to be nearest.
fn stalled_substrate_shape(
    state: cairn_common::executor_protocol::ExecutorSubstrateState,
) -> SubstrateFailureShape {
    use cairn_common::executor_protocol::ExecutorSubstrateState as State;
    match state {
        // Refusing new work on purpose, usually on its way out.
        State::Draining => SubstrateFailureShape::Draining,
        // Connected and no longer reporting: the machine stopped answering.
        State::ConnectedStalled => SubstrateFailureShape::MachineUnreachable,
        // Running work already admitted, and every state the fleet counts as the
        // machine working. Those were answered as capacity above; naming them
        // keeps this match total.
        State::ExecutionRunning
        | State::SupervisorSpawning
        | State::SupervisorRespawning
        | State::ProtocolAttaching
        | State::InitialStorageSweep
        | State::StorageAccounting
        | State::DispatchPreparing
        | State::SlotAdoption
        | State::CapacityBusy => SubstrateFailureShape::Capacity,
    }
}

/// The closing half of every agent-facing infrastructure message: what the
/// failure means for the agent's own work, and where the rest of the story
/// lives. "Runs again" is a fact rather than a hope — an infrastructure verdict
/// is never reusable from the result cache, so the next cadence re-executes the
/// check (see [`crate::execution::cache::get_check_result`]).
const SUBSTRATE_FAILURE_CONSEQUENCE: &str = "This is a failure inside Cairn, not a result about your change: no verdict was recorded, and the check runs again the next time checks run. The full diagnostic is in Cairn's operator log.";

/// A check-infrastructure failure, composed into its two audiences at the seam
/// where an executor outcome becomes a check verdict.
///
/// The agent-facing half is authored here, at the source, rather than scrubbed
/// downstream: [`Self::agent_message`] is plain language an agent can act on.
/// [`Self::diagnostic`] keeps the substrate detail — outcome variants,
/// slot-absolute paths, queue evidence — and goes only to the operator log,
/// whose record already carries the check name, job id, and suite id that
/// correlate it back to the agent's verdict.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SubstrateFailure {
    shape: SubstrateFailureShape,
    diagnostic: String,
    build_service_implicated: bool,
    /// What the batch was waiting ON, when Cairn's own placement records say.
    /// Agent-facing on purpose: unlike a cell id or a queue position, the
    /// identity of the work holding the machine is a fact about the world that
    /// explains the row and predicts its recovery.
    waited_on: Option<String>,
}

impl SubstrateFailure {
    pub(crate) fn new(shape: SubstrateFailureShape, diagnostic: impl Into<String>) -> Self {
        Self {
            shape,
            diagnostic: diagnostic.into(),
            build_service_implicated: false,
            waited_on: None,
        }
    }

    /// Record what this failure spent its patience waiting on.
    pub(crate) fn name_the_wait(&mut self, waited_on: &str) {
        self.waited_on = Some(waited_on.to_string());
    }

    pub(crate) fn waited_on(&self) -> Option<&str> {
        self.waited_on.as_deref()
    }

    /// Mark this failure as one the shared build-cache service caused, so its
    /// verdict may carry the service's advisory. Applicability is a property of
    /// the failure, established where the failure is composed — never inferred
    /// downstream from whether the daemon happens to be sick at the time.
    pub(crate) fn implicating_build_service(mut self) -> Self {
        self.build_service_implicated = true;
        self
    }

    pub(crate) fn build_service_implicated(&self) -> bool {
        self.build_service_implicated
    }

    /// The authored text an agent reads in place of the substrate diagnostic.
    pub(crate) fn agent_message(&self) -> String {
        match &self.waited_on {
            Some(waited_on) => format!(
                "{} It was {waited_on}. {SUBSTRATE_FAILURE_CONSEQUENCE}",
                self.shape.lead()
            ),
            None => format!("{} {SUBSTRATE_FAILURE_CONSEQUENCE}", self.shape.lead()),
        }
    }

    /// The substrate diagnostic. Operator log only.
    pub(crate) fn diagnostic(&self) -> &str {
        &self.diagnostic
    }

    /// The condition class this failure preserves.
    pub(crate) fn shape(&self) -> SubstrateFailureShape {
        self.shape
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CheckExecutionFailure {
    /// The check's own process failed — output the agent owns and can act on.
    Process(String),
    /// Cairn's machinery failed before, around, or after the check.
    Substrate(SubstrateFailure),
    /// Submission declined to launch this command at all: the triple's bounded
    /// retries are spent. This is not a failure of anything — nothing ran — and
    /// it must never be classified, stored, or counted as one.
    Suppressed,
}

impl CheckExecutionFailure {
    pub(crate) fn substrate(shape: SubstrateFailureShape, diagnostic: impl Into<String>) -> Self {
        Self::Substrate(SubstrateFailure::new(shape, diagnostic))
    }
}

impl From<String> for CheckExecutionFailure {
    fn from(error: String) -> Self {
        Self::Process(error)
    }
}
use crate::execution::check_parsers::{
    extract_running_tests, format_failure_excerpt, format_failure_names, parse_check_output,
    ParsedCheckResult, MAX_FAILURE_NAMES,
};
use crate::execution::selection::{plan_checks, CheckPlan};
use crate::jj::{logical_changed_files, logical_tree_hash, tree_entries, GraphFileChange, JjEnv};
use crate::mcp::handlers::run::{CheckExecResult, CheckStatusEntry, CheckStatusPayload};
use crate::mcp::handlers::RunContext;
use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};

/// Default per-check time cap for the mid-turn `when:write` cadence. Its checks
/// are light (change-scoped test runs, a formatter, small consistency guards),
/// so 10 minutes is ample. A check may raise its own via the schema `timeout`.
const DEFAULT_WRITE_CHECK_TIMEOUT_MS: u32 = 600_000;
/// Default per-check time cap for the turn-end `when:review` cadence. Sized to
/// comfortably cover a COLD, uncached full Rust compile + ~1900 tests on this
/// hardware: observed *successful* `rust-full` runs already reach ~9.3 min, so
/// the prior hard 10-min ceiling guillotined healthy-but-slow suites (dozens of
/// rows killed at ~600s in this project's cache). An uncached cold build
/// (sccache down, CAIRN-2621) runs longer still, so 30 min gives ~3x headroom
/// over the slowest observed green. A check may override via the schema
/// `timeout` field.
pub(crate) const DEFAULT_REVIEW_CHECK_TIMEOUT_MS: u32 = 1_800_000;
/// Hard ceiling on a check's configured `timeout` (seconds → ms): a guardrail so
/// a config typo cannot wedge a check for hours. 60 minutes.
const MAX_CHECK_TIMEOUT_MS: u32 = 3_600_000;

/// Resolve one check's effective timeout in ms: its schema `timeout` (SECONDS,
/// clamped to [`MAX_CHECK_TIMEOUT_MS`]) when set, else the cadence default.
pub(crate) fn resolve_check_timeout_ms(check: Option<&CheckCommand>, default_ms: u32) -> u32 {
    match check.and_then(|c| c.timeout) {
        Some(secs) => secs.saturating_mul(1000).min(MAX_CHECK_TIMEOUT_MS),
        None => default_ms,
    }
}

/// Terminal classification refining a FAILING check's binary `passed = false`
/// verdict, so a timeout or a spawn failure renders AS itself instead of an
/// opaque `exit -1`. Persisted (snake_case) in `check_result_cache.failure_kind`;
/// `None`/absent means an ordinary failure (non-zero exit) or a pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckFailureKind {
    /// Killed at its timeout budget.
    TimedOut,
    /// The process could not be spawned (e.g. its cwd vanished mid-run).
    SpawnError,
    /// Died by signal mid-run without hitting the budget (crash / OOM kill).
    Killed,
    /// Positive evidence identifies a host, toolchain, or shared-service failure.
    Infrastructure,
    /// A recognized test runner exited abnormally without assertion failures.
    RunnerError,
}

impl CheckFailureKind {
    pub(crate) fn is_infrastructure(self) -> bool {
        matches!(
            self,
            CheckFailureKind::SpawnError
                | CheckFailureKind::Infrastructure
                | CheckFailureKind::RunnerError
        )
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            CheckFailureKind::TimedOut => "timed_out",
            CheckFailureKind::SpawnError => "spawn_error",
            CheckFailureKind::Killed => "killed",
            CheckFailureKind::Infrastructure => "infrastructure",
            CheckFailureKind::RunnerError => "runner_error",
        }
    }

    /// Parse a persisted `failure_kind` string back into the enum; `None` for a
    /// pass, an ordinary failure, or a legacy `NULL`. (Not `FromStr`: the
    /// absent/unknown case is an ordinary `None`, not a parse error.)
    pub(crate) fn from_stored(s: &str) -> Option<Self> {
        match s {
            "timed_out" => Some(CheckFailureKind::TimedOut),
            "spawn_error" => Some(CheckFailureKind::SpawnError),
            "killed" => Some(CheckFailureKind::Killed),
            "infrastructure" => Some(CheckFailureKind::Infrastructure),
            "runner_error" => Some(CheckFailureKind::RunnerError),
            _ => None,
        }
    }

    /// The human verdict fragment, given the run duration (used for the timeout
    /// budget it was killed at).
    pub(crate) fn describe(self, duration_ms: i64) -> String {
        match self {
            CheckFailureKind::TimedOut => {
                format!("timed out after {}", format_timeout_budget(duration_ms))
            }
            CheckFailureKind::SpawnError => "failed to spawn".to_string(),
            CheckFailureKind::Killed => "killed (signal)".to_string(),
            CheckFailureKind::Infrastructure => "infrastructure/toolchain failure".to_string(),
            CheckFailureKind::RunnerError => "test runner failed".to_string(),
        }
    }
}

/// Format a timeout budget compactly: whole minutes at or above a minute, else
/// seconds. `600_000` → `10m`, `1_800_000` → `30m`, `45_000` → `45s`.
fn format_timeout_budget(duration_ms: i64) -> String {
    if duration_ms >= 60_000 {
        format!("{}m", (duration_ms as f64 / 60_000.0).round() as i64)
    } else {
        format!("{}s", (duration_ms as f64 / 1000.0).round() as i64)
    }
}

/// Chars of combined check output retained in the cache row's `output_tail`.
const OUTPUT_TAIL_CHARS: usize = 4_000;

#[derive(Debug, Clone, PartialEq, Eq)]
struct FailureClassification {
    kind: CheckFailureKind,
    reason: String,
    evidence_line: Option<usize>,
}

/// Name the way a failing check DIED when the death is not an ordinary test
/// failure, so a red verdict never sends an agent hunting for a test that never
/// failed. `None` means "this is an ordinary failure": the parsed failures and
/// the exit code already explain it.
fn classify_check_failure(
    command: &str,
    exit_code: Option<i32>,
    timed_out: bool,
    spawn_error: bool,
    parsed: Option<&ParsedCheckResult>,
    output: &str,
) -> Option<FailureClassification> {
    if exit_code == Some(0) {
        return None;
    }
    if spawn_error {
        return Some(FailureClassification {
            kind: CheckFailureKind::SpawnError,
            reason: "Cairn: check process failed to spawn".to_string(),
            evidence_line: None,
        });
    }
    if timed_out {
        return Some(FailureClassification {
            kind: CheckFailureKind::TimedOut,
            reason: "Cairn: check exceeded its timeout budget".to_string(),
            evidence_line: None,
        });
    }
    if exit_code.is_none() {
        return Some(FailureClassification {
            kind: CheckFailureKind::Killed,
            reason: "Cairn: check process died without an exit code".to_string(),
            evidence_line: None,
        });
    }
    // Named failure sites — failing assertions, or files that failed to collect
    // — are an ordinary failure the detail path already renders.
    if parsed.is_some_and(|result| result.failed > 0 || result.suite_failures > 0) {
        return None;
    }

    let lines: Vec<&str> = output.lines().collect();
    let transport = lines.iter().position(|line| {
        let line = line.to_ascii_lowercase();
        line.contains("failed to send data to or receive data from server")
            || line.contains("failed client/server communication")
            || line.contains("failed to fill whole buffer")
            || line.contains("server looks like it shut down unexpectedly")
    });
    let abnormal_254 = (exit_code == Some(254))
        .then(|| {
            lines.iter().position(|line| {
                let line = line.to_ascii_lowercase();
                (line.contains("rustc") || line.contains("sccache"))
                    && (line.contains("exit status: 254")
                        || line.contains("exited with status 254"))
                    && (line.contains("process didn't exit successfully")
                        || line.contains("failed to execute compile")
                        || line.contains("compiler process"))
            })
        })
        .flatten();
    let missing_generated = lines.iter().position(|line| {
        let line = line.replace('\\', "/").to_ascii_lowercase();
        line.contains("target/")
            && line.contains("/build/")
            && line.contains("/out/")
            && (line.contains("couldn't read")
                || line.contains("failed to read")
                || line.contains("no such file or directory"))
    });
    if let Some(evidence_line) = transport.or(abnormal_254).or(missing_generated) {
        return Some(FailureClassification {
            kind: CheckFailureKind::Infrastructure,
            reason:
                "Cairn: infrastructure/toolchain failure matched reviewed abnormal-build evidence"
                    .to_string(),
            evidence_line: Some(evidence_line),
        });
    }

    if let Some(result) = parsed.filter(|result| result.parser == "vitest") {
        // Vitest exited nonzero having reported neither a failing test nor a
        // failing suite. The dominant cause is an error that escaped a test file
        // asynchronously: Vitest fails the run over it but attributes it to no
        // test, and its JSON `success` stays true. Point at that line so the tail
        // excerpt carries the stack rather than trailing render noise.
        let unhandled = lines
            .iter()
            .position(|line| line.to_ascii_lowercase().contains("unhandled error"));
        let mut reason = if result.passed == 0 {
            "Cairn: Vitest failed before reporting any test assertions".to_string()
        } else {
            format!(
                "Cairn: Vitest runner failed after {} tests passed with no assertion failures",
                result.passed
            )
        };
        if unhandled.is_some() {
            reason.push_str(
                " \u{2014} an error escaped a test file after it finished; Vitest fails the run \
                 without attributing it to any test",
            );
        }
        return Some(FailureClassification {
            kind: CheckFailureKind::RunnerError,
            reason,
            evidence_line: unhandled,
        });
    }

    // A Vitest command that exited nonzero having emitted no report at all never
    // reached a test: its config or its dependencies failed to load. Without this
    // arm the verdict is a bare `exit 1` beside a tail of resolver noise.
    if parsed.is_none() && crate::execution::check_parsers::is_vitest_command(command) {
        return Some(FailureClassification {
            kind: CheckFailureKind::RunnerError,
            reason: "Cairn: Vitest exited without producing a report \u{2014} the run failed \
                     before collecting any test file"
                .to_string(),
            evidence_line: None,
        });
    }
    None
}

fn classified_output_excerpt(
    output: &str,
    classification: Option<&FailureClassification>,
) -> String {
    let Some(classification) = classification else {
        return tail(output, OUTPUT_TAIL_CHARS);
    };
    let lines: Vec<&str> = output.lines().collect();
    let context = classification.evidence_line.map(|index| {
        let start = index.saturating_sub(2);
        let end = (index + 3).min(lines.len());
        lines[start..end].join("\n")
    });
    let mut prefix = classification.reason.clone();
    if let Some(context) = context.filter(|context| !context.is_empty()) {
        prefix.push_str("\nEvidence:\n");
        prefix.push_str(&context);
    }
    prefix.push_str("\n\nFinal output:\n");
    let remaining = OUTPUT_TAIL_CHARS.saturating_sub(prefix.chars().count());
    prefix.push_str(&tail(output, remaining));
    prefix.chars().take(OUTPUT_TAIL_CHARS).collect()
}

/// Cancel any in-flight `when:review` check suite for `job_id` when a commit
/// seals mid-turn. The branch just advanced, so that suite — launched at the
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
pub(crate) async fn cancel_stale_review_on_branch_advance(orch: &Orchestrator, job_id: &str) {
    for stale in jobs_sharing_branch(&orch.db.local, job_id).await {
        orch.cancel_turn_end_checks(&stale);
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
async fn jobs_sharing_branch(db: &LocalDb, job_id: &str) -> Vec<String> {
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

/// Run the affected `when:write` checks after a source-touching commit has been
/// sealed, streaming their output live and returning a compact inline pass/fail
/// summary to append to the originating tool result.
///
/// Returns `None` whenever nothing applied: no run context (so no streaming
/// target), no `checks` contract, no resolvable changed-file set, an empty
/// change set, or no `when:write` check whose impact the change set matches (a
/// doc-only / non-source commit). A cache hit returns the stored verdict without
/// re-running.
pub(crate) async fn run_write_checks_after_seal(
    orch: &Orchestrator,
    run_context: Option<&RunContext>,
    cwd: &str,
    tool_use_id: &str,
) -> Option<String> {
    let _guard = run_context.map(|context| WriteChecksInFlightGuard::new(orch, &context.job_id));
    run_write_checks_after_seal_inner(orch, run_context, cwd, tool_use_id).await
}

struct WriteChecksInFlightGuard<'a> {
    orch: &'a Orchestrator,
    job_id: String,
}

impl<'a> WriteChecksInFlightGuard<'a> {
    fn new(orch: &'a Orchestrator, job_id: &str) -> Self {
        orch.begin_write_checks(job_id);
        let _ = orch.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "check_result_cache", "action": "update"}),
        );
        Self {
            orch,
            job_id: job_id.to_string(),
        }
    }
}

impl Drop for WriteChecksInFlightGuard<'_> {
    fn drop(&mut self) {
        self.orch.end_write_checks(&self.job_id);
        let _ = self.orch.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "check_result_cache", "action": "update"}),
        );
    }
}

async fn run_write_checks_after_seal_inner(
    orch: &Orchestrator,
    run_context: Option<&RunContext>,
    cwd: &str,
    tool_use_id: &str,
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
    let logical = crate::mcp::handlers::branch::resolve_current_for_read(
        orch,
        &crate::mcp::types::McpCallbackRequest {
            run_id: Some(run_context.run_id.clone()),
            cwd: cwd.to_string(),
            ..Default::default()
        },
    )
    .await
    .ok()?;

    // 2. Load the checks contract DECLARED BY that exact commit. The definition
    // and the content it evaluates are the same tree, so a `.cairn/config.yaml`
    // edit governs the commits that carry it and nothing else — a branch
    // experimenting with check config cannot reach a sibling job's cadence
    // (CAIRN-3333). A project-level Settings edit takes effect for a job on the
    // first commit that contains it.
    let CommitChecksContract {
        contract: ChecksContract {
            checks,
            extra_inputs,
        },
        defined_by_commit,
    } = load_checks_contract_at_commit(&logical.object_repository_path, &logical.commit_id).await?;
    if checks.is_empty() {
        return None;
    }
    // Structural guard, deliberately a hard assert rather than a log: planning
    // selects, keys, and submits checks that this exact commit declared. If a
    // future edit reintroduces a second contract source, this fails visibly here
    // rather than silently recording a sibling's definition.
    assert_eq!(
        defined_by_commit, logical.commit_id,
        "write-cadence checks must be defined by the commit they evaluate"
    );

    let jj = JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
    let planning_jj = jj.clone();
    let planning_repo = logical.repository_path;
    let planning_head = logical.commit_id;
    let planning_base = live_base.unwrap_or(logical.default_commit_id);
    let planning_checks = checks.clone();
    let planning_extra_inputs = extra_inputs.clone();
    let planning_db = orch.db.local.clone();
    let planning_project_id = run_context.project_id.clone();
    let planning_job_id = run_context.job_id.clone();
    let planning_started = std::time::Instant::now();
    let planned = tokio::task::spawn_blocking(move || {
        let changed =
            logical_changed_files(&planning_jj, &planning_repo, &planning_base, &planning_head)?;
        if changed.is_empty() {
            return None;
        }
        // The sealed tree's entry listing feeds BOTH halves of input resolution:
        // the manifest blobs the dependency closures are derived from, and the
        // per-check filtered-tree cache component. One read serves both, and it
        // is skipped entirely when no check declares inputs at all.
        let entries = if any_check_declares_inputs(planning_checks.values()) {
            tree_entries(&planning_jj, &planning_repo, &planning_head).ok()
        } else {
            None
        };
        let blobs = TreeBlobs {
            jj: &planning_jj,
            repository: &planning_repo,
        };
        let snapshot = TreeSnapshot::new(entries.as_deref(), &blobs);
        let inputs = ResolvedInputs::resolve(&planning_checks, &planning_extra_inputs, &snapshot);
        let plans = applicable_write_checks(&planning_checks, &inputs, &changed, &planning_repo);
        if plans.is_empty() {
            return None;
        }
        let tree_hash = logical_tree_hash(&planning_jj, &planning_repo, &planning_head).ok()?;
        let sealed_commit = planning_head.clone();
        // The narrowing baseline is this JOB's last green run of each check, not the
        // project's most recent row of any status. Both qualifiers carry weight: a
        // sibling branch's tree is not an anchor on this branch's lineage, and a red
        // run must not displace the green one it superseded.
        let baseline_by_check: HashMap<String, CheckResultCacheEntry> =
            list_latest_passing_check_results_for_job(planning_db.clone(), &planning_job_id)
                .unwrap_or_default()
                .into_iter()
                .map(|row| (row.check_name.clone(), row))
                .collect();
        let keyed: Vec<(CheckPlan, String)> = plans
            .into_iter()
            .map(|plan| {
                let check = planning_checks
                    .get(&plan.name)
                    .expect("planned check must retain its configured definition");
                let selector = inputs.for_check(&plan.name);
                let input_hash = check_result_key(
                    check,
                    selector,
                    entries.as_deref(),
                    &tree_hash,
                    &check_platform_identity(),
                    check_toolchain_identity(),
                );
                let should_reselect = get_check_result(
                    planning_db.clone(),
                    &planning_project_id,
                    &plan.name,
                    &input_hash,
                )
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
                    replan_one_check(
                        &plan.name,
                        check,
                        &inputs,
                        &selected_changed,
                        &planning_repo,
                    )
                    .unwrap_or(plan)
                } else {
                    plan
                };
                (selected_plan, input_hash)
            })
            .collect();
        Some((keyed, tree_hash, sealed_commit))
    })
    .await
    .ok()??;
    let (keyed, tree_hash, sealed_commit) = planned;

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
    // This wave is fleet-backed and placement has not happened yet. Treat every
    // plan as an execution candidate; reservation still removes infrastructure-
    // suppressed triples before admission. A coordinator-local hit must not erase
    // a command that the scheduler may place remotely.
    let miss_indices: Vec<usize> = (0..keyed.len()).collect();
    let status_notify: CheckStatusNotify = Arc::new(move |checks, phase, phase_detail| {
        let _ = emitter.emit(
            "check-status",
            serde_json::to_value(CheckStatusPayload {
                run_id: notify_run_id.clone(),
                tool_use_id: notify_tool_use_id.clone(),
                checks,
                phase,
                phase_detail,
            })
            .unwrap_or(serde_json::Value::Null),
        );
    });
    let status_board = CheckStatusBoard::new(&keyed, status_notify);
    status_board.set_phase(
        (!miss_indices.is_empty()).then_some("dispatching"),
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
            status_board.transition(
                index,
                if hit.passed { "passed" } else { "failed" },
                Some("cached".into()),
            );
        }
    }

    let slot_env = slot_check_env(jj.shell_env());
    let batch_outcome = if miss_indices.is_empty() {
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
                    order: &fixer_first_submission_order(&keyed, &checks, &miss_indices),
                    timeouts: &timeouts,
                    sealed_commit: &sealed_commit,
                    slot_env: &slot_env,
                    tool_use_id,
                    status_board: Some(status_board.clone()),
                },
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
    if let Some(delta) = delta {
        let (Some(request), Some(store_dir)) = (request.as_ref(), store_dir.as_ref()) else {
            return Some(
                "Checks: \u{2717} write-check fold (the slot publication context was lost)".into(),
            );
        };
        let author = GitAuthor::new("Cairn checks", "checks@cairn.local");
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
            Ok(published) => published,
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
        let rekeyed = rekey_wave_onto_fix(
            orch,
            WriteCheckBatchRequest {
                run_context,
                repo_root,
                checks: &checks,
                extra_inputs: &extra_inputs,
                keyed: &keyed,
                order: &miss_indices,
                timeouts: &timeouts,
                sealed_commit: &sealed_commit,
                slot_env: &slot_env,
                tool_use_id,
                status_board: Some(status_board.clone()),
            },
            cwd,
            &published.paths,
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
        batched_results.extend(rekeyed.results);
        fixed = Some(published);
    }
    let observation_commit = fixed
        .as_ref()
        .map(|published| published.commit.as_str())
        .unwrap_or(sealed_commit.as_str());
    let batched_results = Arc::new(std::sync::Mutex::new(batched_results));
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
        tool_use_id,
        CheckExecMode::Shared,
        Some(orch),
        Some(status_board),
        move |index, _command, _stream_id| {
            let batched_results = batched_results.clone();
            async move {
                batched_results
                    .lock()
                    .unwrap()
                    .remove(&index)
                    .unwrap_or_else(|| {
                        Err(CheckExecutionFailure::substrate(
                            SubstrateFailureShape::Result,
                            format!("missing batched outcome for plan index {index}"),
                        ))
                    })
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
    let summary = format!("Checks: {}", format_check_summary(&results));
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
struct WriteCheckBatchRequest<'a> {
    run_context: &'a RunContext,
    repo_root: &'a Path,
    checks: &'a HashMap<String, CheckCommand>,
    /// Node-level extra inputs, carried so the post-fix re-key can resolve every
    /// selector afresh against the tree the fix actually landed.
    extra_inputs: &'a HashMap<String, Vec<String>>,
    keyed: &'a [(CheckPlan, String)],
    order: &'a [usize],
    timeouts: &'a [u32],
    sealed_commit: &'a str,
    slot_env: &'a [(String, String)],
    tool_use_id: &'a str,
    status_board: Option<CheckStatusBoard>,
}

/// Submit one build-slot batch for the given cache-MISS plan indices, in the
/// given execution order. Every failure mode resolves to per-index failures, so
/// the caller always holds exactly one outcome per submitted index.
async fn submit_write_check_batch_for(
    orch: &Orchestrator,
    batch: WriteCheckBatchRequest<'_>,
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
        slot_env,
        tool_use_id,
        status_board,
    } = batch;
    let items: Vec<PlannedCheckBatchItem> = order
        .iter()
        .map(|index| PlannedCheckBatchItem {
            index: *index,
            name: keyed[*index].0.name.clone(),
            input_hash: keyed[*index].1.clone(),
            resource_identity_key: check_resource_identity(
                &keyed[*index].0.name,
                checks
                    .get(&keyed[*index].0.name)
                    .expect("planned check must retain its configured definition"),
            )
            .key,
            command: keyed[*index].0.command.clone(),
            stream_id: crate::mcp::handlers::run::check_stream_id(tool_use_id, *index),
            env: slot_env.to_vec(),
            timeout_ms: timeouts[*index],
            executor: checks
                .get(&keyed[*index].0.name)
                .and_then(|check| check.executor.clone()),
            resource_class: keyed[*index].0.resource_class,
        })
        .collect();
    if let Some(board) = status_board.as_ref() {
        board.set_phase(Some("provisioning"), Some("resolving build slot".into()));
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
            submit_planned_check_batch(
                orch,
                PlannedCheckBatchRequest {
                    project_id: run_context.project_id.clone(),
                    repository,
                    store_dir,
                    sealed_commit: sealed_commit.to_string(),
                    requesting_job_id: run_context.job_id.clone(),
                    owner: cairn_common::executor_protocol::CellOwnerRef {
                        project_id: run_context.project_id.clone(),
                        project_key: Some(run_context.project_key.clone()),
                        issue_number: run_context.issue_number,
                        job_id: Some(run_context.job_id.clone()),
                        execution_seq: run_context.exec_seq,
                        node_kind: run_context.job_name.clone(),
                    },
                    affinity_key: Some(run_context.job_id.clone()),
                    priority: CellPriority::WriteCheck,
                    env: slot_env.to_vec(),
                    items,
                    run_context: Some(run_context.clone()),
                    mutation_policy: MutationPolicy::AllowDelta,
                    status_board,
                },
            )
            .await
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
struct FixedWave {
    /// The wave's plans paired with their cache key on the POST-fix tree. A
    /// re-verified check also carries its re-planned command.
    keyed: Vec<(CheckPlan, String)>,
    /// Whole-tree hash of the post-fix commit, re-stamped onto every row.
    tree_hash: String,
    /// Outcomes from the re-verification batch, by plan index. They replace the
    /// pre-fix outcome for those checks; every other index keeps its original.
    results: HashMap<usize, Result<CheckExecResult, CheckExecutionFailure>>,
    /// Set when re-verification dirtied the tree AGAIN. Never folded.
    non_convergent: Option<crate::fleet::MutationDelta>,
}

/// What re-planning a wave against the fixed commit yields: every plan paired
/// with its re-derived result key, the indices whose verdict the fix invalidated,
/// and the fixed commit's tree hash.
type Replan = (Vec<(CheckPlan, String)>, Vec<usize>, String);

/// Re-key a wave onto the commit its fix landed, re-verifying exactly the checks
/// whose verdict the fix invalidated.
///
/// Survival is decided per check by [`verdict_survives_fix`]. In the ordinary
/// case — a declared formatter rewriting files inside its own `impact` — every
/// verdict survives and no second batch is submitted at all; the wave costs one
/// slot admission and one execution per check. A fix that no declared fixer
/// explains, or a check that was answered from the cache before the fix touched
/// its inputs, falls back to running that check once against the fixed commit.
async fn rekey_wave_onto_fix(
    orch: &Orchestrator,
    wave: WriteCheckBatchRequest<'_>,
    cwd: &str,
    fixed_paths: &[String],
) -> Result<FixedWave, String> {
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
    let fixed_paths = fixed_paths.to_vec();
    let jj = JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
    let repository = logical.repository_path.clone();
    let head = logical.commit_id.clone();
    let base = live_base.unwrap_or(logical.default_commit_id);
    let planning_head = head.clone();
    let planning_repository = repository.clone();

    let (rekeyed, invalidated, tree_hash) =
        tokio::task::spawn_blocking(move || -> Result<Replan, String> {
            let tree_hash = logical_tree_hash(&jj, &planning_repository, &planning_head)?;
            let entries = if any_check_declares_inputs(checks.values()) {
                tree_entries(&jj, &planning_repository, &planning_head).ok()
            } else {
                None
            };
            let blobs = TreeBlobs {
                jj: &jj,
                repository: &planning_repository,
            };
            let snapshot = TreeSnapshot::new(entries.as_deref(), &blobs);
            let inputs = ResolvedInputs::resolve(&checks, &extra_inputs, &snapshot);
            // Attribution is a property of the whole wave: the fix is the slot's
            // combined delta, so the question is whether the declared fixers that
            // ran can account for every path in it. It is asked against the FIXED
            // tree's inputs, which is the tree the verdicts are being keyed to.
            let fixers: Vec<&InputSelector> = fixer_names
                .iter()
                .map(|name| inputs.for_check(name))
                .collect();
            let attributed = fix_is_attributed_to_declared_fixers(&fixed_paths, &fixers);
            let changed = logical_changed_files(&jj, &planning_repository, &base, &planning_head)
                .unwrap_or_default();
            let mut rekeyed = Vec::with_capacity(plans.len());
            let mut invalidated = Vec::new();
            for (index, plan) in plans.into_iter().enumerate() {
                let check = checks
                    .get(&plan.name)
                    .expect("planned check must retain its configured definition");
                let key_after = check_result_key(
                    check,
                    inputs.for_check(&plan.name),
                    entries.as_deref(),
                    &tree_hash,
                    &check_platform_identity(),
                    check_toolchain_identity(),
                );
                if verdict_survives_fix(
                    executed.contains(&index),
                    attributed,
                    superseded.contains(&index),
                    &keys_before[index],
                    &key_after,
                ) {
                    rekeyed.push((plan, key_after));
                    continue;
                }
                // The fix changed this check's inputs and nothing proves it saw
                // them. Re-plan against the fixed commit so a `{changedFiles}`
                // selector covers the fixed paths too, then run it once.
                let replanned =
                    replan_one_check(&plan.name, check, &inputs, &changed, &planning_repository)
                        .unwrap_or_else(|| plan.clone());
                invalidated.push(index);
                rekeyed.push((replanned, key_after));
            }
            Ok((rekeyed, invalidated, tree_hash))
        })
        .await
        .map_err(|error| format!("join the post-fix planning unit: {error}"))??;

    // An invalidated verdict may still be answerable from the cache on the FIXED
    // tree; only a genuine miss needs the slot.
    let misses: Vec<usize> = invalidated
        .into_iter()
        .filter(|index| {
            needs_execution(
                orch.db.local.clone(),
                &wave.run_context.project_id,
                &rekeyed[*index].0,
                &rekeyed[*index].1,
            )
        })
        .collect();
    if misses.is_empty() {
        return Ok(FixedWave {
            keyed: rekeyed,
            tree_hash,
            results: HashMap::new(),
            non_convergent: None,
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
            ..wave
        },
    )
    .await;
    Ok(FixedWave {
        keyed: rekeyed,
        tree_hash,
        results: outcome.results,
        non_convergent: outcome.delta,
    })
}

/// Where a project declares its checks, inside whatever tree is being read.
const CHECKS_CONFIG_PATH: &str = ".cairn/config.yaml";

/// A checks contract together with the commit that DECLARED it.
///
/// The two travel together because a verdict is only interpretable when you know
/// which definition produced it. Every execution path binds both halves to one
/// commit: the definition and the content it evaluates come from the same tree.
#[derive(Debug, Clone)]
pub(crate) struct CommitChecksContract {
    pub(crate) contract: ChecksContract,
    pub(crate) defined_by_commit: String,
}

/// Load the `checks` contract DECLARED BY an immutable commit.
///
/// The bytes come out of the commit's own tree through the git object database,
/// so no checkout is materialized and no live file can influence the answer. A
/// missing, unreadable, invalid, or check-less `.cairn/config.yaml` in that
/// commit means exactly what an absent contract has always meant: nothing is
/// selected. There is deliberately NO fallback to the project checkout — falling
/// back is what let a check defined only on one agent branch execute inside a
/// sibling job's cadence against the sibling's tree (CAIRN-3333).
///
/// `object_repository` is the path holding the git object database (the project
/// checkout); jj writes every sealed commit into it, so a branch commit that was
/// never checked out anywhere is still readable here.
pub(crate) fn checks_contract_at_commit(
    object_repository: &Path,
    commit: &str,
) -> Option<CommitChecksContract> {
    let bytes = match crate::mcp::handlers::read::file_at_commit(
        object_repository.to_path_buf(),
        commit.to_string(),
        CHECKS_CONFIG_PATH,
    ) {
        Ok(Some(bytes)) => bytes,
        Ok(None) => return None,
        Err(error) => {
            log::warn!(
                "checks: cannot read {CHECKS_CONFIG_PATH} at commit {commit}: {error}; \
                 no checks are selected for it"
            );
            return None;
        }
    };
    let content = match String::from_utf8(bytes) {
        Ok(content) => content,
        Err(_) => {
            log::warn!(
                "checks: {CHECKS_CONFIG_PATH} at commit {commit} is not valid UTF-8; \
                 no checks are selected for it"
            );
            return None;
        }
    };
    // The migration flag is ignored on purpose: a sealed commit is read, never
    // rewritten. Only the checkout loader migrates.
    let (settings, _needs_migration) =
        match crate::config::project_settings::parse_project_settings(&content) {
            Ok(parsed) => parsed,
            Err(error) => {
                log::warn!(
                    "checks: {CHECKS_CONFIG_PATH} at commit {commit} is invalid ({error}); \
                     no checks are selected for it"
                );
                return None;
            }
        };
    Some(CommitChecksContract {
        contract: crate::config::project_settings::checks_contract_from(settings)?,
        defined_by_commit: commit.to_string(),
    })
}

/// [`checks_contract_at_commit`] off the async caller's runtime thread. The read
/// touches the object database, which is filesystem work.
pub(crate) async fn load_checks_contract_at_commit(
    object_repository: &Path,
    commit: &str,
) -> Option<CommitChecksContract> {
    let object_repository = object_repository.to_path_buf();
    let commit = commit.to_string();
    tokio::task::spawn_blocking(move || checks_contract_at_commit(&object_repository, &commit))
        .await
        .ok()
        .flatten()
}

/// The subset of planned checks that both apply to the change set AND run at the
/// TURN-END cadence. `when:review` (including the `idle` legacy alias) runs at
/// every turn-end; `when:write` never runs here (it is the mid-turn cadence). An
/// impact-scoped check that no changed file matches has `applies == false`. Pure,
/// so the cadence gate is unit-tested.
pub(crate) fn applicable_turn_end_checks(
    checks: &HashMap<String, CheckCommand>,
    inputs: &ResolvedInputs,
    changed: &[GraphFileChange],
    repo_root: &Path,
) -> Vec<CheckPlan> {
    plan_checks(checks, inputs, changed, repo_root)
        .into_iter()
        .filter(|plan| plan.applies)
        .filter(|plan| {
            checks
                .get(&plan.name)
                .is_some_and(|check| match check.when {
                    CheckWhen::Review => true,
                    CheckWhen::Write => false,
                })
        })
        .collect()
}

/// Applicable hard requirements for synchronous combined-tree verification.
/// Advisory review checks remain part of the turn-end feedback cadence, but a
/// manual child-PR merge must not launch them.
fn applicable_combined_tree_gate_checks(
    checks: &HashMap<String, CheckCommand>,
    inputs: &ResolvedInputs,
    changed: &[GraphFileChange],
    repo_root: &Path,
) -> Vec<CheckPlan> {
    applicable_turn_end_checks(checks, inputs, changed, repo_root)
        .into_iter()
        .filter(|plan| {
            checks
                .get(&plan.name)
                .is_some_and(|check| check.policy == CheckPolicy::Gate)
        })
        .collect()
}

fn applicable_write_checks(
    checks: &HashMap<String, CheckCommand>,
    inputs: &ResolvedInputs,
    changed: &[GraphFileChange],
    repo_root: &Path,
) -> Vec<CheckPlan> {
    plan_checks(checks, inputs, changed, repo_root)
        .into_iter()
        .filter(|plan| plan.applies)
        .filter(|plan| {
            checks
                .get(&plan.name)
                .is_some_and(|check| check.when == CheckWhen::Write)
        })
        .collect()
}

fn replan_one_check(
    name: &str,
    check: &CheckCommand,
    inputs: &ResolvedInputs,
    changed: &[GraphFileChange],
    repo_root: &Path,
) -> Option<CheckPlan> {
    let mut one = HashMap::new();
    one.insert(name.to_string(), check.clone());
    plan_checks(&one, inputs, changed, repo_root)
        .into_iter()
        .next()
        .filter(|plan| plan.applies)
}

/// Changed-file selector for a cache miss. The planner stays pure: this runner
/// reads cache rows and tree objects, then hands `plan_checks` either the narrowed
/// delta or the conservative cumulative branch diff as ordinary data.
fn selected_changed_files_for_miss(
    latest: Option<&CheckResultCacheEntry>,
    current_entries: Option<&[(String, String)]>,
    check: &CheckCommand,
    inputs: &ResolvedInputs,
    selector: &InputSelector,
    cumulative: &[GraphFileChange],
    tree: &TreeBlobs<'_>,
) -> Vec<GraphFileChange> {
    let (jj, repo_root) = (tree.jj, tree.repository);
    let Some(latest) = latest.filter(|row| row.passed) else {
        return cumulative.to_vec();
    };
    let Some(current_entries) = current_entries else {
        return cumulative.to_vec();
    };
    let baseline_entries = match tree_entries(jj, repo_root, &latest.tree_hash) {
        Ok(entries) => entries,
        Err(e) => {
            log::warn!(
                "when:write checks: failed to read cached baseline tree {} for {}: {e}; \
                 using cumulative selection",
                latest.tree_hash,
                latest.check_name
            );
            return cumulative.to_vec();
        }
    };
    // The baseline's key must be recomputed under the selector the baseline TREE
    // resolves to, because that is the selector the original run keyed by. A
    // closure is a pure function of the tree, so a baseline whose manifests
    // differ genuinely had a different input set.
    let baseline_snapshot = TreeSnapshot::new(Some(&baseline_entries), tree);
    let baseline_selector = inputs.resolve_for_tree(check, &baseline_snapshot);
    baseline_delta_changed_files(
        Some(latest),
        Some(&baseline_entries),
        Some(current_entries),
        check,
        &baseline_selector,
        selector,
        &check_platform_identity(),
        check_toolchain_identity(),
        cumulative,
    )
}

/// Whether a cached green row was produced under the CURRENT check contract.
///
/// Recomputes the row's cache key from the baseline tree's own entries under the
/// live contract and compares it against the key actually stored. Equality means
/// every input folded into [`check_result_key`] — command, impact globs, policy,
/// cadence, resource class, timeout, executor selector, platform, toolchain —
/// is unchanged since that verdict was written.
///
/// This gate exists because the contract is deliberately re-read from the LIVE
/// project config on every commit (see `run_write_checks_after_seal_inner`), so it
/// can change underneath a cached row. The dangerous direction is **impact
/// expansion**: a check whose globs were `src/**` passes on a tree that also
/// contains an unexamined `packages/ui` edit, the globs later grow to include
/// `packages/ui/**`, and diffing that same tree under the new globs reports the UI
/// file as unchanged — so it is omitted from the selector even though no passing
/// run ever covered it. The input-hash cache correctly misses in that situation;
/// only the narrowing baseline needed the same contract awareness.
///
/// Failure is one-directional by construction: any mismatch, including an
/// incidental one such as a toolchain bump, discards the baseline and falls back to
/// the cumulative branch diff. That costs selectivity, never coverage. A false
/// accept is not reachable, because every contract field is hashed into the key.
fn baseline_matches_current_contract(
    baseline: &CheckResultCacheEntry,
    baseline_entries: &[(String, String)],
    check: &CheckCommand,
    baseline_selector: &InputSelector,
    platform: &str,
    toolchain: &str,
) -> bool {
    check_result_key(
        check,
        baseline_selector,
        Some(baseline_entries),
        &baseline.tree_hash,
        platform,
        toolchain,
    ) == baseline.input_hash
}

/// Pure decision rule for choosing a placeholder-selection change set. A passing
/// baseline means the cached verdict covered the baseline tree's impact-matched
/// subset, so the next run only has to select tests/targets reachable from the
/// paths whose matching tree entries changed since then.
///
/// A usable baseline has to clear three gates, and they fail for different reasons:
///
/// - **Passing.** Narrowing is anchored on a green verdict, so a later red run must
///   not displace the green one it superseded.
/// - **Same job** ([`list_latest_passing_check_results_for_job`], enforced by the
///   caller's query). Only a same-branch tree narrows usefully. Against a
///   concurrently running sibling branch the delta is the symmetric difference of
///   two unrelated trees, so it drags in every file the sibling touched and
///   routinely exceeds the cumulative branch diff it was supposed to beat.
/// - **Same contract** ([`baseline_matches_current_contract`]). The checks contract
///   is re-read live on every commit, so a verdict can outlive the definition that
///   produced it. Diffing an old green tree under newly widened `impact` globs
///   silently omits files the old run never examined.
///
/// The first two are selectivity concerns; the third is a coverage concern. All
/// three fall back to the cumulative branch diff, which is always safe.
#[allow(clippy::too_many_arguments)]
fn baseline_delta_changed_files(
    latest: Option<&CheckResultCacheEntry>,
    baseline_entries: Option<&[(String, String)]>,
    current_entries: Option<&[(String, String)]>,
    check: &CheckCommand,
    baseline_selector: &InputSelector,
    current_selector: &InputSelector,
    platform: &str,
    toolchain: &str,
    cumulative: &[GraphFileChange],
) -> Vec<GraphFileChange> {
    let Some(latest) = latest.filter(|row| row.passed) else {
        return cumulative.to_vec();
    };
    let (Some(baseline), Some(current)) = (baseline_entries, current_entries) else {
        return cumulative.to_vec();
    };
    if !baseline_matches_current_contract(
        latest,
        baseline,
        check,
        baseline_selector,
        platform,
        toolchain,
    ) {
        return cumulative.to_vec();
    }
    match diff_tree_entries_for_selector(baseline, current, current_selector) {
        delta if !delta.is_empty() => delta,
        _ => cumulative.to_vec(),
    }
}

fn diff_tree_entries_for_selector(
    baseline: &[(String, String)],
    current: &[(String, String)],
    selector: &InputSelector,
) -> Vec<GraphFileChange> {
    let baseline: BTreeMap<&str, &str> = baseline
        .iter()
        .filter(|(path, _)| selector.matches(path))
        .map(|(path, blob)| (path.as_str(), blob.as_str()))
        .collect();
    let current: BTreeMap<&str, &str> = current
        .iter()
        .filter(|(path, _)| selector.matches(path))
        .map(|(path, blob)| (path.as_str(), blob.as_str()))
        .collect();
    let paths: BTreeSet<&str> = baseline.keys().chain(current.keys()).copied().collect();
    let mut changes = Vec::new();
    for path in paths {
        let before = baseline.get(path);
        let after = current.get(path);
        if before == after {
            continue;
        }
        changes.push(GraphFileChange {
            path: path.to_string(),
            previous_path: None,
            status: match (before, after) {
                (None, Some(_)) => "added",
                (Some(_), None) => "deleted",
                (Some(_), Some(_)) => "modified",
                (None, None) => unreachable!(),
            }
            .to_string(),
            additions: 0,
            deletions: 0,
        });
    }
    changes
}

/// The outcome of one planned check: its exit-code-driven verdict, the parsed
/// per-test detail (enrichment, may be absent), and the retained combined-output
/// tail used as the excerpt fallback. Carried out of [`run_planned_checks`] so
/// the inline summary can render WHAT failed, not just the exit code.
pub(crate) struct CheckOutcome {
    pub(crate) name: String,
    pub(crate) passed: bool,
    pub(crate) exit_code: Option<i32>,
    /// Terminal classification for a FAILING check (timeout / spawn error /
    /// signal kill), so a summary renders the real failure, not a bare exit
    /// code. `None` for a pass or an ordinary non-zero exit.
    pub(crate) failure_kind: Option<CheckFailureKind>,
    /// Structured per-test result, when the runner's output could be parsed.
    pub(crate) parsed: Option<ParsedCheckResult>,
    /// Retained combined-output tail — the excerpt source when the parse carries
    /// no per-failure messages (nextest) or there is no parse at all.
    pub(crate) output_tail: String,
    /// Whether this verdict was REUSED from the cache rather than run for this
    /// commit. The summary annotates cache hits so a reused verdict is
    /// distinguishable from a fresh run at a glance.
    pub(crate) cached: bool,
    /// Wall-clock duration of the run that produced this verdict, in ms. On a
    /// cache hit this is the stored duration of the original run. Surfaced for
    /// non-test-runner checks (typecheck, api, …) where a test count is not
    /// meaningful.
    pub(crate) duration_ms: i64,
    /// When set, Cairn did not execute this check AT ALL: the triple had already
    /// failed for infrastructure reasons this many consecutive times, reaching
    /// [`crate::execution::cache::OBSERVED_INFRA_FAILURE_BOUND`]. This is neither a verdict
    /// nor a reused one — it is the honest absence of both — so it must never
    /// wake anyone or read as a red the agent's change caused.
    pub(crate) suppressed_after: Option<i64>,
    /// The immutable observation this evaluation recorded, as the recorder wrote
    /// it. A caller that must name the row reads it from here rather than
    /// re-deriving a key: a remote verdict is keyed by an empty environment
    /// fingerprint that no coordinator-side key can match, and a coalesced
    /// sibling's row was never this caller's to compute. `None` is the honest
    /// answer when nothing was recorded — a suppressed triple never ran, and a
    /// failed write leaves the verdict standing without a durable row.
    pub(crate) recorded: Option<crate::execution::cache::RecordedCheckObservation>,
}

impl CheckOutcome {
    /// Whether this outcome is a verdict about the agent's own change. An
    /// infrastructure failure is a fact about Cairn; a suppression is the absence
    /// of a run. Neither is the agent's business, and this is the predicate that
    /// keeps both out of the wake path.
    pub(crate) fn is_genuine_failure(&self) -> bool {
        !self.passed
            && self.suppressed_after.is_none()
            && !self
                .failure_kind
                .is_some_and(CheckFailureKind::is_infrastructure)
    }
}

/// The agent-facing text for a check Cairn declined to run. Authored in the same
/// voice as [`SubstrateFailure::agent_message`]: what happened, that the agent's
/// change is not implicated, what will make it run again, and where the substrate
/// half of the story lives. The stored diagnostic from the last real attempt is
/// appended so the agent still sees WHAT kept breaking.
pub(crate) fn suppressed_check_message(streak: i64, last_diagnostic: &str) -> String {
    let head = format!(
        "Cairn is no longer running this check. It failed for infrastructure reasons \
         {streak} times in a row on unchanged inputs, so Cairn stopped executing it rather \
         than repeating the same failure indefinitely. This is a failure inside Cairn, not a \
         result about your change: no verdict was recorded, and none is being withheld. The \
         check runs again as soon as its inputs change, and an operator can restore it for \
         these same inputs by repairing the substrate and restarting Cairn. The full \
         diagnostic is in Cairn's operator log."
    );
    let last = last_diagnostic.trim();
    if last.is_empty() {
        head
    } else {
        format!("{head}\n\nLast infrastructure failure:\n{last}")
    }
}

/// Versioned identity of the host tools that can affect project-check outcomes.
/// Probes run at most once per runner process; cache lookups never shell out.
static CHECK_TOOLCHAIN_IDENTITY: OnceLock<String> = OnceLock::new();

pub(crate) fn check_platform_identity() -> String {
    format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH)
}

pub fn check_toolchain_identity() -> &'static str {
    CHECK_TOOLCHAIN_IDENTITY
        .get_or_init(|| {
            fn version(program: &str, args: &[&str]) -> String {
                std::process::Command::new(program)
                    .args(args)
                    .output()
                    .ok()
                    .filter(|output| output.status.success())
                    .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string())
                    .filter(|output| !output.is_empty())
                    .unwrap_or_else(|| "unavailable".to_string())
            }
            format!(
                "rustc={};bun={}",
                version("rustc", &["--version", "--verbose"]),
                version("bun", &["--version"])
            )
        })
        .as_str()
}

fn check_environment_fingerprint(variable_names: impl IntoIterator<Item = String>) -> String {
    crate::execution::check_identity::local_environment_identity(
        vec![check_toolchain_identity().to_string()],
        variable_names,
    )
    .fingerprint
}

fn plan_environment_fingerprint(plan: &CheckPlan) -> String {
    check_environment_fingerprint(plan.verdict_environment_names.clone())
}

#[cfg(test)]
fn current_check_environment_fingerprint() -> String {
    check_environment_fingerprint(Vec::new())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CheckReuseDecision {
    reusable: bool,
    reason: Option<String>,
}

fn check_reuse_decision(
    command: &str,
    passed: bool,
    failure_kind: Option<CheckFailureKind>,
    parsed: Option<&ParsedCheckResult>,
) -> CheckReuseDecision {
    let reject = |reason: &str| CheckReuseDecision {
        reusable: false,
        reason: Some(reason.to_string()),
    };
    if !passed {
        return reject("check did not pass");
    }
    if failure_kind.is_some() {
        return reject("check ended with an infrastructure or process failure");
    }
    let configured_test_runner = command.contains("test:rust")
        || command.contains("cargo test")
        || command.contains("nextest")
        || command.contains("vitest");
    let Some(parsed) = parsed else {
        return if configured_test_runner {
            reject("test runner did not produce a structured result")
        } else {
            CheckReuseDecision {
                reusable: true,
                reason: None,
            }
        };
    };
    if !parsed.is_test_runner() {
        return CheckReuseDecision {
            reusable: true,
            reason: None,
        };
    }
    if !parsed.complete {
        return reject("test result is incomplete or degraded");
    }
    if parsed.selection == "empty" || parsed.tests_run() == 0 {
        return reject("test selection was empty");
    }
    if parsed.undeclared_skips > 0 {
        return reject("test result contains undeclared skips");
    }
    if parsed
        .tests
        .iter()
        .any(|test| test.retried || test.attempts > 1)
    {
        return reject("one or more tests required a retry");
    }
    if parsed.tests.iter().any(|test| test.flaky) {
        return reject("one or more tests were marked flaky");
    }
    CheckReuseDecision {
        reusable: true,
        reason: None,
    }
}

#[allow(clippy::too_many_arguments)]
fn fresh_observation_write(
    project_id: &str,
    commit: CheckRunCommit<'_>,
    tree_hash: &str,
    input_hash: &str,
    plan: &CheckPlan,
    job_id: &str,
    tool_use_id: &str,
    exit_code: i32,
    failure_kind: Option<CheckFailureKind>,
    duration_ms: i64,
    output_tail: &str,
    provenance: Option<&cairn_common::executor_protocol::CellExecutionMeta>,
    parsed: Option<&ParsedCheckResult>,
    reuse: CheckReuseDecision,
) -> FreshCheckObservationWrite {
    let tests = parsed
        .map(|result| {
            result
                .tests
                .iter()
                .map(|test| CheckTestResultRow {
                    test_id: test.id.clone(),
                    status: test.status.clone(),
                    duration_ms: test.duration_ms.map(|value| value as i64),
                    attempt_count: Some(test.attempts as i64),
                    failure_excerpt: test.failure_message.clone(),
                    skip_reason: test.skip_reason.clone(),
                    declaration_source: test.skip_declaration_source.clone(),
                    flaky: test.flaky,
                })
                .collect()
        })
        .unwrap_or_default();
    // A remote executor's verdict environment is not the coordinator's environment.
    // Until executor admission returns the selected machine's full advertised identity
    // and locally hashed verdict variables, retain the observation for diagnosis but
    // never publish it as reusable under a coordinator-derived cache key.
    let remote_environment_unknown =
        provenance.is_some_and(|meta| meta.executor_id != crate::fleet::COLOCATED_EXECUTOR_ID);
    let reuse = if remote_environment_unknown {
        CheckReuseDecision {
            reusable: false,
            reason: Some(
                "selected executor did not report a complete verdict environment identity"
                    .to_string(),
            ),
        }
    } else {
        reuse
    };
    let environment_fingerprint = if remote_environment_unknown {
        String::new()
    } else {
        plan_environment_fingerprint(plan)
    };
    FreshCheckObservationWrite {
        id: uuid::Uuid::new_v4().to_string(),
        project_id: project_id.to_string(),
        commit_sha: commit.evaluated.to_string(),
        defined_by_commit_sha: commit.defined_by.to_string(),
        tree_hash: tree_hash.to_string(),
        check_name: plan.name.clone(),
        input_hash: input_hash.to_string(),
        environment_fingerprint,
        exit_code,
        verdict: if exit_code == 0 { "passed" } else { "failed" }.to_string(),
        failure_kind: failure_kind.map(|kind| kind.as_str().to_string()),
        complete: parsed
            .map(|result| !result.is_test_runner() || result.complete)
            .unwrap_or(!plan.command.contains("test")),
        reusable: reuse.reusable,
        non_reusable_reason: reuse.reason,
        parser_version: crate::execution::check_identity::CHECK_PARSER_VERSION as i64,
        result_schema_version: crate::execution::check_identity::CHECK_RESULT_SCHEMA_VERSION as i64,
        ran_at: chrono::Utc::now().timestamp_millis(),
        duration_ms,
        job_id: Some(job_id.to_string()),
        run_id: tool_use_id
            .strip_prefix("manual-check:")
            .and_then(|value| value.split_once(':').map(|(run_id, _)| run_id.to_string())),
        cadence: if tool_use_id.starts_with("turn-checks:") {
            "review"
        } else if tool_use_id.starts_with("manual-check:") {
            "manual"
        } else {
            "write"
        }
        .to_string(),
        executor_id: provenance.map(|meta| meta.executor_id.clone()),
        executor_device_id: provenance.map(|meta| meta.executor_device_id.clone()),
        executor_connection_generation: provenance
            .map(|meta| meta.executor_connection_generation as i64),
        executor_cell_id: provenance.map(|meta| meta.cell_id.clone()),
        executor_lease_epoch: provenance.map(|meta| meta.cell_epoch as i64),
        executor_started_at_unix_ms: provenance.map(|meta| meta.started_at_unix_ms as i64),
        executor_finished_at_unix_ms: provenance.map(|meta| meta.finished_at_unix_ms as i64),
        runner_build_id: cairn_common::build_identity::current_executable_build_id().ok(),
        toolchain_fingerprint: Some(check_toolchain_identity().to_string()),
        output_tail: output_tail.to_string(),
        target_results_json: parsed.and_then(|result| serde_json::to_string(result).ok()),
        tests,
    }
}

fn check_command_identity(
    check: &CheckCommand,
    selector: &InputSelector,
    entries: Option<&[(String, String)]>,
    tree_hash: &str,
    platform: &str,
    toolchain: &str,
) -> CommandResourceIdentity {
    let (os, arch) = platform.split_once('-').unwrap_or((platform, "unknown"));
    let content =
        crate::execution::check_identity::content_identity(check, selector, entries, tree_hash);
    let environment = crate::execution::check_identity::environment_identity(
        crate::execution::check_identity::CheckEnvironmentInput {
            os: os.to_string(),
            arch: arch.to_string(),
            executor_id: None,
            device_id: None,
            capabilities: vec![toolchain.to_string()],
            runner_build_id: cairn_common::build_identity::current_executable_build_id().ok(),
            variable_names: crate::execution::check_identity::verdict_environment_names(check),
        },
        |name| std::env::var(name).ok(),
    );
    CommandResourceIdentity {
        version: cairn_common::executor_protocol::COMMAND_RESOURCE_IDENTITY_VERSION,
        key: crate::execution::check_identity::combined_result_key(&content, &environment),
    }
}

pub(crate) fn check_result_key(
    check: &CheckCommand,
    selector: &InputSelector,
    entries: Option<&[(String, String)]>,
    tree_hash: &str,
    platform: &str,
    toolchain: &str,
) -> String {
    check_command_identity(check, selector, entries, tree_hash, platform, toolchain).key
}

/// Execute the planned checks against the sealed tree, consulting the cache
/// first. Each plan is paired with its per-check input hash (the cache key);
/// `tree_hash` is the whole-tree pointer re-stamped onto every evaluated row so
/// the `/checks` listing still surfaces the check at the current tree. Generic
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
async fn run_planned_checks_with_board<F, Fut, N, E>(
    db: Arc<LocalDb>,
    project_id: &str,
    commit: CheckRunCommit<'_>,
    tree_hash: &str,
    job_id: &str,
    plans: &[(CheckPlan, String)],
    tool_use_id: &str,
    mode: CheckExecMode,
    diagnostic_orch: Option<&Orchestrator>,
    status_board: Option<CheckStatusBoard>,
    execute: F,
    notify: N,
) -> Vec<CheckOutcome>
where
    F: Fn(usize, String, String) -> Fut,
    Fut: std::future::Future<Output = Result<CheckExecResult, E>>,
    E: Into<CheckExecutionFailure>,
    N: Fn(Vec<CheckStatusEntry>) + Send + Sync + 'static,
{
    // Checklist snapshot, seeded all-`pending` from the plan list. Each check
    // transitions ITS OWN entry and re-emits the whole snapshot, so the live line
    // is self-healing (latest snapshot wins). A std Mutex keeps the transition
    // helper a plain `Fn`; it is only ever locked to mutate + clone and released
    // before the (synchronous) emit, so no guard is held across an await.
    let board = status_board.unwrap_or_else(|| {
        CheckStatusBoard::new(plans, Arc::new(move |checks, _, _| notify(checks)))
    });
    board.emit_initial();

    // Phase 1: resolve cache HITS sequentially, and collect the MISS indices to
    // execute. A fleet-backed call cannot admit coordinator-local evidence here:
    // executor placement has not happened yet, so the eventual executor may be
    // remote. Until selection returns a trusted environment identity, only the
    // direct coordinator-local path (`diagnostic_orch == None`) can prove that
    // this fingerprint describes the environment that would execute the check.
    // `outcomes` is index-addressed so misses can complete out of order
    // (concurrent `Isolated` mode) and still reassemble into plan order.
    let admit_coordinator_cache = diagnostic_orch.is_none();
    let mut outcomes: Vec<Option<CheckOutcome>> = (0..plans.len()).map(|_| None).collect();
    let mut misses: Vec<usize> = Vec::new();
    for (index, (plan, input_hash)) in plans.iter().enumerate() {
        if !admit_coordinator_cache {
            misses.push(index);
            continue;
        }
        // Cache hit ⇒ reuse the stored verdict and rehydrate the structured
        // detail; run nothing. The lookup is keyed by the per-check INPUT hash, so
        // a commit that changed none of this check's impact-matched files hits
        // even though the whole-tree hash moved.
        let environment_fingerprint = plan_environment_fingerprint(plan);
        let schema_version = crate::execution::check_identity::CHECK_RESULT_SCHEMA_VERSION as i64;
        let Ok(Some(entry)) = get_exact_reusable_check_result(
            db.clone(),
            project_id,
            &plan.name,
            input_hash,
            &environment_fingerprint,
            schema_version,
        ) else {
            // Legacy, incomplete, red, flaky, or environment-mismatched evidence is diagnostic only.
            misses.push(index);
            continue;
        };
        let source_observation = get_reusable_check_observation_id(
            db.clone(),
            project_id,
            &plan.name,
            input_hash,
            &environment_fingerprint,
            schema_version,
        )
        .ok()
        .flatten();
        let Some(source_observation) = source_observation else {
            // Legacy rows remain available for diagnosis, but cannot suppress execution.
            misses.push(index);
            continue;
        };
        let recorded = crate::execution::cache::RecordedCheckObservation {
            id: source_observation.clone(),
            environment_fingerprint: environment_fingerprint.clone(),
            // Only a reusable observation is ever admitted as a hit.
            reusable: true,
        };
        let _ = record_cached_check_observation(
            db.clone(),
            CachedCheckObservationWrite {
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
                environment_fingerprint: environment_fingerprint.clone(),
                result_schema_version: schema_version,
                source_observation_id: source_observation,
                evaluated_at: chrono::Utc::now().timestamp_millis(),
            },
        );
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
            let _ = orch.services.emitter.emit(
                "db-change",
                serde_json::json!({"table": "build_slots", "action": "update"}),
            );
        }
        // Rehydrate the structured per-test detail persisted at run time.
        let parsed = entry
            .target_results_json
            .as_deref()
            .and_then(|s| serde_json::from_str::<ParsedCheckResult>(s).ok());
        let outcome = CheckOutcome {
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
        };
        // A cache hit jumps straight from pending to its final state.
        board.transition(
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
            // Admission happens once, in the executor's priority-aware queue, and
            // this submission declares what it will fan out to so that queue can
            // account for it. There is deliberately no second gate here: a
            // runner-local permit taken BEFORE entering the executor's queue is
            // what let a review request hold host capacity while a later
            // write-cadence request waited behind it (CAIRN-3108).
            board.transition(index, "running", None);
            let stream_id = crate::mcp::handlers::run::check_stream_id(tool_use_id, index);
            let started = Instant::now();
            let exec_outcome = execute(index, plan.command.clone(), stream_id)
                .await
                .map_err(Into::<CheckExecutionFailure>::into);

            // A refused reservation is not an execution. Submission declined to
            // launch this command because the triple's retry budget is spent, so
            // there is no output to parse, no verdict to classify, and nothing to
            // store — doing any of those would invent a result for a command that
            // never ran. Render the suppression from the last real attempt.
            if matches!(exec_outcome, Err(CheckExecutionFailure::Suppressed)) {
                let outcome = match get_suppressed_check_result(
                    db.clone(),
                    project_id,
                    &plan.name,
                    input_hash,
                ) {
                    Ok(Some(entry)) => {
                        record_suppressed_check(db, tree_hash, job_id, &plan.name, entry)
                    }
                    // The row cannot normally be missing — only a suppressed
                    // triple is ever refused — but the outcome must still say
                    // "not run" rather than fall through to a verdict.
                    _ => CheckOutcome {
                        name: plan.name.clone(),
                        passed: false,
                        exit_code: None,
                        failure_kind: Some(CheckFailureKind::Infrastructure),
                        parsed: None,
                        output_tail: suppressed_check_message(
                            crate::execution::cache::OBSERVED_INFRA_FAILURE_BOUND,
                            "the stored diagnostic is no longer available",
                        ),
                        cached: false,
                        duration_ms: 0,
                        suppressed_after: Some(
                            crate::execution::cache::OBSERVED_INFRA_FAILURE_BOUND,
                        ),
                        recorded: None,
                    },
                };
                board.transition(index, "failed", summary_annotation(&outcome));
                return (index, outcome);
            }

            let (
                exit_code,
                output,
                timed_out,
                spawn_error,
                substrate_failure,
                authoritative_duration_ms,
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
                    None,
                    duration_ms,
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
                Err(CheckExecutionFailure::Substrate(failure)) => (
                    None,
                    failure.agent_message(),
                    false,
                    false,
                    Some(failure),
                    None,
                    None,
                    None,
                ),
                Err(CheckExecutionFailure::Suppressed) => {
                    unreachable!("a refused reservation returns above")
                }
            };
            let duration_ms =
                authoritative_duration_ms.unwrap_or_else(|| started.elapsed().as_millis() as i64);
            let passed = exit_code == Some(0);

            // Parse before classifying: positive assertion failures outrank any
            // incidental infrastructure warning in the combined output.
            let parsed = parse_check_output(&plan.command, &output);
            let classification = if substrate_failure.is_some() {
                Some(FailureClassification {
                    kind: CheckFailureKind::Infrastructure,
                    reason: "Cairn: this check produced no verdict because Cairn's own \
                             infrastructure failed"
                        .to_string(),
                    evidence_line: None,
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
            let failure_kind = classification.as_ref().map(|c| c.kind);
            let target_results_json = parsed.as_ref().and_then(|p| serde_json::to_string(p).ok());
            // A substrate failure's `output` is already the message composed for
            // the agent; framing authored text as "evidence" beneath a reason
            // header would only wrap it in a second voice.
            let mut output_tail = if substrate_failure.is_some() {
                tail(&output, OUTPUT_TAIL_CHARS)
            } else {
                classified_output_excerpt(&output, classification.as_ref())
            };

            if failure_kind == Some(CheckFailureKind::Infrastructure) {
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
                let build_service =
                    diagnostic_orch.map(|orch| orch.build_service_diagnostic_snapshot("sccache"));
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

            let publication_role = match publication {
                Some(publication) => Some(publication.acquire().await),
                None => None,
            };
            // Either a sibling already recorded this verdict and named the row it
            // wrote, or this evaluation records it and names that row for every
            // sibling. Both arms end holding the same answer to "what is this
            // verdict's durable identity", which is what the caller reports.
            let (recorded, published_here) = match publication_role {
                Some(crate::fleet::PublicationRole::Published(recorded)) => (recorded, false),
                role => {
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
                        executor_id: provenance.as_ref().map(|meta| meta.executor_id.clone()),
                        executor_device_id: provenance
                            .as_ref()
                            .map(|meta| meta.executor_device_id.clone()),
                        executor_connection_generation: provenance
                            .as_ref()
                            .map(|meta| meta.executor_connection_generation as i64),
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
                        toolchain_fingerprint: Some(check_toolchain_identity().to_string()),
                    };
                    let reuse =
                        check_reuse_decision(&plan.command, passed, failure_kind, parsed.as_ref());
                    let observation = fresh_observation_write(
                        project_id,
                        commit,
                        tree_hash,
                        input_hash,
                        plan,
                        job_id,
                        tool_use_id,
                        exit_code.unwrap_or(-1),
                        failure_kind,
                        duration_ms,
                        &output_tail,
                        provenance.as_ref(),
                        parsed.as_ref(),
                        reuse,
                    );
                    let identity = crate::execution::cache::RecordedCheckObservation {
                        id: observation.id.clone(),
                        environment_fingerprint: observation.environment_fingerprint.clone(),
                        reusable: observation.reusable,
                    };
                    let recorded = match record_fresh_check_observation(db.clone(), observation) {
                        Ok(()) => Some(identity),
                        Err(error) => {
                            log::warn!("failed to record immutable check observation: {error}");
                            None
                        }
                    };
                    // The legacy row is retained only for infrastructure retry/suppression diagnosis.
                    if failure_kind.is_some_and(CheckFailureKind::is_infrastructure) {
                        let _ = store_check_result(db.clone(), legacy_write);
                    }
                    if let Some(crate::fleet::PublicationRole::Publisher(guard)) = role {
                        guard.published(recorded.clone());
                    }
                    (recorded, true)
                }
            };

            // The bound may have just been reached by the store above. Claim the
            // ONE escalation this triple is allowed and, if we won it, put the
            // operator's half of the story where the rest of it already lives.
            if published_here && failure_kind.is_some_and(CheckFailureKind::is_infrastructure) {
                escalate_suppressed_check(
                    db,
                    project_id,
                    &plan.name,
                    input_hash,
                    job_id,
                    tool_use_id,
                    &output_tail,
                );
            }

            let outcome = CheckOutcome {
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
            };
            board.transition(
                index,
                if passed { "passed" } else { "failed" },
                summary_annotation(&outcome),
            );
            (index, outcome)
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
        .map(|o| o.expect("every plan resolved to a hit or a miss outcome"))
        .collect()
}

/// Whether a triple needs a fresh execution: it has no reusable verdict AND it is
/// not already infrastructure-suppressed.
///
/// This is ADVISORY. It keeps a plan that is visibly suppressed from consuming
/// build-slot admission, but it does not decide anything — the authoritative
/// answer is [`crate::execution::cache::claim_check_execution`] in phase 1 of
/// [`run_planned_checks_with_board`], which reserves the attempt in the same
/// statement that grants it. Keeping the decision at the execution seam is what
/// makes the bound hold under two cadences evaluating one triple at once; a
/// filter this far upstream could only ever read state that is already stale by
/// the time the command runs.
///
/// A filter that admits a plan the engine then suppresses is safe: the engine
/// records the suppressed outcome itself, so the batch still settles.
fn needs_execution(db: Arc<LocalDb>, project_id: &str, plan: &CheckPlan, input_hash: &str) -> bool {
    get_exact_reusable_check_result(
        db,
        project_id,
        &plan.name,
        input_hash,
        &plan_environment_fingerprint(plan),
        crate::execution::check_identity::CHECK_RESULT_SCHEMA_VERSION as i64,
    )
    .ok()
    .flatten()
    .is_none()
}

/// Turn a suppressed triple's stored row into this evaluation's outcome, and
/// re-stamp that row onto the current tree so the tree-keyed `/checks` listing
/// still shows the check instead of silently dropping it off the checklist.
///
/// The re-stamp carries `cached: true`, which the cache layer reads as
/// [`crate::execution::cache::InfraStreakOp::Hold`]: nothing executed, so the
/// counter must not move. The row's own `output_tail` is left as the faithful
/// record of the last real infrastructure failure; the suppression is rendered
/// FROM the counter by every surface, rather than overwritten into the evidence.
pub(crate) fn record_suppressed_check(
    db: &Arc<LocalDb>,
    tree_hash: &str,
    job_id: &str,
    check_name: &str,
    entry: CheckResultCacheEntry,
) -> CheckOutcome {
    let streak = entry.infra_failure_streak;
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
        },
    );
    log::info!(
        "check {check_name:?} is infrastructure-suppressed after {streak} consecutive \
         infrastructure failures; not executing it for job {job_id}"
    );
    CheckOutcome {
        name: check_name.to_string(),
        passed: false,
        exit_code: None,
        failure_kind: entry
            .failure_kind
            .as_deref()
            .and_then(CheckFailureKind::from_stored)
            .or(Some(CheckFailureKind::Infrastructure)),
        parsed: None,
        output_tail: suppressed_check_message(streak, &entry.output_tail),
        cached: false,
        duration_ms: 0,
        suppressed_after: Some(streak),
        // Nothing ran, so there is no observation of this evaluation. The
        // re-stamped legacy row is the last real failure's record, not this
        // evaluation's verdict.
        recorded: None,
    }
}

/// Claim and emit the single operator escalation a suppressed triple is allowed.
///
/// It rides the same `check infrastructure failure` log channel that already
/// carries the substrate half of every infrastructure failure, so an operator
/// reads the escalation beside the diagnostics that led to it. It fires once per
/// triple rather than once per evaluation because
/// [`claim_infra_escalation`] settles that in the database.
fn escalate_suppressed_check(
    db: &Arc<LocalDb>,
    project_id: &str,
    check_name: &str,
    input_hash: &str,
    job_id: &str,
    tool_use_id: &str,
    last_diagnostic: &str,
) {
    match claim_infra_escalation(db.clone(), project_id, check_name, input_hash) {
        Ok(true) => log::error!(
            "check infrastructure failure: {}",
            serde_json::json!({
                "escalation": "suppressed",
                "check": check_name,
                "projectId": project_id,
                "inputHash": input_hash,
                "jobId": job_id,
                "suiteId": tool_use_id,
                "bound": crate::execution::cache::OBSERVED_INFRA_FAILURE_BOUND,
                "lastDiagnostic": last_diagnostic,
                "consequence": "Cairn has stopped executing this check for these inputs. It \
                                runs again when its inputs change, or for these same inputs \
                                after the substrate is repaired and Cairn restarts.",
            })
        ),
        Ok(false) => {}
        Err(error) => log::warn!(
            "failed to claim the infrastructure escalation for check {check_name:?}: {error}"
        ),
    }
}

/// Render the inline pass/fail summary appended to the originating tool result.
/// The first line is the compact per-check status
/// (`\u{2713} frontend \u{b7} \u{2717} typecheck (exit 1)`); each failing check
/// then gets a detail block naming the failing tests and a bounded output
/// excerpt, so the agent learns WHAT broke without re-running the suite. Pure, so
/// it is unit-tested directly.
fn format_check_summary(results: &[CheckOutcome]) -> String {
    let header = results
        .iter()
        .map(|o| {
            let mark = if o.passed { '\u{2713}' } else { '\u{2717}' };
            match summary_annotation(o) {
                Some(ann) => format!("{mark} {} ({ann})", o.name),
                None => format!("{mark} {}", o.name),
            }
        })
        .collect::<Vec<_>>()
        .join(" \u{b7} ");

    let mut out = header;
    for o in results.iter().filter(|o| !o.passed) {
        if let Some(detail) = format_check_detail(o) {
            out.push_str("\n\n");
            out.push_str(&detail);
        }
    }
    out
}

/// The parenthetical annotation for one check's status line, or `None` when there
/// is nothing worth adding beyond the bare `\u{2713}`/`\u{2717} <name>`. This is
/// the trust-carrying part of the summary: it turns four indistinguishable greens
/// (a real N-test pass, a zero-selection vacuous pass, a reused cache hit, and a
/// pass whose suite skipped part of itself) into four visibly different lines.
///
/// - Passing TEST-RUNNER check: `12 tests`, or `no tests matched the change`
///   when the selector executed zero tests (a `related` run that matched nothing).
/// - Any skipped test is named: `12 tests, 3 skipped`, and `no tests ran, 12
///   skipped` for a suite that skipped ITSELF entirely — the shape that let a
///   whole cross-surface suite read green while validating nothing (CAIRN-3164).
///   A skip is not a pass, so it can never be silent in the verdict.
/// - Passing non-test check (tsc/api/dead-code): `4.1s` on a fresh run (duration
///   is the only meaningful signal; a test count would be a lie).
/// - Failing TEST-RUNNER check: `2 of 40 failed, exit 101`, or `1 suite failed
///   to load, exit 1` when a file threw during collection and so ran no test.
/// - Failing non-test check: `exit 101`, or `failed to run` on a spawn error.
/// - A cache hit appends `cached` so a reused verdict never masquerades as fresh.
///
/// Pure, so it is unit-tested directly.
fn summary_annotation(o: &CheckOutcome) -> Option<String> {
    // A check Cairn declined to run has no verdict to annotate, and every other
    // arm below would describe one. Say what actually happened instead.
    if let Some(streak) = o.suppressed_after {
        return Some(format!(
            "not run \u{2014} suppressed after {streak} infrastructure failures"
        ));
    }
    let test_parse = o.parsed.as_ref().filter(|p| p.is_test_runner());
    let mut parts: Vec<String> = Vec::new();
    if o.passed {
        match test_parse {
            Some(p) if p.tests_run() == 0 && p.skipped > 0 => {
                parts.push(format!("no tests ran, {} skipped", p.skipped))
            }
            Some(p) if p.tests_run() == 0 => parts.push("no tests matched the change".to_string()),
            Some(p) if p.skipped > 0 => {
                parts.push(format!("{} tests, {} skipped", p.tests_run(), p.skipped))
            }
            Some(p) => parts.push(format!("{} tests", p.tests_run())),
            // Non-test check: duration is the only honest signal, and only on a
            // fresh run (a cache hit's stored duration would be misleading).
            None if !o.cached && o.duration_ms > 0 => {
                parts.push(format_check_duration(o.duration_ms))
            }
            None => {}
        }
    } else if let Some(kind) = o.failure_kind {
        // A classified death renders as itself, never a zero-failure assertion
        // count the agent would chase into tests that never failed.
        if kind == CheckFailureKind::RunnerError {
            let passed = o.parsed.as_ref().map(|parsed| parsed.passed).unwrap_or(0);
            if passed == 0 {
                parts.push("test runner failed before reporting tests".to_string());
            } else {
                parts.push(format!(
                    "test runner failed after {passed} tests passed with no assertion failures"
                ));
            }
        } else {
            parts.push(kind.describe(o.duration_ms));
        }
    } else {
        match test_parse {
            Some(p) => {
                let exit = o
                    .exit_code
                    .map(|c| format!(", exit {c}"))
                    .unwrap_or_default();
                // A file that failed to collect ran no test, so folding it into
                // the assertion tally would read as "0 of 882 failed" — a red
                // check pointing at nothing. Count the two separately.
                let mut segments = Vec::new();
                if p.failed > 0 || p.suite_failures == 0 {
                    segments.push(format!("{} of {} failed", p.failed, p.tests_run()));
                }
                if p.suite_failures > 0 {
                    let noun = if p.suite_failures == 1 {
                        "suite"
                    } else {
                        "suites"
                    };
                    segments.push(format!("{} {noun} failed to load", p.suite_failures));
                }
                parts.push(format!("{}{exit}", segments.join(", ")));
            }
            None => match o.exit_code {
                Some(code) => parts.push(format!("exit {code}")),
                None => parts.push("failed to run".to_string()),
            },
        }
    }
    if o.cached {
        parts.push("cached".to_string());
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join(", "))
    }
}

/// Render a check duration compactly: `4.1s` at or above a second, `850ms` below.
fn format_check_duration(ms: i64) -> String {
    if ms >= 1000 {
        format!("{:.1}s", ms as f64 / 1000.0)
    } else {
        format!("{ms}ms")
    }
}

/// One failing check's detail block: a `\u{2717} <name> \u{2014} N failed: ...`
/// line (when structured names are available) over a fenced, bounded excerpt.
/// `None` when there is nothing to add beyond the header status (no structured
/// names and no output to excerpt). Pure.
/// For a timed-out check, the `N still running at kill: a, b, c +M more` line
/// naming the nextest tests that were mid-flight when the budget expired. `None`
/// when the check did not time out or no running tests could be parsed — the
/// agent's first question is "what was it doing when it died?".
fn timeout_running_names(o: &CheckOutcome) -> Option<String> {
    if o.failure_kind != Some(CheckFailureKind::TimedOut) {
        return None;
    }
    let running = extract_running_tests(&o.output_tail);
    if running.is_empty() {
        return None;
    }
    let shown: Vec<&str> = running
        .iter()
        .take(MAX_FAILURE_NAMES)
        .map(String::as_str)
        .collect();
    let more = running.len().saturating_sub(shown.len());
    let listed = if more > 0 {
        format!("{}, +{more} more", shown.join(", "))
    } else {
        shown.join(", ")
    };
    Some(format!("{} still running at kill: {listed}", running.len()))
}

fn format_check_detail(o: &CheckOutcome) -> Option<String> {
    // A timeout has no failing tests to name, but its still-running tests are
    // exactly the detail the agent needs; fall back to parsed failures otherwise.
    let names =
        timeout_running_names(o).or_else(|| o.parsed.as_ref().and_then(format_failure_names));
    let excerpt = format_failure_excerpt(o.parsed.as_ref(), &o.output_tail);
    let head = match names {
        Some(n) => format!("\u{2717} {} \u{2014} {n}", o.name),
        None if excerpt.trim().is_empty() => return None,
        None => format!("\u{2717} {}:", o.name),
    };
    let mut block = head;
    if !excerpt.trim().is_empty() {
        block.push_str("\n```\n");
        block.push_str(excerpt.trim_end());
        block.push_str("\n```");
    }
    Some(block)
}

/// Last `max_chars` characters of `s`, on a char boundary.
fn tail(s: &str, max_chars: usize) -> String {
    let count = s.chars().count();
    if count <= max_chars {
        return s.to_string();
    }
    s.chars().skip(count - max_chars).collect()
}

/// The fork point a node's write-check impact gate diffs from, resolved live.
///
/// `None` when the job has no branch or an endpoint does not resolve; the
/// caller falls back to the project default tip, which is what this path did
/// for a job with no recorded coordinate at all. What it must never do is start
/// from `jobs.base_commit`: that row is the coordinate the branch was cut at
/// and does not follow a base advance, so a stale one hands the impact gate
/// every file the target merged in the meantime.
async fn live_node_base(orch: &Orchestrator, job_id: &str) -> Option<String> {
    crate::diff::live_job_branch_range(&orch.db.local, job_id, &orch.config_dir)
        .await
        .map_err(|error| {
            log::debug!("node {job_id} write checks: no live base coordinate ({error})");
        })
        .ok()
        .flatten()
        .map(|range| range.base)
}

async fn submit_review_check(
    orch: &Orchestrator,
    result_identity: crate::execution::cache::CheckResultIdentity,
    request: CellRequest,
) -> Result<CheckExecResult, CheckExecutionFailure> {
    let build_service = orch.build_service_diagnostic_snapshot("sccache");
    if let Some(failure) = active_build_service_failure(&build_service) {
        return Err(CheckExecutionFailure::Substrate(
            SubstrateFailure::new(SubstrateFailureShape::Dispatch, failure)
                .implicating_build_service(),
        ));
    }
    let submitted = match orch
        .fleet
        .submit_pure_verdict(orch, result_identity, request)
        .await
    {
        Ok(submitted) => submitted,
        Err(outcome) => {
            return Err(check_result_from_cell_outcome(outcome, None).unwrap_err());
        }
    };
    check_result_from_cell_outcome(submitted.outcome, Some(submitted.publication))
}

/// Append the shared build-cache service's advisory to a verdict — but only for
/// a failure the service can plausibly explain.
///
/// Two failure classes qualify. A check whose OWN output looked like an abnormal
/// build reaches [`CheckFailureKind::Infrastructure`] through
/// [`classify_check_failure`]'s evidence arms, which is exactly the shape a sick
/// cache daemon produces; it carries no [`SubstrateFailure`], so `substrate` is
/// `None`. And a dispatch refusal the service itself caused is marked
/// [`SubstrateFailure::implicating_build_service`] where it is composed.
///
/// Every other substrate failure — a cell's storage cleanup, a capacity
/// deadline, a cancellation, a lost result — is unrelated to the cache, and
/// attaching "this commonly causes failures of this kind" to one would assert a
/// cause that is not one, however sick the daemon happens to be at that moment.
/// The snapshot itself is operator vocabulary and rides the log record
/// regardless.
/// What the log says when an infrastructure failure carried no substrate
/// evidence at all — itself the finding, rather than a silently absent key.
const NO_SUBSTRATE_DIAGNOSTIC: &str = "no substrate diagnostic recorded";

/// The operator-facing line for a check that failed on infrastructure.
///
/// The cause leads and the environment follows, because the reader's first
/// question is always "what broke" and only their second is "what else was
/// true at the time". Sorted into the environment object, the diagnostic
/// trailed kilobytes of build-service fingerprint and process table — present,
/// unreadable, and so unread: a `bun: command not found` that would have been
/// diagnosed at a glance instead went unnoticed across an evening of failed
/// review checks.
fn infrastructure_failure_log_line(
    check: &str,
    substrate_diagnostic: Option<&str>,
    environment: serde_json::Value,
) -> String {
    format!(
        "check infrastructure failure: {check}: {}\nenvironment: {environment}",
        substrate_diagnostic.unwrap_or(NO_SUBSTRATE_DIAGNOSTIC)
    )
}

fn with_build_service_advisory(
    output_tail: String,
    build_service: Option<&crate::orchestrator::build_services::BuildServiceDiagnosticSnapshot>,
    substrate: Option<&SubstrateFailure>,
) -> String {
    if substrate.is_some_and(|failure| !failure.build_service_implicated()) {
        return output_tail;
    }
    let Some(advisory) = build_service.and_then(|snapshot| snapshot.agent_advisory()) else {
        return output_tail;
    };
    // The remainder resumes where the head stopped, so a short verdict is not
    // printed twice around the advisory.
    let head: String = output_tail.chars().take(1_500).collect();
    let remainder: String = output_tail.chars().skip(1_500).collect();
    let reserved = head.chars().count() + advisory.chars().count() + 4;
    let rest = tail(&remainder, OUTPUT_TAIL_CHARS.saturating_sub(reserved));
    if rest.is_empty() {
        format!("{head}\n\n{advisory}")
    } else {
        format!("{head}\n\n{advisory}\n\n{rest}")
    }
}

/// The operator-facing diagnostic for a build-service failure that must stop
/// check dispatch, or `None`. The agent-facing half of the same failure is
/// composed by [`SubstrateFailure`] at the call site.
fn active_build_service_failure(
    snapshot: &crate::orchestrator::build_services::BuildServiceDiagnosticSnapshot,
) -> Option<String> {
    snapshot
        .current_failure()
        .map(|failure| format!("sccache port conflict: {failure}"))
}

fn append_sandbox_denial_evidence(
    output: &mut String,
    denials: &[cairn_common::executor_protocol::SandboxDenialEvidence],
) {
    for evidence in denials {
        if !output.is_empty() {
            output.push_str("\n\n");
        }
        let scope = match &evidence.denial {
            cairn_common::executor_protocol::SandboxDenial::Path(path) => {
                format!("path={path}")
            }
            cairn_common::executor_protocol::SandboxDenial::Command => "scope=command".to_string(),
        };
        output.push_str(&format!(
            "Cairn sandbox denial evidence: operation={}, {scope}, command={}, stream={}",
            evidence.operation.as_deref().unwrap_or("unknown"),
            evidence.command,
            evidence.stream_id,
        ));
    }
}

fn append_tracked_modification_evidence(
    output: &mut String,
    evidence: Option<&cairn_common::executor_protocol::TrackedModificationEvidence>,
) {
    let Some(evidence) = evidence else { return };
    if !output.is_empty() {
        output.push_str("\n\n");
    }

    output.push_str(&format!(
        "note: check modified tracked paths: {} ({} files, +{} -{}); changes were discarded",
        evidence.paths.join(", "),
        evidence.files_changed,
        evidence.lines_added,
        evidence.lines_deleted,
    ));
}

fn check_result_from_cell_outcome(
    outcome: CellOutcome,
    publication: Option<crate::fleet::PublicationCoordination>,
) -> Result<CheckExecResult, CheckExecutionFailure> {
    match outcome {
        CellOutcome::Completed {
            exit_code,
            mut output,
            timed_out,
            metadata,
            mutation_delta: None,
            sandbox_denials,
            tracked_modifications,
            ..
        } => {
            let duration_ms = metadata
                .duration_ms
                .map(|duration| i64::try_from(duration).unwrap_or(i64::MAX));
            append_sandbox_denial_evidence(&mut output, &sandbox_denials);
            append_tracked_modification_evidence(&mut output, tracked_modifications.as_ref());
            Ok(CheckExecResult {
                exit_code,
                output,
                timed_out,
                duration_ms,
                provenance: Some(metadata),
                publication,
            })
        }
        CellOutcome::Completed {
            mutation_delta: Some(delta),
            ..
        } => Err(CheckExecutionFailure::substrate(
            SubstrateFailureShape::Result,
            format!(
                "cell produced mutation delta {} based on {}",
                delta.delta_commit, delta.base_commit
            ),
        )),
        CellOutcome::FailedAfterExecution { diagnostic, .. } => {
            Err(CheckExecutionFailure::substrate(
                SubstrateFailureShape::Result,
                format!("slot result publication failed: {diagnostic}"),
            ))
        }
        CellOutcome::Unavailable {
            reason:
                cairn_common::executor_protocol::CellUnavailableReason::Deadline {
                    host_pressure,
                    substrate: Some(substrate),
                },
            diagnostic,
        } => {
            let now = unix_time_ms_for_checks();
            let mut facts = vec![format!(
                "substrate={:?}, lastProgressAge={}ms",
                substrate.state,
                now.saturating_sub(substrate.last_progress_unix_ms)
            )];
            if let Some(depth) = substrate.queue_depth {
                facts.push(format!("queueDepth={depth}"));
            }
            if let Some(position) = substrate.queue_position {
                facts.push(format!("queuePosition={position}"));
            }
            if let Some(active) = substrate.active_cell_count {
                facts.push(format!("activeSlots={active}"));
            }
            if let Some(started) = substrate.oldest_running_started_at_unix_ms {
                facts.push(format!(
                    "oldestRunningAge={}ms",
                    now.saturating_sub(started)
                ));
            }
            if let Some(pressure) = host_pressure {
                facts.push(format!("hostPressure={pressure:?}"));
            }
            Err(CheckExecutionFailure::substrate(
                SubstrateFailureShape::Capacity,
                format!("build-slot deadline ({}) — {diagnostic}", facts.join(", ")),
            ))
        }
        CellOutcome::Unavailable { reason, diagnostic } => Err(CheckExecutionFailure::substrate(
            no_start_shape(&reason),
            format!("{reason:?}: {diagnostic}"),
        )),
        CellOutcome::StorageFailure {
            stage,
            kind,
            diagnostic,
            ..
        } => Err(CheckExecutionFailure::substrate(
            SubstrateFailureShape::Storage,
            format!("storage failure ({stage:?}/{kind:?}): {diagnostic}"),
        )),
        CellOutcome::Cancelled { .. } => Err(CheckExecutionFailure::substrate(
            SubstrateFailureShape::Cancelled,
            "cell request was cancelled",
        )),
    }
}

#[derive(Debug)]
pub(crate) enum ReviewTreeGateResult {
    Green,
    CheckFailed { name: String, detail: String },
    InfrastructureFailure(String),
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn verify_review_tree(
    orch: &Orchestrator,
    project_id: &str,
    repository: &str,
    planning_root: &Path,
    commit_id: &str,
    tree_hash: &str,
    tree_entries: &[(String, String)],
    changed: &[GraphFileChange],
    requesting_job_id: &str,
    priority: CellPriority,
) -> ReviewTreeGateResult {
    if changed.is_empty() {
        return ReviewTreeGateResult::Green;
    }
    // The gate's definition comes from the PROSPECTIVE commit it is verifying,
    // not from the project checkout: the combined tree is what merges, so the
    // checks it must satisfy are the ones that tree declares.
    let Some(CommitChecksContract {
        contract: ChecksContract {
            checks,
            extra_inputs,
        },
        defined_by_commit,
    }) = load_checks_contract_at_commit(Path::new(repository), commit_id).await
    else {
        return ReviewTreeGateResult::Green;
    };
    assert_eq!(
        defined_by_commit, commit_id,
        "a combined-tree gate must be defined by the tree it verifies"
    );
    // The gate evaluates the PROSPECTIVE tree, so its input closures are derived
    // from that tree's manifests — the same entries it keys verdicts by.
    let gate_jj = JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
    let gate_repository = PathBuf::from(repository);
    let blobs = TreeBlobs {
        jj: &gate_jj,
        repository: &gate_repository,
    };
    let snapshot = TreeSnapshot::new(Some(tree_entries), &blobs);
    let inputs = ResolvedInputs::resolve(&checks, &extra_inputs, &snapshot);
    let mut plans = applicable_combined_tree_gate_checks(&checks, &inputs, changed, planning_root);
    if plans.is_empty() {
        return ReviewTreeGateResult::Green;
    }

    let platform = check_platform_identity();
    let toolchain = check_toolchain_identity();
    let mut keyed = Vec::with_capacity(plans.len());
    for plan in plans.drain(..) {
        let configured = checks
            .get(&plan.name)
            .expect("planned check must retain its configured definition");
        let key = check_result_key(
            configured,
            inputs.for_check(&plan.name),
            Some(tree_entries),
            tree_hash,
            &platform,
            toolchain,
        );
        keyed.push((plan, key));
    }

    let changed_path =
        std::env::temp_dir().join(format!("cairn-merge-checks-{}.txt", uuid::Uuid::new_v4()));
    let changed_body = changed
        .iter()
        .map(|change| change.path.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    if let Err(error) = std::fs::write(&changed_path, changed_body) {
        return ReviewTreeGateResult::InfrastructureFailure(format!(
            "failed to prepare changed-file input: {error}"
        ));
    }
    let env = vec![(
        "CAIRN_CHECK_CHANGED_FILES".to_string(),
        changed_path.to_string_lossy().into_owned(),
    )];
    let timeouts: Vec<u32> = keyed
        .iter()
        .map(|(plan, _)| {
            resolve_check_timeout_ms(checks.get(&plan.name), DEFAULT_REVIEW_CHECK_TIMEOUT_MS)
        })
        .collect();
    let command_identity_keys: Vec<_> = keyed
        .iter()
        .map(|(plan, _)| {
            check_resource_identity(
                &plan.name,
                checks
                    .get(&plan.name)
                    .expect("planned check must retain its configured definition"),
            )
            .key
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
    // One horizon for the whole wave: these are the items of one planned check
    // run, and staggering their willingness to wait by whenever each closure
    // happened to run would make the ordering of a fan-out decide which checks
    // get a verdict.
    let wait_horizon_unix_ms = crate::fleet::default_wait_horizon_unix_ms(
        &crate::config::settings::load_fleet(&orch.config_dir),
    );
    let project = project_id.to_string();
    let repository = repository.to_string();
    let commit = commit_id.to_string();
    let job = requesting_job_id.to_string();
    let resolved_owner =
        crate::execution::checks_turn_end::resolve_job_coords(&orch.db.local, requesting_job_id)
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
    let outcomes = run_planned_checks_at_commit(
        orch.db.local.clone(),
        project_id,
        CheckRunCommit {
            evaluated: commit_id,
            defined_by: &defined_by_commit,
        },
        tree_hash,
        requesting_job_id,
        &keyed,
        &format!("merge-gate:{requesting_job_id}"),
        CheckExecMode::Isolated,
        Some(orch),
        move |index, command, _| {
            let project = project.clone();
            let repository = repository.clone();
            let commit = commit.clone();
            let job = job.clone();
            let owner = resolved_owner.clone();
            let env = env.clone();
            let timeout_ms = timeouts[index];
            let executor = executors[index].clone();
            let command_resource_identity = CommandResourceIdentity {
                version: cairn_common::executor_protocol::COMMAND_RESOURCE_IDENTITY_VERSION,
                key: command_identity_keys[index].clone(),
            };
            let resource_reservation =
                declared_check_reservation(keyed_ref[index].0.resource_class);
            async move {
                let request = CellRequest {
                    request_id: uuid::Uuid::new_v4().to_string(),
                    attempt_id: uuid::Uuid::new_v4().to_string(),
                    project_id: project.clone(),
                    repository: cairn_common::executor_protocol::RepositoryLocator::ColocatedPath {
                        project_id: project.clone(),
                        repository_id: project.clone(),
                        absolute_path: repository,
                    },
                    base_commit: commit,
                    command_class: CellCommandClass::classify(&command),
                    command,
                    owner,
                    cwd: String::new(),
                    env,
                    priority,
                    wait_horizon_unix_ms,
                    waiting_since_unix_ms: unix_time_ms_for_checks(),
                    timeout_ms,
                    mutation_policy: MutationPolicy::PureVerdict,
                    requesting_job_id: Some(job),
                    affinity_key: None,
                    executor,
                    pinned_executor_id: None,
                    placement_mobility: Default::default(),
                    command_resource_identity: Some(command_resource_identity),
                    resource_reservation,
                    learned_estimate: None,
                };
                submit_review_check(
                    orch,
                    crate::execution::cache::CheckResultIdentity::new(
                        &project,
                        &keyed_ref[index].0.name,
                        &keyed_ref[index].1,
                    ),
                    request,
                )
                .await
            }
        },
        |_| {},
    )
    .await;
    let _ = std::fs::remove_file(changed_path);

    review_tree_gate_result(outcomes)
}

fn review_tree_gate_result(outcomes: Vec<CheckOutcome>) -> ReviewTreeGateResult {
    for outcome in outcomes {
        if outcome.passed {
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
    ReviewTreeGateResult::Green
}

fn unix_time_ms_for_checks() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    /// An operator reading the log finds the cause in the first line, ahead of
    /// an environment dump that can run to kilobytes. This ordering is the whole
    /// point: the same failure, with its diagnostic sorted into the dump, went
    /// undiagnosed while every review check on the host failed.
    #[test]
    fn infrastructure_failure_log_leads_with_the_cause() {
        use super::infrastructure_failure_log_line;

        let line = infrastructure_failure_log_line(
            "rust-lint",
            Some(
                "Preparation: cell preparation command failed: bun i\nstderr:\n/bin/sh: bun: command not found\n\nExit code: 127",
            ),
            serde_json::json!({
                "buildService": { "configFingerprint": "x".repeat(4_096) },
                "processTree": ["a", "b"],
            }),
        );

        let first = line.lines().next().unwrap();
        assert!(first.contains("rust-lint"), "first line: {first}");
        assert!(
            first.contains("cell preparation command failed: bun i"),
            "first line: {first}"
        );
        assert!(
            line.find("buildService") > line.find("bun: command not found"),
            "the environment dump must follow the cause, not bury it"
        );
    }

    /// A missing diagnostic is a finding, not an absence to be silently skipped:
    /// it says the failure reached the log with no substrate evidence attached.
    #[test]
    fn infrastructure_failure_log_names_a_missing_diagnostic() {
        use super::{infrastructure_failure_log_line, NO_SUBSTRATE_DIAGNOSTIC};

        let line = infrastructure_failure_log_line("lint", None, serde_json::json!({}));
        assert!(line
            .lines()
            .next()
            .unwrap()
            .contains(NO_SUBSTRATE_DIAGNOSTIC));
    }

    /// Eligibility is a property of what the work does, not of what it asked
    /// for. Both a constrained and an unconstrained pure-verdict group are free
    /// to move; a mutating group never is, whatever it stated.
    #[test]
    fn only_pure_verdict_check_batches_are_free_to_move() {
        use super::batch_placement_mobility;
        use crate::fleet::MutationPolicy;
        use cairn_common::executor_protocol::PlacementMobility;

        assert_eq!(
            batch_placement_mobility(&MutationPolicy::PureVerdict),
            PlacementMobility::SpillEligible
        );
        assert_eq!(
            batch_placement_mobility(&MutationPolicy::AllowDelta),
            PlacementMobility::PinnedOrColocated,
            "a batch whose delta has to come back cannot be placed away from the tree it mutates"
        );
    }

    /// The selector partition runs before dispatch, so a group that named two
    /// contradictory machines is refused as the configuration error it is rather
    /// than becoming a placement problem.
    #[test]
    fn contradictory_selectors_within_a_group_refuse_before_dispatch() {
        use super::{merge_batch_executor, PlannedCheckBatchItem};
        use cairn_common::executor_protocol::ExecutorSelector;

        let named = |name: &str| ExecutorSelector {
            name: Some(name.into()),
            ..ExecutorSelector::default()
        };
        let item = |name: &str| PlannedCheckBatchItem {
            executor: Some(named(name)),
            ..batch_item("true", super::CheckResourceClass::Shared)
        };
        let refusal = merge_batch_executor(&[item("bglab-ub"), item("bglab-mac")]).unwrap_err();
        assert!(refusal.contains("bglab-ub"), "{refusal}");
        assert!(refusal.contains("bglab-mac"), "{refusal}");
    }

    use super::*;

    /// Every check's selector, resolved with NO tree in hand. Glob and
    /// no-input checks resolve exactly as they do in production; a `scope:`
    /// check would degrade to the conservative whole-tree selector, which the
    /// tests below do not exercise (see `execution::inputs` for those).
    fn inputs(checks: &HashMap<String, CheckCommand>) -> ResolvedInputs {
        ResolvedInputs::resolve(checks, &HashMap::new(), &TreeSnapshot::empty())
    }

    fn inputs_for(name: &str, check: &CheckCommand) -> ResolvedInputs {
        inputs(&HashMap::from([(name.to_string(), check.clone())]))
    }

    fn selector(check: &CheckCommand) -> InputSelector {
        crate::execution::inputs::resolve_one(check, &HashMap::new(), &TreeSnapshot::empty())
    }

    #[test]
    fn slot_check_env_enforces_line_tables_only_without_mutating_agent_env() {
        let agent_env = vec![
            ("PATH".to_string(), "/tools".to_string()),
            (SLOT_CHECK_DEV_DEBUG_ENV.0.to_string(), "full".to_string()),
        ];
        let slot_env = slot_check_env(agent_env.clone());

        assert_eq!(
            slot_env
                .iter()
                .find(|(key, _)| key == SLOT_CHECK_DEV_DEBUG_ENV.0)
                .map(|(_, value)| value.as_str()),
            Some(SLOT_CHECK_DEV_DEBUG_ENV.1)
        );
        assert_eq!(
            agent_env
                .iter()
                .find(|(key, _)| key == SLOT_CHECK_DEV_DEBUG_ENV.0)
                .map(|(_, value)| value.as_str()),
            Some("full")
        );
    }

    #[test]
    fn manual_contract_snapshot_refuses_command_changed_at_execution_seam() {
        let original = plan("race", "command-a");
        let snapshot_command = original.command.clone();
        let snapshot_identity = check_resource_identity(
            "race",
            &serde_yaml::from_str::<CheckCommand>("command: command-a\n").unwrap(),
        )
        .key;

        // This models Settings replacing the live contract after planning. The
        // operation carries only owned snapshot values across the seam.
        let changed = plan("race", "command-b");
        let changed_identity = check_resource_identity(
            "race",
            &serde_yaml::from_str::<CheckCommand>("command: command-b\n").unwrap(),
        )
        .key;

        assert_ne!(snapshot_identity, changed_identity);
        assert!(require_snapshot_command(&snapshot_command, "command-a").is_ok());
        assert!(require_snapshot_command(&snapshot_command, &changed.command).is_err());
    }

    #[test]
    fn manual_producer_uses_the_same_observation_shape_with_manual_provenance() {
        let plan = plan("rust-tests", "bun run test:rust");
        let write = fresh_observation_write(
            "project",
            CheckRunCommit {
                evaluated: "commit",
                defined_by: "commit",
            },
            "tree",
            "input",
            &plan,
            "job",
            "manual-check:run-123:rust-tests",
            0,
            None,
            12,
            "ok",
            None,
            None,
            CheckReuseDecision {
                reusable: true,
                reason: None,
            },
        );
        assert_eq!(write.cadence, "manual");
        assert_eq!(write.run_id.as_deref(), Some("run-123"));
        assert_eq!(write.commit_sha, "commit");
        assert_eq!(write.check_name, "rust-tests");
    }

    #[test]
    fn dirty_manual_cache_context_rejects_store() {
        let context = ManualCheckCacheContext {
            project_id: "project".into(),
            job_id: "job".into(),
            commit_sha: "commit".into(),
            tree_hash: "tree".into(),
            input_hash: "input".into(),
            cacheable: false,
            entry: None,
        };
        assert!(context.require_cacheable().is_err());
        assert!(ManualCheckCacheContext {
            cacheable: true,
            ..context
        }
        .require_cacheable()
        .is_ok());
    }
    use crate::config::project_settings::{CheckPolicy, CheckResourceClass};
    use crate::db::DbState;
    use crate::execution::selection::CheckScope;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{LocalDb, MigrationRunner, SearchIndex, TURSO_MIGRATIONS};
    use std::process::Command;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tempfile::TempDir;

    async fn test_orchestrator(config_dir: &Path) -> Orchestrator {
        let local = LocalDb::open(config_dir.join("checks.db")).await.unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&local)
            .await
            .unwrap();
        let search = Arc::new(SearchIndex::open_or_create(config_dir.join("search")).unwrap());
        Orchestrator::builder(
            Arc::new(DbState::new(Arc::new(local), search)),
            Arc::new(TestServicesBuilder::new().build()),
            config_dir.to_path_buf(),
        )
        .build()
    }

    #[tokio::test]
    async fn write_check_guard_cleans_up_on_drop_and_counts_overlap() {
        let temp = TempDir::new().unwrap();
        let orch = test_orchestrator(temp.path()).await;
        let first = WriteChecksInFlightGuard::new(&orch, "job-a");
        let second = WriteChecksInFlightGuard::new(&orch, "job-a");
        assert!(orch.write_checks_in_flight("job-a"));

        drop(first);
        assert!(
            orch.write_checks_in_flight("job-a"),
            "one dropped future must not clear an overlapping write batch",
        );
        drop(second);
        assert!(
            !orch.write_checks_in_flight("job-a"),
            "dropping the final future must clear sidebar state",
        );
    }

    fn git(repo: &Path, args: &[&str]) -> String {
        let output = Command::new("git")
            .args(args)
            .current_dir(repo)
            .output()
            .expect("git should run in cache-key fixture");
        assert!(
            output.status.success(),
            "git {:?} failed: {}",
            args,
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout).trim().to_string()
    }

    fn git_tree_entries(repo: &Path, revision: &str) -> Vec<(String, String)> {
        git(repo, &["ls-tree", "-r", revision])
            .lines()
            .map(|line| {
                let (metadata, path) = line.split_once('\t').expect("ls-tree row has a path");
                let blob = metadata
                    .split_whitespace()
                    .nth(2)
                    .expect("ls-tree row has an object id");
                (path.to_string(), blob.to_string())
            })
            .collect()
    }

    fn check_ref_exists(repo: &Path, reference: &str) -> bool {
        Command::new("git")
            .args(["show-ref", "--verify", "--quiet", reference])
            .current_dir(repo)
            .status()
            .unwrap()
            .success()
    }

    fn committed_git_repo(temp: &TempDir) -> (std::path::PathBuf, String) {
        let repo = temp.path().join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        git(&repo, &["init", "-q"]);
        std::fs::write(repo.join("checked.txt"), "sealed").unwrap();
        git(&repo, &["add", "checked.txt"]);
        git(
            &repo,
            &[
                "-c",
                "user.name=Cairn Test",
                "-c",
                "user.email=test@cairn.local",
                "commit",
                "-q",
                "-m",
                "sealed",
            ],
        );
        let commit = git(&repo, &["rev-parse", "HEAD"]);
        (repo, commit)
    }

    #[tokio::test]
    async fn post_publication_verification_failure_removes_temporary_check_ref() {
        let temp = tempfile::tempdir().unwrap();
        let orch = test_orchestrator(temp.path()).await;
        let (repo, commit) = committed_git_repo(&temp);
        let store = temp.path().join("store");
        let request_id = "verification-failure";
        let reference = format!("refs/cairn/checks/{request_id}");

        let error = publish_check_commit_ref_with_verifier(
            &orch,
            &repo,
            &store,
            &commit,
            request_id,
            |_repo, _reference| Err("injected post-publication verification failure".into()),
        )
        .await
        .unwrap_err();

        assert!(error.contains("injected post-publication verification failure"));
        assert!(!check_ref_exists(&repo, &reference));
    }

    #[tokio::test]
    async fn dropped_temporary_check_ref_retries_cleanup_under_store_lock() {
        let temp = tempfile::tempdir().unwrap();
        let orch = test_orchestrator(temp.path()).await;
        let (repo, commit) = committed_git_repo(&temp);
        let store = temp.path().join("store");
        let reference = "refs/cairn/checks/cancelled".to_string();
        git(&repo, &["update-ref", &reference, &commit]);
        let held = orch
            .acquire_jj_store_lock(&store, "hold cancellation cleanup test")
            .await;

        drop(TemporaryCheckRef {
            orch: orch.clone(),
            repository: repo.clone(),
            store_dir: store,
            reference: reference.clone(),
            commit,
            armed: true,
        });
        tokio::task::yield_now().await;
        assert!(
            check_ref_exists(&repo, &reference),
            "cleanup must not bypass the held store lock"
        );
        drop(held);

        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while check_ref_exists(&repo, &reference) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancellation cleanup removes the ref after acquiring the store lock");
    }

    #[test]
    fn result_key_is_impact_filtered_and_includes_definition_platform_and_toolchain() {
        let mut definition = check("cargo test", Some(&["src/**"]), CheckWhen::Review);
        let base = vec![
            ("src/lib.rs".to_string(), "blob-a".to_string()),
            ("docs/readme.md".to_string(), "docs-a".to_string()),
        ];
        let key = check_result_key(
            &definition,
            &selector(&definition),
            Some(&base),
            "whole-tree-a",
            "macos-aarch64",
            "rustc=1;bun=1",
        );

        let mut changed_definition = definition.clone();
        changed_definition.command = "cargo test --all".to_string();
        assert_ne!(
            key,
            check_result_key(
                &changed_definition,
                &selector(&changed_definition),
                Some(&base),
                "whole-tree-a",
                "macos-aarch64",
                "rustc=1;bun=1"
            )
        );
        changed_definition = definition.clone();
        changed_definition.policy = CheckPolicy::Gate;
        assert_ne!(
            key,
            check_result_key(
                &changed_definition,
                &selector(&changed_definition),
                Some(&base),
                "whole-tree-a",
                "macos-aarch64",
                "rustc=1;bun=1"
            )
        );
        changed_definition = definition.clone();
        changed_definition.resource_class = CheckResourceClass::Exclusive;
        assert_ne!(
            key,
            check_result_key(
                &changed_definition,
                &selector(&changed_definition),
                Some(&base),
                "whole-tree-a",
                "macos-aarch64",
                "rustc=1;bun=1"
            )
        );
        changed_definition = definition.clone();
        changed_definition.timeout = Some(42);
        assert_ne!(
            key,
            check_result_key(
                &changed_definition,
                &selector(&changed_definition),
                Some(&base),
                "whole-tree-a",
                "macos-aarch64",
                "rustc=1;bun=1"
            )
        );
        changed_definition = definition.clone();
        changed_definition.executor = Some(cairn_common::executor_protocol::ExecutorSelector {
            name: Some("bglab-mac".to_string()),
            os: None,
            required_toolchains: vec!["rust".to_string()],
        });
        assert_ne!(
            key,
            check_result_key(
                &changed_definition,
                &selector(&changed_definition),
                Some(&base),
                "whole-tree-a",
                "macos-aarch64",
                "rustc=1;bun=1"
            )
        );
        assert_ne!(
            key,
            check_result_key(
                &definition,
                &selector(&definition),
                Some(&base),
                "whole-tree-a",
                "linux-x86_64",
                "rustc=1;bun=1"
            )
        );
        assert_ne!(
            key,
            check_result_key(
                &definition,
                &selector(&definition),
                Some(&base),
                "whole-tree-a",
                "macos-aarch64",
                "rustc=2;bun=1"
            )
        );

        let inside_changed = vec![
            ("src/lib.rs".to_string(), "blob-b".to_string()),
            ("docs/readme.md".to_string(), "docs-a".to_string()),
        ];
        assert_ne!(
            key,
            check_result_key(
                &definition,
                &selector(&definition),
                Some(&inside_changed),
                "whole-tree-b",
                "macos-aarch64",
                "rustc=1;bun=1"
            )
        );
        let outside_changed = vec![
            ("src/lib.rs".to_string(), "blob-a".to_string()),
            ("docs/readme.md".to_string(), "docs-b".to_string()),
        ];
        assert_eq!(
            key,
            check_result_key(
                &definition,
                &selector(&definition),
                Some(&outside_changed),
                "whole-tree-b",
                "macos-aarch64",
                "rustc=1;bun=1"
            )
        );

        definition.impact = Some(vec!["docs/**".to_string()]);
        assert_ne!(
            key,
            check_result_key(
                &definition,
                &selector(&definition),
                Some(&base),
                "whole-tree-a",
                "macos-aarch64",
                "rustc=1;bun=1"
            )
        );
    }

    // --- keying by the derived closure --------------------------------------

    /// The repository's real manifests plus the source files a case needs, as a
    /// sealed tree. `version` distinguishes two trees whose source differs.
    fn workspace_tree(sources: &[(&str, &str)]) -> crate::execution::inputs::fixtures::TreeFixture {
        let mut fixture = crate::execution::inputs::fixtures::real_workspace();
        for (path, version) in sources {
            fixture = fixture.source(path, version);
        }
        fixture
    }

    fn scoped(command: &str, tokens: &[&str]) -> CheckCommand {
        let mut definition = check(command, None, CheckWhen::Review);
        definition.scope = Some(crate::config::project_settings::CheckScopeSelector::Many(
            tokens.iter().map(|token| token.to_string()).collect(),
        ));
        definition
    }

    /// This check's key over this tree, with its selector resolved from the same
    /// tree — exactly what the runners do.
    fn scoped_key(
        definition: &CheckCommand,
        extra_inputs: &HashMap<String, Vec<String>>,
        fixture: &crate::execution::inputs::fixtures::TreeFixture,
    ) -> String {
        let entries = fixture.entries();
        let snapshot = TreeSnapshot::new(Some(&entries), fixture);
        let selector = crate::execution::inputs::resolve_one(definition, extra_inputs, &snapshot);
        check_result_key(
            definition,
            &selector,
            Some(&entries),
            "whole-tree",
            TEST_PLATFORM,
            TEST_TOOLCHAIN,
        )
    }

    #[test]
    fn a_change_outside_a_scoped_check_s_closure_reuses_its_verdict() {
        let rust = scoped("bun run test:rust", &["rust:cairn-core"]);
        let no_extra = HashMap::new();
        let before = workspace_tree(&[
            ("src/App.tsx", "v1"),
            ("src-tauri/os/cairn-core/src/lib.rs", "v1"),
        ]);
        let frontend_only = workspace_tree(&[
            ("src/App.tsx", "v2"),
            ("src-tauri/os/cairn-core/src/lib.rs", "v1"),
        ]);
        assert_eq!(
            scoped_key(&rust, &no_extra, &before),
            scoped_key(&rust, &no_extra, &frontend_only),
            "a frontend-only commit changes no Rust input, so the verdict is reused"
        );

        // And a change INSIDE the closure moves the key, including one in a
        // transitive dependency the check never names.
        for path in [
            "src-tauri/os/cairn-core/src/lib.rs",
            "src-tauri/os/cairn-vcs/src/lib.rs",
            "src-tauri/Cargo.lock",
        ] {
            let changed = workspace_tree(&[
                ("src/App.tsx", "v1"),
                ("src-tauri/os/cairn-core/src/lib.rs", "v1"),
                (path, "v2"),
            ]);
            assert_ne!(
                scoped_key(&rust, &no_extra, &before),
                scoped_key(&rust, &no_extra, &changed),
                "{path} is an input of rust:cairn-core and must produce a new key"
            );
        }
    }

    #[test]
    fn retargeting_a_scope_or_an_extra_input_produces_a_new_key() {
        let tree = workspace_tree(&[("src-tauri/os/cairn-core/src/lib.rs", "v1")]);
        let no_extra = HashMap::new();
        let core = scoped("bun run test:rust", &["rust:cairn-core"]);
        let cmd = scoped("bun run test:rust", &["rust:cairn-cmd"]);
        assert_ne!(
            scoped_key(&core, &no_extra, &tree),
            scoped_key(&cmd, &no_extra, &tree),
            "the selector definition is part of the key"
        );

        let with_extra = HashMap::from([(
            "rust:cairn-db".to_string(),
            vec!["src-tauri/turso_migrations/**".to_string()],
        )]);
        assert_ne!(
            scoped_key(&core, &no_extra, &tree),
            scoped_key(&core, &with_extra, &tree),
            "declaring an extra input its closure reaches re-keys the check"
        );
    }

    #[test]
    fn a_migration_is_an_input_of_every_check_whose_closure_reaches_cairn_db() {
        let extra_inputs = HashMap::from([(
            "rust:cairn-db".to_string(),
            vec!["src-tauri/turso_migrations/**".to_string()],
        )]);
        let core = scoped("bun run test:rust", &["rust:cairn-core"]);
        let cmd = scoped("bun run test:rust", &["rust:cairn-cmd"]);
        let before = workspace_tree(&[("src-tauri/turso_migrations/0084_x.sql", "v1")]);
        let after = workspace_tree(&[("src-tauri/turso_migrations/0084_x.sql", "v2")]);
        assert_ne!(
            scoped_key(&core, &extra_inputs, &before),
            scoped_key(&core, &extra_inputs, &after)
        );
        assert_eq!(
            scoped_key(&cmd, &extra_inputs, &before),
            scoped_key(&cmd, &extra_inputs, &after),
            "cairn-cmd never compiles the migrations in"
        );
    }

    #[test]
    fn a_config_error_runs_nothing_and_caches_nothing() {
        let mut both = check(
            "bun run test:rust",
            Some(&["src-tauri/**"]),
            CheckWhen::Write,
        );
        both.scope = Some(crate::config::project_settings::CheckScopeSelector::One(
            "rust:cairn-core".to_string(),
        ));
        let map = HashMap::from([("rust".to_string(), both)]);
        let plans = plan_checks(
            &map,
            &inputs(&map),
            &[change("src-tauri/os/cairn-core/src/lib.rs")],
            Path::new("/repo"),
        );
        let plan = plans.first().expect("one plan");
        let error = plan
            .config_error
            .as_deref()
            .expect("a check with two input definitions is unrunnable");
        assert!(error.contains("impact"));
        assert!(error.contains("scope"));
    }

    #[test]
    fn resource_identity_is_stable_across_tree_states() {
        let definition = check("cargo test", Some(&["src/**"]), CheckWhen::Review);
        assert_ne!(
            check_result_key(
                &definition,
                &selector(&definition),
                None,
                "tree-one",
                "platform",
                "toolchain"
            ),
            check_result_key(
                &definition,
                &selector(&definition),
                None,
                "tree-two",
                "platform",
                "toolchain"
            ),
        );
        assert_eq!(
            check_resource_identity("rust-full", &definition),
            check_resource_identity("rust-full", &definition),
        );
        assert_ne!(
            check_resource_identity("rust-full", &definition),
            check_resource_identity("rust-fmt", &definition),
        );
    }

    #[test]
    fn cargo_control_inputs_select_a_rust_check() {
        // A Rust check that only watched `**/*.rs` would skip a rebuild that a
        // lockfile bump, a feature-flag edit, a linker setting, or a build script
        // makes necessary. The impact set below is the one a Rust check must
        // declare to cover every input cargo's output depends on; this pins that
        // each of those inputs actually selects the check.
        //
        // The globs are the fixture, not a reading of the repository's own
        // `.cairn/config.yaml`: a check set is project configuration a user may
        // edit or clear at will, so asserting against the live file tests the
        // configuration rather than the selection logic and breaks on an
        // unrelated settings edit.
        let impact = [
            "src-tauri/**/*.rs",
            "src-tauri/Cargo.toml",
            "src-tauri/**/Cargo.toml",
            "src-tauri/Cargo.lock",
            "src-tauri/.cargo/config.toml",
            "src-tauri/**/build.rs",
        ];
        let definition = check("bun run check:rust", Some(&impact), CheckWhen::Review);
        let temp = TempDir::new().unwrap();

        for path in [
            "src-tauri/os/cairn-core/src/execution/checks.rs",
            "src-tauri/.cargo/config.toml",
            "src-tauri/Cargo.toml",
            "src-tauri/os/cairn-core/Cargo.toml",
            "src-tauri/Cargo.lock",
            "src-tauri/os/cairn-core/build.rs",
        ] {
            let map = HashMap::from([("rust".to_string(), definition.clone())]);
            let planned = plan_checks(&map, &inputs(&map), &[change(path)], temp.path());
            assert!(
                planned
                    .iter()
                    .any(|plan| plan.name == "rust" && plan.applies),
                "a Rust check must apply when {path} changes; impact={impact:?}"
            );
        }

        // The impact set stays a filter: a change outside it selects nothing.
        let map = HashMap::from([("rust".to_string(), definition)]);
        let planned = plan_checks(
            &map,
            &inputs(&map),
            &[change("web/src/pages/docs/recipes.mdx")],
            temp.path(),
        );
        assert!(
            planned.iter().all(|plan| !plan.applies),
            "a documentation-only change selects no Rust check"
        );
    }

    #[test]
    fn result_key_uses_tree_content_not_commit_history() {
        let temp = TempDir::new().unwrap();
        let repo = temp.path();
        git(repo, &["init", "--quiet"]);
        git(repo, &["config", "user.name", "Cairn Test"]);
        git(repo, &["config", "user.email", "cairn@example.test"]);
        std::fs::create_dir_all(repo.join("src")).unwrap();
        std::fs::write(repo.join("src/lib.rs"), "pub fn value() -> u8 { 1 }\n").unwrap();
        std::fs::write(repo.join("README.md"), "fixture\n").unwrap();
        git(repo, &["add", "."]);
        git(repo, &["commit", "--quiet", "-m", "first message"]);

        let first_commit = git(repo, &["rev-parse", "HEAD"]);
        let tree = git(repo, &["rev-parse", "HEAD^{tree}"]);
        let second_commit = {
            let mut child = Command::new("git")
                .args(["commit-tree", tree.as_str(), "-p", first_commit.as_str()])
                .current_dir(repo)
                .env("GIT_AUTHOR_NAME", "Cairn Test")
                .env("GIT_AUTHOR_EMAIL", "cairn@example.test")
                .env("GIT_COMMITTER_NAME", "Cairn Test")
                .env("GIT_COMMITTER_EMAIL", "cairn@example.test")
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .spawn()
                .unwrap();
            use std::io::Write;
            child
                .stdin
                .as_mut()
                .unwrap()
                .write_all(b"different message and parent\n")
                .unwrap();
            let output = child.wait_with_output().unwrap();
            assert!(output.status.success());
            String::from_utf8_lossy(&output.stdout).trim().to_string()
        };
        assert_ne!(first_commit, second_commit);

        let definition = check("cargo test", Some(&["src/**"]), CheckWhen::Review);
        let first_entries = git_tree_entries(repo, &first_commit);
        let second_entries = git_tree_entries(repo, &second_commit);
        assert_eq!(
            check_result_key(
                &definition,
                &selector(&definition),
                Some(&first_entries),
                &tree,
                "test-platform",
                "test-toolchain",
            ),
            check_result_key(
                &definition,
                &selector(&definition),
                Some(&second_entries),
                &tree,
                "test-platform",
                "test-toolchain",
            ),
            "identical impact-filtered trees must hit despite different commit history"
        );
    }

    fn change(path: &str) -> GraphFileChange {
        GraphFileChange {
            path: path.to_string(),
            previous_path: None,
            status: "modified".to_string(),
            additions: 1,
            deletions: 0,
        }
    }

    fn check(command: &str, impact: Option<&[&str]>, when: CheckWhen) -> CheckCommand {
        CheckCommand {
            command: command.to_string(),
            impact: impact.map(|globs| globs.iter().map(|s| s.to_string()).collect()),
            scope: None,
            policy: CheckPolicy::Advisory,
            when,
            resource_class: CheckResourceClass::Shared,
            timeout: None,
            executor: None,
            verdict_environment: Vec::new(),
            fixes: false,
        }
    }

    fn cache_entry(check_name: &str, tree_hash: &str, passed: bool) -> CheckResultCacheEntry {
        CheckResultCacheEntry {
            project_id: "project-a".to_string(),
            tree_hash: tree_hash.to_string(),
            input_hash: format!("input-{tree_hash}"),
            check_name: check_name.to_string(),
            exit_code: if passed { 0 } else { 1 },
            passed,
            output_tail: String::new(),
            duration_ms: 1,
            ran_at: 1,
            target_results_json: None,
            job_id: None,
            cached: None,
            failure_kind: None,
            infra_failure_streak: 0,
            executor_id: None,
            executor_device_id: None,
            executor_connection_generation: None,
            executor_cell_id: None,
            executor_lease_epoch: None,
            executor_started_at_unix_ms: None,
            executor_finished_at_unix_ms: None,
            toolchain_fingerprint: None,
            defined_by_commit_sha: Some(format!("commit-{tree_hash}")),
        }
    }

    /// The repo's `checks` shape: two `when:write` checks (frontend, typecheck)
    /// scoped to the frontend trees, one `when:review` check (rust) scoped to
    /// src-tauri.
    fn repo_checks() -> HashMap<String, CheckCommand> {
        let mut checks = HashMap::new();
        checks.insert(
            "frontend".to_string(),
            check(
                "bunx vitest related {changedFiles}",
                Some(&["src/**", "packages/ui/**"]),
                CheckWhen::Write,
            ),
        );
        checks.insert(
            "typecheck".to_string(),
            check(
                "bunx tsc --noEmit",
                Some(&["src/**", "packages/ui/**"]),
                CheckWhen::Write,
            ),
        );
        checks.insert(
            "rust".to_string(),
            check(
                "bun run test:rust",
                Some(&["src-tauri/**"]),
                CheckWhen::Review,
            ),
        );
        checks
    }

    // --- the write-cadence gate -------------------------------------------

    #[test]
    fn gate_selects_write_checks_for_a_src_change() {
        let plans = applicable_write_checks(
            &repo_checks(),
            &inputs(&repo_checks()),
            &[change("src/App.tsx")],
            Path::new("/repo"),
        );
        let names: Vec<&str> = plans.iter().map(|p| p.name.as_str()).collect();
        assert_eq!(names, vec!["frontend", "typecheck"]);
    }

    #[test]
    fn gate_is_empty_for_a_doc_only_change() {
        let plans = applicable_write_checks(
            &repo_checks(),
            &inputs(&repo_checks()),
            &[change("docs/x.md")],
            Path::new("/repo"),
        );
        assert!(plans.is_empty(), "a doc-only commit triggers no checks");
    }

    #[test]
    fn gate_excludes_review_checks_for_a_rust_change() {
        let plans = applicable_write_checks(
            &repo_checks(),
            &inputs(&repo_checks()),
            &[change("src-tauri/os/cairn-core/src/lib.rs")],
            Path::new("/repo"),
        );
        // rust matches the change but is when:review; frontend/typecheck do not
        // match the src-tauri impact ⇒ nothing applies at the write cadence.
        assert!(
            !plans.iter().any(|p| p.name == "rust"),
            "a when:review check never runs on write"
        );
        assert!(plans.is_empty());
    }

    // --- contract source: the evaluated commit declares its own checks ------

    /// Write a minimal `.cairn/config.yaml` declaring one `when:write` check
    /// whose command greps a marker file of the same name.
    fn write_checks_config(dir: &Path, check_name: &str) {
        let path = crate::config::project_settings::get_project_config_path(dir);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(
            &path,
            format!(
                "checks:\n  {check_name}:\n    command: grep marker {check_name}.txt\n    impact:\n      - src/**\n    when: write\n"
            ),
        )
        .unwrap();
    }

    /// An empty `checks:` mapping is a config that declares no checks at all.
    fn write_checkless_config(dir: &Path) {
        let path = crate::config::project_settings::get_project_config_path(dir);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, "setupCommands: []\n").unwrap();
    }

    fn commit_at(repo: &Path, message: &str) -> String {
        git(repo, &["add", "-A"]);
        git(
            repo,
            &[
                "-c",
                "user.name=Cairn Test",
                "-c",
                "user.email=test@cairn.local",
                "commit",
                "-q",
                "-m",
                message,
            ],
        );
        git(repo, &["rev-parse", "HEAD"])
    }

    /// A repository whose default branch carries check A and whose sibling
    /// branch carries check B — the exact shape of the leak: two live commits,
    /// each declaring a check the other's tree knows nothing about.
    fn sibling_definitions(temp: &TempDir) -> (std::path::PathBuf, String, String) {
        let repo = temp.path().join("repo");
        std::fs::create_dir_all(repo.join("src")).unwrap();
        git(&repo, &["init", "-q", "-b", "main"]);
        std::fs::write(repo.join("src").join("lib.rs"), "fn base() {}").unwrap();
        let base = commit_at(&repo, "base");

        write_checks_config(&repo, "check-a");
        std::fs::write(repo.join("check-a.txt"), "marker").unwrap();
        let commit_a = commit_at(&repo, "define check-a");

        git(&repo, &["checkout", "-q", "-b", "sibling", &base]);
        write_checks_config(&repo, "check-b");
        std::fs::write(repo.join("check-b.txt"), "marker").unwrap();
        let commit_b = commit_at(&repo, "define check-b");
        // Leave the checkout on the default branch, as a project checkout sits
        // while an agent branch advances elsewhere.
        git(&repo, &["checkout", "-q", "main"]);
        (repo, commit_a, commit_b)
    }

    /// Each sealed commit declares its own checks, and only its own: the
    /// definition and the content it evaluates come from one tree.
    #[test]
    fn a_commit_declares_its_own_checks_and_only_its_own() {
        let temp = tempfile::tempdir().unwrap();
        let (repo, commit_a, commit_b) = sibling_definitions(&temp);

        for (commit, own, foreign) in [
            (&commit_a, "check-a", "check-b"),
            (&commit_b, "check-b", "check-a"),
        ] {
            let loaded = checks_contract_at_commit(&repo, commit)
                .unwrap_or_else(|| panic!("commit {commit} declares {own}"));
            assert_eq!(
                loaded.defined_by_commit, *commit,
                "the contract records the commit that declared it"
            );
            let names: Vec<&str> = loaded.contract.checks.keys().map(String::as_str).collect();
            assert_eq!(names, vec![own], "only {own} is declared at {commit}");
            assert_eq!(
                loaded.contract.checks[own].command,
                format!("grep marker {own}.txt"),
                "the command is the one this commit's own tree declares"
            );

            // Selection agrees: planning this contract can only ever select the
            // commit's own check, so a sibling's definition is never run or
            // recorded against it.
            let changed = vec![change("src/lib.rs")];
            let inputs = inputs(&loaded.contract.checks);
            let planned =
                applicable_write_checks(&loaded.contract.checks, &inputs, &changed, &repo);
            let planned_names: Vec<&str> = planned.iter().map(|plan| plan.name.as_str()).collect();
            assert_eq!(planned_names, vec![own]);
            assert!(
                !planned.iter().any(|plan| plan.command.contains(foreign)),
                "{foreign} must never be planned for {commit}"
            );
        }
    }

    /// The canonical checkout sits on the default branch while a sibling job
    /// seals its own commit. The sibling's cadence sees only its own check; the
    /// project-level projection still reads the canonical configuration.
    #[test]
    fn a_sibling_commit_neither_reads_nor_feeds_the_project_projection() {
        let temp = tempfile::tempdir().unwrap();
        let (repo, _commit_a, commit_b) = sibling_definitions(&temp);

        let sibling = checks_contract_at_commit(&repo, &commit_b).unwrap();
        assert!(
            !sibling.contract.checks.contains_key("check-a"),
            "the default branch's check must not reach a sibling commit"
        );

        let project = crate::config::project_settings::load_checks_contract(&repo)
            .expect("the project checkout declares checks");
        assert!(
            project.checks.contains_key("check-a"),
            "the project-level projection reads the canonical checkout"
        );
        assert!(
            !project.checks.contains_key("check-b"),
            "a branch's definition never becomes the project's"
        );
    }

    /// A check begins at the commit that adds it and stops at the commit that
    /// removes it — never earlier, never later.
    #[test]
    fn adding_and_removing_a_check_takes_effect_only_from_its_own_commit() {
        let temp = tempfile::tempdir().unwrap();
        let repo = temp.path().join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        git(&repo, &["init", "-q", "-b", "main"]);
        write_checkless_config(&repo);
        let before = commit_at(&repo, "no checks yet");
        write_checks_config(&repo, "check-a");
        let added = commit_at(&repo, "add check-a");
        write_checkless_config(&repo);
        let removed = commit_at(&repo, "remove check-a");

        assert!(
            checks_contract_at_commit(&repo, &before).is_none(),
            "a later addition cannot reach back to an earlier commit"
        );
        assert!(checks_contract_at_commit(&repo, &added)
            .unwrap()
            .contract
            .checks
            .contains_key("check-a"));
        assert!(
            checks_contract_at_commit(&repo, &removed).is_none(),
            "the removal takes effect at the commit that carries it"
        );
    }

    /// A commit with no config, an unparseable one, or one that declares no
    /// checks selects nothing — the same as an absent contract always has. It
    /// never falls back to another tree.
    #[test]
    fn an_absent_or_invalid_config_selects_nothing_rather_than_falling_back() {
        let temp = tempfile::tempdir().unwrap();
        let repo = temp.path().join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        git(&repo, &["init", "-q", "-b", "main"]);
        std::fs::write(repo.join("README.md"), "no config here").unwrap();
        let bare = commit_at(&repo, "no config");
        let config_path = crate::config::project_settings::get_project_config_path(&repo);
        std::fs::create_dir_all(config_path.parent().unwrap()).unwrap();
        std::fs::write(&config_path, "checks: [this is not a mapping\n").unwrap();
        let invalid = commit_at(&repo, "invalid config");
        // A later commit DOES declare a check; neither earlier commit may borrow it.
        write_checks_config(&repo, "check-a");
        let valid = commit_at(&repo, "valid config");

        assert!(checks_contract_at_commit(&repo, &bare).is_none());
        assert!(checks_contract_at_commit(&repo, &invalid).is_none());
        assert!(checks_contract_at_commit(&repo, &valid).is_some());
        assert!(
            checks_contract_at_commit(&repo, "0000000000000000000000000000000000000000").is_none(),
            "an unresolvable commit selects nothing"
        );
    }

    // --- passing-baseline delta selection ---------------------------------

    #[test]
    fn tree_entry_delta_reports_added_changed_removed_under_impact_globs() {
        let baseline = vec![
            ("src/a.ts".to_string(), "a1".to_string()),
            ("src/b.ts".to_string(), "b1".to_string()),
            ("src/removed.ts".to_string(), "r1".to_string()),
            ("docs/ignored.md".to_string(), "d1".to_string()),
        ];
        let current = vec![
            ("src/a.ts".to_string(), "a2".to_string()),
            ("src/b.ts".to_string(), "b1".to_string()),
            ("src/added.ts".to_string(), "n1".to_string()),
            ("docs/ignored.md".to_string(), "d2".to_string()),
        ];
        let impact = vec!["src/**".to_string()];

        let delta = diff_tree_entries_for_selector(
            &baseline,
            &current,
            &InputSelector::from_globs(&impact),
        );
        let observed: Vec<(&str, &str)> = delta
            .iter()
            .map(|change| (change.path.as_str(), change.status.as_str()))
            .collect();
        assert_eq!(
            observed,
            vec![
                ("src/a.ts", "modified"),
                ("src/added.ts", "added"),
                ("src/removed.ts", "deleted"),
            ]
        );
    }

    const TEST_PLATFORM: &str = "test-platform";
    const TEST_TOOLCHAIN: &str = "test-toolchain";

    /// A cache row whose `input_hash` is the REAL key for `check` over
    /// `baseline_entries` — i.e. a green row genuinely written under that contract.
    /// Narrowing now requires this, so a row with a synthetic hash reads as
    /// contract-mismatched and correctly declines to narrow.
    fn cache_entry_under(
        check_name: &str,
        tree_hash: &str,
        passed: bool,
        check: &CheckCommand,
        baseline_entries: &[(String, String)],
    ) -> CheckResultCacheEntry {
        let mut entry = cache_entry(check_name, tree_hash, passed);
        entry.input_hash = check_result_key(
            check,
            &selector(check),
            Some(baseline_entries),
            tree_hash,
            TEST_PLATFORM,
            TEST_TOOLCHAIN,
        );
        entry
    }

    fn delta_for(
        row: Option<&CheckResultCacheEntry>,
        baseline: Option<&[(String, String)]>,
        current: Option<&[(String, String)]>,
        check: &CheckCommand,
        cumulative: &[GraphFileChange],
    ) -> Vec<GraphFileChange> {
        baseline_delta_changed_files(
            row,
            baseline,
            current,
            check,
            &selector(check),
            &selector(check),
            TEST_PLATFORM,
            TEST_TOOLCHAIN,
            cumulative,
        )
    }

    #[test]
    fn baseline_decision_uses_delta_only_from_passing_baseline() {
        let baseline = vec![("src/a.ts".to_string(), "a1".to_string())];
        let current = vec![
            ("src/a.ts".to_string(), "a1".to_string()),
            ("src/b.ts".to_string(), "b1".to_string()),
        ];
        let frontend = check(
            "bunx vitest related {changedFiles}",
            Some(&["src/**"]),
            CheckWhen::Write,
        );
        let cumulative = vec![change("src/a.ts"), change("src/b.ts")];
        let passing = cache_entry_under("frontend", "tree-a", true, &frontend, &baseline);
        let failing = cache_entry_under("frontend", "tree-a", false, &frontend, &baseline);

        let narrowed = delta_for(
            Some(&passing),
            Some(&baseline),
            Some(&current),
            &frontend,
            &cumulative,
        );
        assert_eq!(
            narrowed.iter().map(|c| c.path.as_str()).collect::<Vec<_>>(),
            vec!["src/b.ts"]
        );

        let from_fail = delta_for(
            Some(&failing),
            Some(&baseline),
            Some(&current),
            &frontend,
            &cumulative,
        );
        assert_eq!(from_fail, cumulative);

        let from_missing = delta_for(
            None,
            Some(&baseline),
            Some(&current),
            &frontend,
            &cumulative,
        );
        assert_eq!(from_missing, cumulative);
    }

    #[test]
    fn baseline_decision_falls_back_to_cumulative_on_empty_or_uncertain_delta() {
        let entries = vec![("src/a.ts".to_string(), "a1".to_string())];
        let frontend = check(
            "bunx vitest related {changedFiles}",
            Some(&["src/**"]),
            CheckWhen::Write,
        );
        let cumulative = vec![change("src/a.ts")];
        let passing = cache_entry_under("frontend", "tree-a", true, &frontend, &entries);

        let empty_delta = delta_for(
            Some(&passing),
            Some(&entries),
            Some(&entries),
            &frontend,
            &cumulative,
        );
        assert_eq!(empty_delta, cumulative);

        let unreadable_current =
            delta_for(Some(&passing), Some(&entries), None, &frontend, &cumulative);
        assert_eq!(unreadable_current, cumulative);
    }

    /// The checks contract is re-read from the LIVE project config on every commit,
    /// so a green verdict can outlive the definition that produced it. Widening
    /// `impact` is the dangerous direction: the old run never examined the newly
    /// covered tree, yet diffing that same tree under the new globs reports the file
    /// as unchanged and drops it from the selector. Nothing else catches this — the
    /// input-hash cache correctly misses, and the row-ranking query cannot know which
    /// contract a row was written under.
    #[test]
    fn widened_impact_discards_a_baseline_that_never_covered_the_new_glob() {
        // The green run examined only `src/**`, so the UI file in its tree went
        // unchecked even though it sat right there.
        let baseline = vec![
            ("packages/ui/x.ts".to_string(), "x1".to_string()),
            ("src/a.ts".to_string(), "a1".to_string()),
        ];
        let narrow = check(
            "bunx vitest related {changedFiles}",
            Some(&["src/**"]),
            CheckWhen::Write,
        );
        let green = cache_entry_under("frontend", "tree-a", true, &narrow, &baseline);

        // The user then widens the live contract to cover packages/ui too.
        let widened = check(
            "bunx vitest related {changedFiles}",
            Some(&["src/**", "packages/ui/**"]),
            CheckWhen::Write,
        );
        let current = vec![
            ("packages/ui/x.ts".to_string(), "x1".to_string()),
            ("src/a.ts".to_string(), "a1".to_string()),
            ("src/b.ts".to_string(), "b1".to_string()),
        ];
        let cumulative = vec![
            change("packages/ui/x.ts"),
            change("src/a.ts"),
            change("src/b.ts"),
        ];

        let selected = delta_for(
            Some(&green),
            Some(&baseline),
            Some(&current),
            &widened,
            &cumulative,
        );

        assert_eq!(
            selected, cumulative,
            "a baseline written under narrower impact must not anchor narrowing"
        );
        assert!(
            selected.iter().any(|c| c.path == "packages/ui/x.ts"),
            "the newly covered file is unchanged since the baseline tree, so only \
             discarding that baseline keeps it in the selector"
        );
    }

    /// The same-contract case still narrows: the gate must not be so blunt that it
    /// disables the optimization it guards.
    #[test]
    fn unchanged_contract_still_narrows_to_the_tree_delta() {
        let baseline = vec![("src/a.ts".to_string(), "a1".to_string())];
        let current = vec![
            ("src/a.ts".to_string(), "a1".to_string()),
            ("src/b.ts".to_string(), "b1".to_string()),
        ];
        let frontend = check(
            "bunx vitest related {changedFiles}",
            Some(&["src/**"]),
            CheckWhen::Write,
        );
        let green = cache_entry_under("frontend", "tree-a", true, &frontend, &baseline);
        let cumulative = vec![change("src/a.ts"), change("src/b.ts")];

        let selected = delta_for(
            Some(&green),
            Some(&baseline),
            Some(&current),
            &frontend,
            &cumulative,
        );
        assert_eq!(
            selected.iter().map(|c| c.path.as_str()).collect::<Vec<_>>(),
            vec!["src/b.ts"]
        );
    }

    /// Editing the command is the other contract change that invalidates a baseline:
    /// the green verdict was produced by a different runner invocation.
    #[test]
    fn changed_command_discards_the_baseline() {
        let baseline = vec![("src/a.ts".to_string(), "a1".to_string())];
        let current = vec![
            ("src/a.ts".to_string(), "a1".to_string()),
            ("src/b.ts".to_string(), "b1".to_string()),
        ];
        let before = check(
            "bunx vitest related {changedFiles}",
            Some(&["src/**"]),
            CheckWhen::Write,
        );
        let after = check(
            "bunx vitest related --reporter=json {changedFiles}",
            Some(&["src/**"]),
            CheckWhen::Write,
        );
        let green = cache_entry_under("frontend", "tree-a", true, &before, &baseline);
        let cumulative = vec![change("src/a.ts"), change("src/b.ts")];

        let selected = delta_for(
            Some(&green),
            Some(&baseline),
            Some(&current),
            &after,
            &cumulative,
        );
        assert_eq!(selected, cumulative);
    }

    #[test]
    fn passing_baseline_delta_replans_changed_files_selector_to_new_file_only() {
        // Commit A touched f1 and passed, so the cached baseline tree contains f1.
        let baseline = vec![("src/f1.ts".to_string(), "f1-a".to_string())];
        // Commit B touches f2. The cumulative branch diff still contains f1 and f2,
        // but a passing baseline makes the safe selector just the tree delta: f2.
        let current = vec![
            ("src/f1.ts".to_string(), "f1-a".to_string()),
            ("src/f2.ts".to_string(), "f2-b".to_string()),
        ];
        let check = check(
            "bunx vitest related --reporter=json {changedFiles}",
            Some(&["src/**"]),
            CheckWhen::Write,
        );
        let cumulative = vec![change("src/f1.ts"), change("src/f2.ts")];
        let passing = cache_entry_under("frontend", "tree-a", true, &check, &baseline);
        let selected = delta_for(
            Some(&passing),
            Some(&baseline),
            Some(&current),
            &check,
            &cumulative,
        );

        let plan = replan_one_check(
            "frontend",
            &check,
            &inputs_for("frontend", &check),
            &selected,
            Path::new("/repo"),
        )
        .unwrap();
        assert_eq!(
            plan.command,
            "bunx vitest related --reporter=json src/f2.ts"
        );
        assert_eq!(plan.scope, CheckScope::Partial);
    }

    // --- the turn-end-cadence gate ----------------------------------------

    /// A checks map with one check per cadence, all scoped to `src/**`.
    fn cadence_checks() -> HashMap<String, CheckCommand> {
        let mut checks = HashMap::new();
        checks.insert(
            "w".to_string(),
            check("run-w", Some(&["src/**"]), CheckWhen::Write),
        );
        checks.insert(
            "r".to_string(),
            check("run-r", Some(&["src/**"]), CheckWhen::Review),
        );
        checks
    }

    #[test]
    fn turn_end_gate_runs_review_not_write() {
        let plans = applicable_turn_end_checks(
            &cadence_checks(),
            &inputs(&cadence_checks()),
            &[change("src/App.tsx")],
            Path::new("/repo"),
        );
        let names: Vec<&str> = plans.iter().map(|p| p.name.as_str()).collect();
        assert_eq!(
            names,
            vec!["r"],
            "review runs at every turn-end; write never runs here"
        );
    }

    #[test]
    fn turn_end_selection_keeps_advisory_and_gate_review_checks() {
        let mut checks = cadence_checks();
        let mut gate = check("run-gate", Some(&["src/**"]), CheckWhen::Review);
        gate.policy = CheckPolicy::Gate;
        checks.insert("g".to_string(), gate);

        let plans = applicable_turn_end_checks(
            &checks,
            &inputs(&checks),
            &[change("src/App.tsx")],
            Path::new("/repo"),
        );
        let names: Vec<&str> = plans.iter().map(|plan| plan.name.as_str()).collect();
        assert_eq!(names, vec!["g", "r"]);
    }

    #[test]
    fn combined_tree_selection_excludes_default_advisory_review_checks() {
        let default_advisory: CheckCommand =
            serde_yaml::from_str("command: run-advisory\nimpact:\n  - src/**\nwhen: review\n")
                .unwrap();
        assert_eq!(default_advisory.policy, CheckPolicy::Advisory);
        let checks = HashMap::from([("advisory".to_string(), default_advisory)]);

        let plans = applicable_combined_tree_gate_checks(
            &checks,
            &inputs(&checks),
            &[change("src/App.tsx")],
            Path::new("/repo"),
        );
        assert!(plans.is_empty());
    }

    #[test]
    fn combined_tree_selection_keeps_only_applicable_review_gates() {
        let mut applicable = check("run-gate", Some(&["src/**"]), CheckWhen::Review);
        applicable.policy = CheckPolicy::Gate;
        let mut impact_mismatch = check("run-docs", Some(&["docs/**"]), CheckWhen::Review);
        impact_mismatch.policy = CheckPolicy::Gate;
        let mut write = check("run-write", Some(&["src/**"]), CheckWhen::Write);
        write.policy = CheckPolicy::Gate;
        let checks = HashMap::from([
            ("applicable".to_string(), applicable),
            ("impact-mismatch".to_string(), impact_mismatch),
            ("write".to_string(), write),
        ]);

        let plans = applicable_combined_tree_gate_checks(
            &checks,
            &inputs(&checks),
            &[change("src/App.tsx")],
            Path::new("/repo"),
        );
        let names: Vec<&str> = plans.iter().map(|plan| plan.name.as_str()).collect();
        assert_eq!(names, vec!["applicable"]);
    }

    #[test]
    fn turn_end_gate_runs_an_idle_aliased_check() {
        // `when: idle` in a project config deserializes to CheckWhen::Review, so
        // an un-migrated check still runs at turn-end (the alias path).
        let aliased: CheckWhen = serde_yaml::from_str("idle").unwrap();
        assert_eq!(aliased, CheckWhen::Review);
        let mut checks = HashMap::new();
        checks.insert(
            "legacy".to_string(),
            check("run", Some(&["src/**"]), aliased),
        );
        let plans = applicable_turn_end_checks(
            &checks,
            &inputs(&checks),
            &[change("src/App.tsx")],
            Path::new("/repo"),
        );
        let names: Vec<&str> = plans.iter().map(|p| p.name.as_str()).collect();
        assert_eq!(names, vec!["legacy"]);
    }

    #[test]
    fn turn_end_gate_excludes_a_non_matching_impact() {
        // A doc-only change matches no impact glob, so nothing applies.
        let plans = applicable_turn_end_checks(
            &cadence_checks(),
            &inputs(&cadence_checks()),
            &[change("docs/x.md")],
            Path::new("/repo"),
        );
        assert!(plans.is_empty());
    }

    // --- summary formatting -----------------------------------------------

    /// A bare outcome with no structured detail and no output tail, so the
    /// summary renders only the header status line.
    fn outcome(name: &str, passed: bool, exit_code: Option<i32>) -> CheckOutcome {
        CheckOutcome {
            name: name.to_string(),
            passed,
            exit_code,
            failure_kind: None,
            parsed: None,
            output_tail: String::new(),
            cached: false,
            recorded: None,
            duration_ms: 0,
            suppressed_after: None,
        }
    }

    /// A test-runner parse with explicit pass/fail counts, so the summary's
    /// count-bearing annotations are exercised without a real runner.
    fn runner_parse(parser: &str, passed: usize, failed: usize) -> ParsedCheckResult {
        ParsedCheckResult {
            schema_version: 1,
            complete: false,
            selection: "unknown".to_string(),
            tests: vec![],
            undeclared_skips: 0,
            parser: parser.to_string(),
            passed,
            failed,
            skipped: 0,
            suite_failures: 0,
            failures: (0..failed)
                .map(|i| crate::execution::check_parsers::CheckFailure {
                    name: format!("t{i}"),
                    message: None,
                })
                .collect(),
        }
    }

    #[test]
    fn summary_renders_pass_and_fail() {
        // No structured detail / output ⇒ header line only.
        let s = format_check_summary(&[
            outcome("frontend", true, Some(0)),
            outcome("typecheck", false, Some(1)),
        ]);
        assert_eq!(s, "\u{2713} frontend \u{b7} \u{2717} typecheck (exit 1)");
    }

    #[test]
    fn summary_renders_spawn_failure_without_exit_code() {
        let s = format_check_summary(&[outcome("frontend", false, None)]);
        assert_eq!(s, "\u{2717} frontend (failed to run)");
    }

    #[test]
    fn summary_appends_failing_test_names_and_excerpt() {
        let parsed = crate::execution::check_parsers::parse_check_output(
            "bunx tsc --noEmit",
            "a.ts(1,7): error TS2322: Type 'string' is not assignable to type 'number'.",
        );
        let results = vec![CheckOutcome {
            name: "typecheck".to_string(),
            passed: false,
            exit_code: Some(1),
            failure_kind: None,
            parsed,
            output_tail: "raw output tail".to_string(),
            cached: false,
            recorded: None,
            duration_ms: 0,
            suppressed_after: None,
        }];
        let s = format_check_summary(&results);
        // Header status line first.
        assert!(s.starts_with("\u{2717} typecheck (exit 1)"));
        // Then a detail block naming the failing test and quoting the error.
        assert!(s.contains("\u{2717} typecheck \u{2014} 1 failed: a.ts(1,7)"));
        assert!(s.contains("TS2322: Type 'string' is not assignable"));
    }

    /// Build a passing/failing outcome carrying a runner parse, for the
    /// count-bearing annotation tests.
    fn parsed_outcome(
        name: &str,
        passed: bool,
        exit_code: Option<i32>,
        parsed: ParsedCheckResult,
        cached: bool,
    ) -> CheckOutcome {
        CheckOutcome {
            name: name.to_string(),
            passed,
            exit_code,
            failure_kind: None,
            parsed: Some(parsed),
            output_tail: String::new(),
            cached,
            recorded: None,
            duration_ms: 0,
            suppressed_after: None,
        }
    }

    #[test]
    fn summary_shows_test_count_on_a_passing_runner_check() {
        let o = parsed_outcome(
            "frontend",
            true,
            Some(0),
            runner_parse("vitest", 12, 0),
            false,
        );
        assert_eq!(format_check_summary(&[o]), "\u{2713} frontend (12 tests)");
    }

    #[test]
    fn summary_flags_a_zero_selection_pass_honestly() {
        // A `related` selector that matched nothing exits 0 but validated nothing:
        // the annotation must say so rather than render a bare green.
        let o = parsed_outcome(
            "frontend",
            true,
            Some(0),
            runner_parse("vitest", 0, 0),
            false,
        );
        assert_eq!(
            format_check_summary(&[o]),
            "\u{2713} frontend (no tests matched the change)"
        );
    }

    #[test]
    fn summary_names_skipped_tests_inside_a_passing_suite() {
        // A skip is not a pass. A green whose suite skipped part of itself has to
        // read differently from one that ran everything (CAIRN-3164).
        let mut parse = runner_parse("nextest", 5009, 0);
        parse.skipped = 44;
        let o = parsed_outcome("rust-full", true, Some(0), parse, false);
        assert_eq!(
            format_check_summary(&[o]),
            "\u{2713} rust-full (5009 tests, 44 skipped)"
        );
    }

    #[test]
    fn summary_distinguishes_a_wholly_self_skipped_suite_from_a_zero_selection() {
        // The CAIRN-3112 shape: every test in the suite skipped, so the runner
        // reported zero executed. "no tests matched the change" would be a lie
        // about WHY nothing ran.
        let mut parse = runner_parse("nextest", 0, 0);
        parse.skipped = 12;
        let o = parsed_outcome("rust-full", true, Some(0), parse, true);
        assert_eq!(
            format_check_summary(&[o]),
            "\u{2713} rust-full (no tests ran, 12 skipped, cached)"
        );
    }

    #[test]
    fn summary_names_a_suite_collection_failure_instead_of_a_zero_tally() {
        // 881 tests passed and none failed, yet the check is red because one file
        // never got as far as running a test. Folding that into the assertion
        // tally renders "0 of 881 failed": a red check pointing at nothing.
        let mut parse = runner_parse("vitest", 881, 0);
        parse.suite_failures = 1;
        parse.failures = vec![crate::execution::check_parsers::CheckFailure {
            name: "src/components/FileTabView.test.tsx".to_string(),
            message: Some(
                "Cannot find module '../../packages/ui/src/lib/readableMarkdown'".to_string(),
            ),
        }];
        let o = parsed_outcome("frontend-partial", false, Some(1), parse, false);
        let s = format_check_summary(&[o]);
        assert!(
            s.starts_with("\u{2717} frontend-partial (1 suite failed to load, exit 1)"),
            "got: {s}"
        );
        assert!(
            s.contains("src/components/FileTabView.test.tsx"),
            "got: {s}"
        );
        assert!(s.contains("Cannot find module"), "got: {s}");
    }

    #[test]
    fn summary_counts_failing_tests_and_uncollected_suites_separately() {
        let mut parse = runner_parse("vitest", 38, 2);
        parse.suite_failures = 3;
        let o = parsed_outcome("frontend-partial", false, Some(1), parse, false);
        let s = format_check_summary(&[o]);
        assert!(
            s.starts_with(
                "\u{2717} frontend-partial (2 of 40 failed, 3 suites failed to load, exit 1)"
            ),
            "got: {s}"
        );
    }

    #[test]
    fn summary_shows_pass_of_total_on_a_failing_runner_check() {
        let o = parsed_outcome(
            "rust",
            false,
            Some(101),
            runner_parse("nextest", 38, 2),
            false,
        );
        let s = format_check_summary(&[o]);
        assert!(
            s.starts_with("\u{2717} rust (2 of 40 failed, exit 101)"),
            "got: {s}"
        );
    }

    #[test]
    fn summary_shows_duration_on_a_passing_unparsed_check() {
        // typecheck / api have no test-runner parse; a fresh pass shows duration.
        let mut o = outcome("typecheck", true, Some(0));
        o.duration_ms = 4100;
        assert_eq!(format_check_summary(&[o]), "\u{2713} typecheck (4.1s)");
    }

    #[test]
    fn summary_annotates_a_cache_hit() {
        // A reused verdict is distinguishable from a fresh run. Duration is
        // suppressed for a cache hit (it belonged to the original run).
        let mut o = outcome("typecheck", true, Some(0));
        o.cached = true;
        o.duration_ms = 4100;
        assert_eq!(format_check_summary(&[o]), "\u{2713} typecheck (cached)");

        // A cached test-runner pass keeps its count AND flags the reuse.
        let cached_runner = parsed_outcome(
            "frontend",
            true,
            Some(0),
            runner_parse("vitest", 7, 0),
            true,
        );
        assert_eq!(
            format_check_summary(&[cached_runner]),
            "\u{2713} frontend (7 tests, cached)"
        );
    }

    #[test]
    fn fixed_summary_attributes_a_combined_delta_once_at_batch_level() {
        let summary = "Checks: ✓ rust-fmt (2.1s) · ✓ migrations (cached)";
        let rendered = format_fixed_batch_summary(
            summary,
            "1234567890abcdef",
            &["src/lib.rs".into(), "src/main.rs".into()],
        );
        assert_eq!(
            rendered,
            "Checks: ✓ write-check fixes (fixed, 1234567890ab; 2 files) · ✓ rust-fmt (2.1s) · ✓ migrations (cached)"
        );
        assert!(!rendered.contains("migrations (fixed"));
    }

    // --- one wave per commit: fixers first, then re-key, never re-run --------

    /// A declared fixer — the formatter shape this contract exists for.
    fn fixing_check(command: &str, impact: Option<&[&str]>) -> CheckCommand {
        CheckCommand {
            fixes: true,
            ..check(command, impact, CheckWhen::Write)
        }
    }

    /// The repro's wave: this repository's write cadence, where the formatter
    /// sorts LAST by name and every other check reads the Rust sources it
    /// rewrites.
    fn write_wave() -> (HashMap<String, CheckCommand>, Vec<(CheckPlan, String)>) {
        let checks = HashMap::from([
            (
                "api".to_string(),
                check("bun run test:api", Some(&["api/**"]), CheckWhen::Write),
            ),
            (
                "lockfile".to_string(),
                check(
                    "bun run check:lockfile",
                    Some(&["src-tauri/**/Cargo.toml"]),
                    CheckWhen::Write,
                ),
            ),
            (
                "migrations".to_string(),
                check(
                    "bun run check:migrations",
                    Some(&["src-tauri/**/*.rs"]),
                    CheckWhen::Write,
                ),
            ),
            (
                "rust-fmt".to_string(),
                fixing_check("bun run fmt", Some(&["src-tauri/**/*.rs"])),
            ),
        ]);
        // Plans are ordered by name, which is exactly where the formatter lands
        // last and the cascade came from.
        let keyed: Vec<(CheckPlan, String)> = ["api", "lockfile", "migrations", "rust-fmt"]
            .into_iter()
            .map(|name| {
                (
                    plan(name, &checks[name].command),
                    format!("key-before-{name}"),
                )
            })
            .collect();
        (checks, keyed)
    }

    #[test]
    fn a_declared_fixer_is_submitted_before_the_checks_that_read_its_output() {
        let (checks, keyed) = write_wave();
        let order = fixer_first_submission_order(&keyed, &checks, &[0, 1, 2, 3]);
        let names: Vec<&str> = order.iter().map(|i| keyed[*i].0.name.as_str()).collect();
        assert_eq!(
            names,
            vec!["rust-fmt", "api", "lockfile", "migrations"],
            "the fixer runs first; every other check keeps plan order"
        );
        // Indices themselves are untouched, so the status board, the per-check
        // output streams, and the rendered summary all stay in plan order.
        let mut covered = order.clone();
        covered.sort_unstable();
        assert_eq!(covered, vec![0, 1, 2, 3]);
    }

    #[test]
    fn submission_order_is_plan_order_when_nothing_declares_a_fix() {
        let (mut checks, keyed) = write_wave();
        checks.get_mut("rust-fmt").unwrap().fixes = false;
        assert_eq!(
            fixer_first_submission_order(&keyed, &checks, &[0, 1, 2, 3]),
            vec![0, 1, 2, 3],
            "a wave with no declared fixer must submit exactly as it did before"
        );
    }

    #[test]
    fn a_declared_fixers_fix_re_verifies_nothing_and_never_re_runs_the_fixer() {
        let (checks, keyed) = write_wave();
        let fixed = vec![
            "src-tauri/os/a.rs".to_string(),
            "src-tauri/os/b.rs".to_string(),
        ];
        let resolved = [selector(&checks["rust-fmt"])];
        let fixers: Vec<&InputSelector> = resolved.iter().collect();
        assert!(fix_is_attributed_to_declared_fixers(&fixed, &fixers));
        // The fix changed every Rust-impacted check's inputs, so every key moves.
        // Each verdict still describes the tree that landed: the fixer ran first
        // inside the shared slot, so the rest of the wave already validated its
        // output — and the fixer's own verdict is a verdict on the tree it just
        // produced. Nothing here executes twice.
        for (plan, key_before) in &keyed {
            let key_after = format!("key-after-{}", plan.name);
            assert!(
                verdict_survives_fix(true, true, false, key_before, &key_after),
                "{} must not run again against a fix it already saw",
                plan.name
            );
        }
    }

    #[test]
    fn a_verdict_the_fix_invalidated_is_re_verified_rather_than_re_keyed() {
        // Answered from the cache BEFORE the fix: it never saw the fixed tree, so
        // its verdict cannot be keyed onto it. Re-verifying is the only honest
        // answer; re-keying it would be exactly the false green this forbids.
        assert!(!verdict_survives_fix(false, true, false, "before", "after"));
        // Unless the fix left its inputs alone — the same argument the result
        // cache itself rests on.
        assert!(verdict_survives_fix(false, true, false, "same", "same"));
        // An unattributed fix invalidates even a check that ran: something else
        // rewrote the tree mid-wave and nothing proves this check saw it.
        assert!(!verdict_survives_fix(true, false, false, "before", "after"));
        // A fixer another fixer ran after cannot carry: it never saw the output
        // that landed, however well the fold is attributed.
        assert!(!verdict_survives_fix(true, true, true, "before", "after"));
        // Unless the later fixer left its inputs alone.
        assert!(verdict_survives_fix(true, true, true, "same", "same"));
    }

    #[test]
    fn a_fix_outside_every_declared_fixers_impact_is_not_attributed() {
        let fmt = fixing_check("bun run fmt", Some(&["src-tauri/**/*.rs"]));
        assert!(fix_is_attributed_to_declared_fixers(
            &["src-tauri/os/a.rs".to_string()],
            &[&selector(&fmt)]
        ));
        // A lockfile the formatter cannot have written: some undeclared check
        // mutated the tree, so the wave falls back to re-verification.
        assert!(!fix_is_attributed_to_declared_fixers(
            &["src-tauri/Cargo.lock".to_string()],
            &[&selector(&fmt)]
        ));
        // A wave with no declared fixer can attribute nothing, so an un-migrated
        // config keeps re-verifying rather than recording verdicts about a tree
        // no check ran against.
        assert!(!fix_is_attributed_to_declared_fixers(
            &["src-tauri/os/a.rs".to_string()],
            &[]
        ));
        // A fixer with no impact globs owns the whole tree.
        let everything = fixing_check("format-everything", None);
        assert!(fix_is_attributed_to_declared_fixers(
            &["anywhere/at/all".to_string()],
            &[&selector(&everything)]
        ));
    }

    #[test]
    fn an_earlier_fixer_never_carries_a_later_fixers_rewrite() {
        // The wave the single-fixer tests cannot reach: Prettier, then
        // `eslint --fix`. Both are declared fixers over the same files, so
        // ESLint can rewrite a file Prettier already passed. Prettier ran first
        // and never saw that rewrite.
        let checks = HashMap::from([
            (
                "prettier".to_string(),
                fixing_check("bunx prettier --write .", Some(&["src/**/*.ts"])),
            ),
            (
                "eslint".to_string(),
                fixing_check("bunx eslint --fix src/", Some(&["src/**/*.ts"])),
            ),
            (
                "typecheck".to_string(),
                check(
                    "bunx tsc --noEmit",
                    Some(&["src/**/*.ts"]),
                    CheckWhen::Write,
                ),
            ),
        ]);
        let keyed = vec![
            (plan("prettier", "bunx prettier --write ."), "p".to_string()),
            (plan("eslint", "bunx eslint --fix src/"), "e".to_string()),
            (plan("typecheck", "bunx tsc --noEmit"), "t".to_string()),
        ];
        let order = fixer_first_submission_order(&keyed, &checks, &[0, 1, 2]);
        assert_eq!(
            order,
            vec![0, 1, 2],
            "both fixers precede the verdict check"
        );

        let superseded = fixers_superseded_by_a_later_fixer(&keyed, &checks, &order);
        assert!(
            superseded.contains(&0),
            "prettier ran before eslint, so eslint's rewrite is behind its back"
        );
        assert!(
            !superseded.contains(&1),
            "eslint is the last fixer: it observed prettier's output and its own"
        );
        assert!(
            !superseded.contains(&2),
            "a non-fixer runs after the whole fixer prefix"
        );

        // The fold is attributed — every path is inside a declared fixer's impact
        // — and prettier did execute. Attribution alone must NOT carry it.
        let fixed = vec!["src/app.ts".to_string()];
        let resolved = [selector(&checks["prettier"]), selector(&checks["eslint"])];
        let fixers: Vec<&InputSelector> = resolved.iter().collect();
        assert!(fix_is_attributed_to_declared_fixers(&fixed, &fixers));
        assert!(
            !verdict_survives_fix(true, true, superseded.contains(&0), "p", "p-after"),
            "a green prettier verdict must not be keyed to a tree eslint rewrote"
        );
        // The other two are proven by ordering and carry as before.
        assert!(verdict_survives_fix(
            true,
            true,
            superseded.contains(&1),
            "e",
            "e-after"
        ));
        assert!(verdict_survives_fix(
            true,
            true,
            superseded.contains(&2),
            "t",
            "t-after"
        ));
    }

    #[test]
    fn a_lone_fixer_is_never_superseded() {
        // The repository's own wave, and the reason supersession is scoped to
        // fixer-after-fixer: with one declared fixer there is nothing behind it,
        // so rust-fmt still carries its verdict and never re-runs.
        let (checks, keyed) = write_wave();
        let order = fixer_first_submission_order(&keyed, &checks, &[0, 1, 2, 3]);
        assert!(
            fixers_superseded_by_a_later_fixer(&keyed, &checks, &order).is_empty(),
            "a single-fixer wave keeps the one-execution-per-commit contract intact"
        );
    }

    #[test]
    fn a_fixer_that_keeps_rewriting_the_tree_terminates() {
        // Termination is structural now that nothing recurses. A wave publishes
        // at most ONE fix commit, and the fixer's verdict survives that fix, so
        // the wave never asks it for a second one. A further mutation can only
        // come from the bounded verification batch, whose delta is reported as
        // non-convergent and never folded (`FixedWave::non_convergent`).
        let checks = HashMap::from([(
            "fmt".to_string(),
            fixing_check("rewrite-forever", Some(&["**/*.rs"])),
        )]);
        let keyed = vec![(plan("fmt", "rewrite-forever"), "key-before".to_string())];
        assert_eq!(fixer_first_submission_order(&keyed, &checks, &[0]), vec![0]);
        assert!(fix_is_attributed_to_declared_fixers(
            &["a.rs".to_string()],
            &[&selector(&checks["fmt"])]
        ));
        assert!(
            verdict_survives_fix(true, true, false, "key-before", "key-after"),
            "re-checking a fixer against its own output is the loop this removes"
        );
    }

    // --- timeout budgets + failure classification -------------------------

    #[test]
    fn sandbox_denial_evidence_is_legible_without_overwriting_command_output() {
        let mut output = "1,412 tests passed".to_string();
        append_sandbox_denial_evidence(
            &mut output,
            &[cairn_common::executor_protocol::SandboxDenialEvidence {
                denial: cairn_common::executor_protocol::SandboxDenial::Path(
                    "/tmp/tool-cache".into(),
                ),
                operation: Some("file-write-create".into()),
                command: "bunx vitest run".into(),
                stream_id: "turn-checks:1".into(),
            }],
        );

        assert!(output.starts_with("1,412 tests passed"));
        assert!(output.contains("operation=file-write-create"));
        assert!(output.contains("path=/tmp/tool-cache"));
        assert!(output.contains("command=bunx vitest run"));
        assert!(output.contains("stream=turn-checks:1"));
    }

    #[test]
    fn tracked_modification_evidence_is_legible_without_overwriting_command_output() {
        let mut output = "cargo check failed with code 101".to_string();
        append_tracked_modification_evidence(
            &mut output,
            Some(
                &cairn_common::executor_protocol::TrackedModificationEvidence {
                    paths: vec!["Cargo.lock".into(), "src/generated.rs".into()],
                    files_changed: 2,
                    lines_added: 4,
                    lines_deleted: 1,
                },
            ),
        );

        assert!(output.starts_with("cargo check failed with code 101"));
        assert!(output.contains("check modified tracked paths: Cargo.lock, src/generated.rs"));
        assert!(output.contains("2 files, +4 -1"));
        assert!(output.ends_with("changes were discarded"));
    }

    #[test]
    fn timeout_budget_formats_minutes_and_seconds() {
        assert_eq!(format_timeout_budget(600_000), "10m");
        assert_eq!(format_timeout_budget(1_800_000), "30m");
        assert_eq!(format_timeout_budget(45_000), "45s");
        assert_eq!(format_timeout_budget(0), "0s");
    }

    #[test]
    fn resolve_timeout_prefers_schema_then_default_then_cap() {
        let default_ms = DEFAULT_REVIEW_CHECK_TIMEOUT_MS;
        // No check / no schema timeout ⇒ the cadence default.
        assert_eq!(resolve_check_timeout_ms(None, default_ms), default_ms);
        let mut c = check("run", None, CheckWhen::Review);
        assert_eq!(resolve_check_timeout_ms(Some(&c), default_ms), default_ms);
        // A schema timeout (SECONDS) wins, converted to ms.
        c.timeout = Some(120);
        assert_eq!(resolve_check_timeout_ms(Some(&c), default_ms), 120_000);
        // An absurd value is clamped to the hard 60-minute ceiling.
        c.timeout = Some(10_000);
        assert_eq!(
            resolve_check_timeout_ms(Some(&c), default_ms),
            MAX_CHECK_TIMEOUT_MS
        );
    }

    #[test]
    fn defaults_give_the_heavy_review_cadence_more_headroom() {
        // The whole point: review's default must sit well above the 10-min wall
        // the write cadence keeps, or a healthy-but-slow suite is guillotined
        // again (dozens of `rust-full` rows were killed at ~600s). Bind to locals
        // so the guards aren't flagged as constant-value assertions.
        let (write, review) = (
            DEFAULT_WRITE_CHECK_TIMEOUT_MS,
            DEFAULT_REVIEW_CHECK_TIMEOUT_MS,
        );
        assert_eq!(write, 600_000);
        assert!(
            review >= 1_800_000,
            "review default must cover a cold, uncached full Rust build"
        );
        assert!(
            review > write,
            "review default must exceed the tighter write default"
        );
    }

    #[tokio::test]
    async fn merge_gate_classifies_all_substrate_failures_as_infrastructure() {
        use cairn_common::executor_protocol::{
            CellUnavailableReason::*, ObjectInfrastructureStage,
        };
        let unavailable = vec![
            Deadline {
                host_pressure: None,
                substrate: None,
            },
            Provisioning,
            Checkout,
            Spawn,
            Preparation,
            ExecutorUnavailable,
            NoMatchingExecutor,
            ObjectInfrastructure(ObjectInfrastructureStage::FetchInterrupted),
            ObjectInfrastructure(ObjectInfrastructureStage::IntegrityFailure),
            ObjectInfrastructure(ObjectInfrastructureStage::IncompleteClosure),
            ObjectInfrastructure(ObjectInfrastructureStage::InstallFailure),
            ObjectInfrastructure(ObjectInfrastructureStage::UploadFailure),
            ObjectInfrastructure(ObjectInfrastructureStage::ExpiredReceipt),
            ObjectInfrastructure(ObjectInfrastructureStage::StaleReceipt),
        ];
        let mut failures = unavailable
            .into_iter()
            .map(|reason| CellOutcome::Unavailable {
                reason,
                diagnostic: "unavailable".to_string(),
            })
            .collect::<Vec<_>>();
        failures.push(CellOutcome::FailedAfterExecution {
            request_id: "request".to_string(),
            attempt_id: "attempt".to_string(),
            diagnostic: "publication failed".to_string(),
        });
        failures.push(CellOutcome::Cancelled {
            request_id: "request".to_string(),
            attempt_id: "attempt".to_string(),
        });
        failures.push(CellOutcome::StorageFailure {
            request_id: "request".to_string(),
            attempt_id: "attempt".to_string(),
            stage: cairn_common::executor_protocol::StorageFailureStage::Recovery,
            kind: cairn_common::executor_protocol::StorageFailureKind::CleanupFailed,
            diagnostic: "cleanup failed".to_string(),
            slot_retired: false,
        });
        failures.push(CellOutcome::Completed {
            request_id: "request".to_string(),
            attempt_id: "attempt".to_string(),
            exit_code: Some(0),
            output: String::new(),
            timed_out: false,
            metadata: cairn_common::executor_protocol::CellExecutionMeta {
                executor_id: "executor".to_string(),
                executor_device_id: "device".to_string(),
                executor_connection_generation: 1,
                cell_id: "slot".to_string(),
                cell_epoch: 1,
                started_at_unix_ms: 1,
                finished_at_unix_ms: 2,
                duration_ms: None,
                peak_rss_bytes: None,
                peak_physical_footprint_bytes: None,
                disk_delta_bytes: None,
                measurement_quality: None,
            },
            mutation_delta: Some(Box::new(cairn_common::executor_protocol::MutationDelta {
                base_commit: "base".to_string(),
                delta_commit: "delta".to_string(),
                upload_receipt: None,
            })),
            sandbox_denials: Vec::new(),
            tracked_modifications: None,
        });

        for (index, outcome) in failures.into_iter().enumerate() {
            let failure = check_result_from_cell_outcome(outcome, None)
                .expect_err("substrate outcome must not become a command verdict");
            let db = cache_db().await;
            let results = run_planned_checks(
                db,
                "project-a",
                &format!("tree-{index}"),
                "job-a",
                &[(plan("rust", "cargo test"), format!("input-{index}"))],
                "tool",
                CheckExecMode::Shared,
                None,
                move |_, _, _| {
                    let failure = failure.clone();
                    async move { Err::<CheckExecResult, _>(failure) }
                },
                |_| {},
            )
            .await;
            assert!(matches!(
                review_tree_gate_result(results),
                ReviewTreeGateResult::InfrastructureFailure(_)
            ));
        }
    }

    /// Substrate vocabulary an agent must never be handed: slot-absolute paths,
    /// cell and scratch nouns, executor outcome variants, queue evidence. Each
    /// token is chosen to be absent from the authored sentences by construction,
    /// so a match means real diagnostic text leaked into the composed half.
    const SUBSTRATE_VOCABULARY: [&str; 12] = [
        "slot",
        "scratch",
        "substrate",
        "queue",
        "delta",
        "publication",
        "/Users/",
        "CleanupFailed",
        "Recovery",
        "Spawn",
        "Deadline",
        "executor",
    ];

    /// Every reason a cell can refuse to start, mapped to the condition class an
    /// agent is told — one at a time, from an exhaustive match, so a new reason
    /// upstream cannot reach an agent as a generic "could not start".
    ///
    /// The classes that matter are the ones that call for different responses:
    /// wait (capacity), look at the fleet (no machine, draining, unreachable),
    /// look at the environment (preparation), and look at Cairn (dispatch,
    /// storage). Collapsing these into one label is what forced every reader to
    /// the operator log (CAIRN-3345).
    #[test]
    fn every_no_start_reason_keeps_its_own_condition_class() {
        use cairn_common::executor_protocol::{
            AdmissionRejectionReason, CellUnavailableReason, ExecutorSubstrateState,
            HostPressureCondition, ObjectInfrastructureStage,
        };
        let sample = CellUnavailableReason::Provisioning;
        match &sample {
            CellUnavailableReason::Deadline { .. }
            | CellUnavailableReason::Provisioning
            | CellUnavailableReason::Checkout
            | CellUnavailableReason::Spawn
            | CellUnavailableReason::Preparation
            | CellUnavailableReason::SlotUnhealthy
            | CellUnavailableReason::ExecutorUnavailable
            | CellUnavailableReason::NoMatchingExecutor
            | CellUnavailableReason::AdmissionRejected { .. }
            | CellUnavailableReason::ObjectInfrastructure(_) => {}
        }
        let cases = [
            (
                deadline(Some(ExecutorSubstrateState::CapacityBusy), None),
                SubstrateFailureShape::Capacity,
            ),
            (
                deadline(Some(ExecutorSubstrateState::SlotAdoption), None),
                SubstrateFailureShape::Capacity,
            ),
            (
                deadline(Some(ExecutorSubstrateState::ConnectedStalled), None),
                SubstrateFailureShape::MachineUnreachable,
            ),
            (
                deadline(Some(ExecutorSubstrateState::Draining), None),
                SubstrateFailureShape::Draining,
            ),
            (
                deadline(None, None),
                SubstrateFailureShape::MachineUnreachable,
            ),
            (
                deadline(
                    None,
                    Some(vec![HostPressureCondition::MemoryAvailable {
                        available_bytes: 1,
                        floor_bytes: 2,
                    }]),
                ),
                SubstrateFailureShape::Capacity,
            ),
            (
                deadline(
                    None,
                    Some(vec![HostPressureCondition::DiskFree {
                        free_bytes: 1,
                        floor_bytes: 2,
                    }]),
                ),
                SubstrateFailureShape::Storage,
            ),
            (
                deadline(None, Some(Vec::new())),
                SubstrateFailureShape::MachineUnreachable,
            ),
            (
                CellUnavailableReason::AdmissionRejected {
                    reason: AdmissionRejectionReason::QueueFull,
                },
                SubstrateFailureShape::Capacity,
            ),
            (
                CellUnavailableReason::AdmissionRejected {
                    reason: AdmissionRejectionReason::Draining,
                },
                SubstrateFailureShape::Draining,
            ),
            (
                CellUnavailableReason::AdmissionRejected {
                    reason: AdmissionRejectionReason::StorageCleanupFailed,
                },
                SubstrateFailureShape::Storage,
            ),
            (
                CellUnavailableReason::AdmissionRejected {
                    reason: AdmissionRejectionReason::RequestTooLarge,
                },
                SubstrateFailureShape::NoMachine,
            ),
            (
                CellUnavailableReason::ExecutorUnavailable,
                SubstrateFailureShape::MachineUnreachable,
            ),
            (
                CellUnavailableReason::NoMatchingExecutor,
                SubstrateFailureShape::NoMachine,
            ),
            (
                CellUnavailableReason::Provisioning,
                SubstrateFailureShape::Preparation,
            ),
            (
                CellUnavailableReason::Checkout,
                SubstrateFailureShape::Preparation,
            ),
            (
                CellUnavailableReason::Preparation,
                SubstrateFailureShape::Preparation,
            ),
            // The pair that must not collapse: an environment that could not be
            // prepared is re-presented unchanged, and one that was retired for
            // being unfit is tried again somewhere else.
            (
                CellUnavailableReason::SlotUnhealthy,
                SubstrateFailureShape::EnvironmentRetired,
            ),
            (
                CellUnavailableReason::Spawn,
                SubstrateFailureShape::Dispatch,
            ),
            (
                CellUnavailableReason::ObjectInfrastructure(
                    ObjectInfrastructureStage::FetchInterrupted,
                ),
                SubstrateFailureShape::Dispatch,
            ),
        ];
        for (reason, expected) in cases {
            assert_eq!(no_start_shape(&reason), expected, "{reason:?}");
        }
    }

    fn deadline(
        substrate: Option<cairn_common::executor_protocol::ExecutorSubstrateState>,
        pressure: Option<Vec<cairn_common::executor_protocol::HostPressureCondition>>,
    ) -> cairn_common::executor_protocol::CellUnavailableReason {
        cairn_common::executor_protocol::CellUnavailableReason::Deadline {
            host_pressure: pressure.map(|conditions| {
                cairn_common::executor_protocol::HostPressureEvidence { conditions }
            }),
            substrate: substrate.map(|state| {
                cairn_common::executor_protocol::ExecutorSubstrateEvidence::without_queue(
                    state, 0, 0,
                )
            }),
        }
    }

    /// An elapsed deadline is a moment, not a condition: what the wait was ON is
    /// in the executor's evidence, and the fleet already decides from that same
    /// evidence whether the wait was worth having. So retry eligibility here must
    /// equal the fleet's own capacity verdict, specimen for specimen.
    ///
    /// Without this, a machine that stopped answering or was draining would be
    /// waited through two more horizons and then reported as "could not obtain
    /// capacity" — the condition-class collapse this mapping exists to remove,
    /// reintroduced at the one reason where evidence decides.
    #[test]
    fn a_deadlines_retry_eligibility_matches_the_fleets_own_capacity_verdict() {
        use crate::fleet::placement::{classify_unavailable, LinkRestoration};
        use cairn_common::executor_protocol::{ExecutorSubstrateState, HostPressureCondition};
        let specimens = [
            deadline(Some(ExecutorSubstrateState::CapacityBusy), None),
            deadline(Some(ExecutorSubstrateState::SupervisorRespawning), None),
            deadline(Some(ExecutorSubstrateState::SlotAdoption), None),
            deadline(Some(ExecutorSubstrateState::ConnectedStalled), None),
            deadline(Some(ExecutorSubstrateState::Draining), None),
            deadline(None, None),
            deadline(None, Some(Vec::new())),
            deadline(
                None,
                Some(vec![HostPressureCondition::MemoryAvailable {
                    available_bytes: 1,
                    floor_bytes: 2,
                }]),
            ),
            deadline(
                None,
                Some(vec![HostPressureCondition::DiskFree {
                    free_bytes: 1,
                    floor_bytes: 2,
                }]),
            ),
            deadline(
                Some(ExecutorSubstrateState::ConnectedStalled),
                Some(vec![HostPressureCondition::DiskFree {
                    free_bytes: 1,
                    floor_bytes: 2,
                }]),
            ),
        ];
        for reason in specimens {
            assert_eq!(
                no_start_shape(&reason).is_transient(),
                classify_unavailable(&reason, LinkRestoration::NotRestoring).is_capacity(),
                "check composition and fleet placement must read {reason:?} the same way"
            );
        }
    }

    /// Capacity is the one condition time relieves, so it is the only one a
    /// retry could change the answer to. Everything else is re-presented
    /// unchanged and would only spend the machine twice.
    #[test]
    fn only_a_capacity_refusal_is_worth_asking_again() {
        assert!(SubstrateFailureShape::Capacity.is_transient());
        for shape in [
            SubstrateFailureShape::Dispatch,
            SubstrateFailureShape::MachineUnreachable,
            SubstrateFailureShape::Draining,
            SubstrateFailureShape::NoMachine,
            SubstrateFailureShape::Preparation,
            SubstrateFailureShape::Result,
            SubstrateFailureShape::Storage,
            SubstrateFailureShape::Cancelled,
        ] {
            assert!(!shape.is_transient(), "{shape:?} is not relieved by time");
        }
    }

    fn capacity_outcome(indices: [usize; 2]) -> PlannedCheckBatchOutcome {
        PlannedCheckBatchOutcome::failed(
            indices.to_vec(),
            SubstrateFailure::new(SubstrateFailureShape::Capacity, "no room"),
        )
    }

    /// The attempt bound, which is what keeps re-presentation from becoming a
    /// loop. The other bound — the declared patience budget — is exercised in
    /// the patience tests below; either one alone ends the policy.
    #[test]
    fn a_capacity_retry_is_bounded_by_its_attempt_count() {
        let refused = capacity_outcome([0, 1]);
        for attempt in 0..CAPACITY_RETRY_ATTEMPTS {
            assert!(
                matches!(
                    capacity_retry_decision(attempt, &refused),
                    CapacityRetry::Again { .. }
                ),
                "attempt {attempt} is within the bound"
            );
        }
        assert_eq!(
            capacity_retry_decision(CAPACITY_RETRY_ATTEMPTS, &refused),
            CapacityRetry::Surface,
            "the bound is what makes this a policy rather than a loop"
        );
    }

    fn predicted(
        relief_ms: u64,
        occupant_count: usize,
    ) -> crate::fleet::occupancy::MachineOccupancy {
        crate::fleet::occupancy::MachineOccupancy::Predicted(
            crate::fleet::occupancy::OccupancyForecast {
                relief_ms,
                blocking: "CAIRN-3414's rust-tests".into(),
                occupant_count,
            },
        )
    }

    /// The ceiling bounds the whole wait on load Cairn cannot account for, not
    /// one presentation of it, so a batch that has spent it has nothing left to
    /// offer a retry however many attempts its count would still allow.
    #[test]
    fn foreign_patience_bounds_the_whole_wait_rather_than_one_presentation() {
        let live = CheckPatience {
            started: std::time::Instant::now(),
            foreign_ceiling: std::time::Duration::from_millis(REVIEW_CADENCE_FOREIGN_CEILING_MS),
            mobility: PlacementMobility::SpillEligible,
        };
        assert!(!live.foreign_patience_spent());
        assert!(live.remaining_foreign_ceiling_ms() > REVIEW_CADENCE_FOREIGN_CEILING_MS - 10_000);

        let exhausted = CheckPatience {
            started: std::time::Instant::now(),
            foreign_ceiling: std::time::Duration::ZERO,
            mobility: PlacementMobility::SpillEligible,
        };
        assert!(
            exhausted.foreign_patience_spent(),
            "a spent ceiling ends the wait on load nothing can account for"
        );
        assert_eq!(
            exhausted.remaining_foreign_ceiling_ms(),
            CAPACITY_RETRY_BACKOFF_MS[0],
            "a presentation is never given a zero horizon, which would be evicted on arrival"
        );
    }

    /// The operator's rule, which is the whole point of the policy: several
    /// agents working at once is REGULAR operation, so the contention that
    /// creates must never be able to produce a red row about someone's change.
    ///
    /// A wait on Cairn's own measured work carries no ceiling at all — not a
    /// generous one. Whatever the cadence would allow, and however long the
    /// occupant is predicted to take, the horizon is sized to outlast it and the
    /// check keeps its place.
    #[test]
    fn a_wait_on_cairns_own_work_is_never_cut_short_by_a_cadence_ceiling() {
        // Longer than either cadence ceiling, and longer than the whole wait a
        // pre-CAIRN-3429 batch could ever have declared.
        let long_suite = GroupWait::from_occupancy(predicted(30 * 60_000, 1), 1_000);
        assert!(long_suite.self_inflicted);
        assert_eq!(
            long_suite.horizon_ms,
            30 * 60_000 + PREDICTION_MARGIN_MS,
            "the horizon outlasts the occupant, whatever the cadence would otherwise allow"
        );
        assert!(
            long_suite.horizon_ms > REVIEW_CADENCE_FOREIGN_CEILING_MS,
            "a ceiling meant for unaccountable load must not clip an accounted-for wait"
        );

        // Even with the foreign ceiling fully spent, self-inflicted contention
        // is queued rather than abandoned.
        let spent = GroupWait::from_occupancy(predicted(240_000, 1), 0);
        assert!(spent.self_inflicted);
        assert_eq!(spent.horizon_ms, 240_000 + PREDICTION_MARGIN_MS);

        // And it says it is holding a place, not counting down to a refusal.
        assert!(
            spent
                .description
                .contains("holding this check's place until it frees"),
            "{}",
            spent.description
        );
    }

    /// What the operator and the agent actually read. A wait Cairn can attribute
    /// names the work it is queued behind and when that work should end; a wait
    /// it cannot attribute says so rather than inventing an occupant, and only
    /// that second kind is on a clock.
    #[test]
    fn a_wait_is_described_by_what_it_is_actually_on() {
        let known =
            GroupWait::from_occupancy(predicted(240_000, 3), REVIEW_CADENCE_FOREIGN_CEILING_MS);
        assert_eq!(known.horizon_ms, 270_000, "relief plus the settling margin");
        assert_eq!(
            known.description,
            "queued behind CAIRN-3414's rust-tests, predicted to finish in 4m, behind 2 other cells; holding this check's place until it frees"
        );

        for blind in [
            crate::fleet::occupancy::MachineOccupancy::Unforecastable,
            crate::fleet::occupancy::MachineOccupancy::Idle,
        ] {
            let wait = GroupWait::from_occupancy(blind, WRITE_CADENCE_FOREIGN_CEILING_MS);
            assert!(!wait.self_inflicted, "nothing accounts for this wait");
            assert_eq!(
                wait.horizon_ms, WRITE_CADENCE_FOREIGN_CEILING_MS,
                "with nothing to predict, the cadence ceiling is the whole answer"
            );
            assert!(
                wait.description.contains("no measured duration")
                    && wait.description.contains("waiting up to"),
                "an unattributable wait states the absence, and that it is on a clock: {}",
                wait.description
            );
        }
    }

    /// The floor still applies to an accounted-for wait: no forecast is precise
    /// enough to justify a horizon so short the queue cannot act on it.
    #[test]
    fn even_a_momentary_occupant_buys_the_floor() {
        let brief =
            GroupWait::from_occupancy(predicted(1_000, 1), REVIEW_CADENCE_FOREIGN_CEILING_MS);
        assert_eq!(brief.horizon_ms, CHECK_PATIENCE_FLOOR_MS);
    }

    /// The named wait reaches the agent. A capacity row that says only "there
    /// was no room" is the row CAIRN-3429 was filed about; every other shape is
    /// left alone because none of them was about waiting.
    #[test]
    fn only_a_capacity_refusal_is_told_what_it_waited_on() {
        let mut outcome = capacity_outcome([0, 1]);
        outcome.results.insert(
            2,
            Err(CheckExecutionFailure::substrate(
                SubstrateFailureShape::NoMachine,
                "no executor advertises matlab",
            )),
        );
        attribute_capacity_wait(
            &mut outcome,
            &GroupWait::from_occupancy(predicted(240_000, 3), REVIEW_CADENCE_FOREIGN_CEILING_MS),
        );

        for index in [0, 1] {
            let Some(Err(CheckExecutionFailure::Substrate(failure))) = outcome.results.get(&index)
            else {
                panic!("index {index} is a substrate failure");
            };
            let message = failure.agent_message();
            assert!(
                message.contains("CAIRN-3414's rust-tests"),
                "a capacity row names what held the machine: {message}"
            );
            assert!(
                message.contains(SUBSTRATE_FAILURE_CONSEQUENCE),
                "the consequence still closes the message: {message}"
            );
        }

        let Some(Err(CheckExecutionFailure::Substrate(structural))) = outcome.results.get(&2)
        else {
            panic!("index 2 is a substrate failure");
        };
        assert_eq!(
            structural.agent_message(),
            format!(
                "{} {SUBSTRATE_FAILURE_CONSEQUENCE}",
                SubstrateFailureShape::NoMachine.lead()
            ),
            "a refusal that was never about waiting gains no wait story"
        );
    }

    /// Two selector groups are two different waits.
    ///
    /// A batch whose items name different executors is presented as separate
    /// requests against separate machines, so each group is blocked by its own
    /// occupant and is attributed before the results merge. One basis stamped
    /// across the whole batch would tell half its rows the name of work they
    /// were never queued behind.
    #[test]
    fn each_selector_group_is_attributed_to_its_own_blocker() {
        let mut linux_group = capacity_outcome([0, 1]);
        attribute_capacity_wait(
            &mut linux_group,
            &GroupWait::from_occupancy(
                crate::fleet::occupancy::MachineOccupancy::Predicted(
                    crate::fleet::occupancy::OccupancyForecast {
                        relief_ms: 300_000,
                        blocking: "CAIRN-3414's rust-tests".into(),
                        occupant_count: 1,
                    },
                ),
                REVIEW_CADENCE_FOREIGN_CEILING_MS,
            ),
        );

        let mut local_group = PlannedCheckBatchOutcome::failed(
            vec![2],
            SubstrateFailure::new(SubstrateFailureShape::Capacity, "no room"),
        );
        attribute_capacity_wait(
            &mut local_group,
            &GroupWait::from_occupancy(
                crate::fleet::occupancy::MachineOccupancy::Predicted(
                    crate::fleet::occupancy::OccupancyForecast {
                        relief_ms: 4_000,
                        blocking: "CAIRN-3421's frontend-tests".into(),
                        occupant_count: 1,
                    },
                ),
                REVIEW_CADENCE_FOREIGN_CEILING_MS,
            ),
        );

        let mut combined = linux_group;
        combined.results.extend(local_group.results);

        let waited_on = |index: usize| match combined.results.get(&index) {
            Some(Err(CheckExecutionFailure::Substrate(failure))) => {
                failure.waited_on().unwrap().to_string()
            }
            _ => panic!("index {index} is a capacity failure"),
        };
        assert!(waited_on(0).contains("CAIRN-3414's rust-tests"));
        assert!(waited_on(1).contains("CAIRN-3414's rust-tests"));
        assert!(
            waited_on(2).contains("CAIRN-3421's frontend-tests"),
            "the second group kept its own blocker: {}",
            waited_on(2)
        );

        // And the operator's one log line reports both, without repeating the
        // group that covered two checks.
        let logged = describe_capacity_waits(&combined);
        assert_eq!(
            logged.matches("queued behind").count(),
            2,
            "two distinct waits, deduplicated within each group: {logged}"
        );
    }

    /// What must NOT retry. A structural refusal has the same answer next time,
    /// a cancellation was deliberate, and a partially-run batch already spent
    /// the machine on the part that ran.
    #[test]
    fn a_refusal_that_time_cannot_relieve_surfaces_at_once() {
        for shape in [
            SubstrateFailureShape::NoMachine,
            SubstrateFailureShape::Draining,
            SubstrateFailureShape::Preparation,
            SubstrateFailureShape::Storage,
            SubstrateFailureShape::Result,
            SubstrateFailureShape::Cancelled,
            SubstrateFailureShape::Dispatch,
        ] {
            let outcome = PlannedCheckBatchOutcome::failed(
                vec![0],
                SubstrateFailure::new(shape, "diagnostic"),
            );
            assert_eq!(
                capacity_retry_decision(0, &outcome),
                CapacityRetry::Surface,
                "{shape:?} must not be asked again"
            );
        }

        let mut partial = capacity_outcome([0, 1]);
        partial.results.insert(
            1,
            Ok(CheckExecResult {
                exit_code: Some(0),
                output: "ran".into(),
                timed_out: false,
                duration_ms: Some(1),
                provenance: None,
                publication: None,
            }),
        );
        assert_eq!(
            capacity_retry_decision(0, &partial),
            CapacityRetry::Surface,
            "a batch that partly ran is not the same ask any more"
        );

        assert_eq!(
            capacity_retry_decision(
                0,
                &PlannedCheckBatchOutcome {
                    results: HashMap::new(),
                    request: None,
                    delta: None,
                    store_dir: None,
                }
            ),
            CapacityRetry::Surface,
            "an empty outcome states no condition to retry"
        );
    }

    /// The composition rule at the seam: every executor outcome an agent can
    /// meet yields an authored message with no substrate detail in it, and a
    /// diagnostic that keeps every coordinate for the operator log.
    #[test]
    fn every_substrate_outcome_composes_an_agent_message_free_of_substrate_detail() {
        use cairn_common::executor_protocol::CellUnavailableReason::*;
        // The specimen from the transcript that opened CAIRN-3219.
        let scratch_path = "remove cell scratch /Users/mitch/.cairn/build-slots/CAIRN/.authority/slot-366/scratch: Directory not empty";
        let outcomes = vec![
            CellOutcome::StorageFailure {
                request_id: "request".to_string(),
                attempt_id: "attempt".to_string(),
                stage: cairn_common::executor_protocol::StorageFailureStage::Recovery,
                kind: cairn_common::executor_protocol::StorageFailureKind::CleanupFailed,
                diagnostic: scratch_path.to_string(),
                slot_retired: true,
            },
            CellOutcome::Unavailable {
                reason: Deadline {
                    host_pressure: None,
                    substrate: Some(cairn_common::executor_protocol::ExecutorSubstrateEvidence {
                        state:
                            cairn_common::executor_protocol::ExecutorSubstrateState::CapacityBusy,
                        since_unix_ms: 0,
                        last_progress_unix_ms: 0,
                        diagnostic: None,
                        queue_depth: Some(4),
                        queue_position: Some(3),
                        active_cell_count: Some(2),
                        oldest_running_started_at_unix_ms: None,
                    }),
                },
                diagnostic: "acquisition deadline elapsed".to_string(),
            },
            CellOutcome::Unavailable {
                reason: Spawn,
                diagnostic: "/Users/mitch/.cairn/build-slots/CAIRN/slot-366 spawn refused"
                    .to_string(),
            },
            CellOutcome::FailedAfterExecution {
                request_id: "request".to_string(),
                attempt_id: "attempt".to_string(),
                diagnostic: "executor lost the slot before publication".to_string(),
            },
            CellOutcome::Cancelled {
                request_id: "request".to_string(),
                attempt_id: "attempt".to_string(),
            },
            CellOutcome::Unavailable {
                reason: NoMatchingExecutor,
                diagnostic: "no executor advertises toolchain matlab".to_string(),
            },
            CellOutcome::Unavailable {
                reason: AdmissionRejected {
                    reason: cairn_common::executor_protocol::AdmissionRejectionReason::Draining,
                },
                diagnostic: "executor slot pool is draining".to_string(),
            },
            CellOutcome::Unavailable {
                reason: Preparation,
                diagnostic: "could not load canonical project execution policy".to_string(),
            },
            // The specimen from the transcript that opened CAIRN-3430, now on
            // the reason that says the environment was given up rather than the
            // one that says preparation failed.
            CellOutcome::Unavailable {
                reason: SlotUnhealthy,
                diagnostic: scratch_path.to_string(),
            },
            CellOutcome::Unavailable {
                reason: ExecutorUnavailable,
                diagnostic: "connection closed while the request was queued".to_string(),
            },
        ];

        let mut leads = BTreeSet::new();
        for outcome in outcomes {
            let failure = check_result_from_cell_outcome(outcome, None)
                .expect_err("substrate outcome must not become a command verdict");
            let CheckExecutionFailure::Substrate(failure) = failure else {
                panic!("a substrate outcome must compose a substrate failure");
            };
            let message = failure.agent_message();
            for token in SUBSTRATE_VOCABULARY {
                assert!(
                    !message.contains(token),
                    "agent message must not carry {token:?}: {message}"
                );
            }
            assert!(
                message.contains("not a result about your change")
                    && message.contains("operator log"),
                "agent message must say whose failure this is and where the rest is: {message}"
            );
            assert!(
                !failure.diagnostic().is_empty(),
                "the operator half must keep the diagnostic"
            );
            leads.insert(
                message
                    .split_once(". ")
                    .expect("lead sentence")
                    .0
                    .to_string(),
            );
        }
        // Ten outcome classes, ten distinct openings: composition informs, it
        // does not flatten every infrastructure failure into one sentence. The
        // count is the assertion — a new class that reuses an existing lead is
        // exactly the collapse this guards against.
        assert_eq!(leads.len(), 10, "each outcome class needs its own lead");
    }

    /// Composition happens at the executor seam, so it must survive the whole
    /// path to the text an agent actually reads: the check outcome appended to
    /// its tool result, and the cache row the `/checks` surface and the wake
    /// detail read back later.
    #[tokio::test]
    async fn the_agent_facing_verdict_carries_the_composed_message_not_the_diagnostic() {
        let scratch_path = "remove cell scratch /Users/mitch/.cairn/build-slots/CAIRN/.authority/slot-366/scratch: Directory not empty";
        let failure = check_result_from_cell_outcome(
            CellOutcome::StorageFailure {
                request_id: "request".to_string(),
                attempt_id: "attempt".to_string(),
                stage: cairn_common::executor_protocol::StorageFailureStage::Recovery,
                kind: cairn_common::executor_protocol::StorageFailureKind::CleanupFailed,
                diagnostic: scratch_path.to_string(),
                slot_retired: true,
            },
            None,
        )
        .expect_err("a storage failure is never a verdict");

        let db = cache_db().await;
        let results = run_planned_checks(
            db.clone(),
            "project-a",
            "tree-specimen",
            "job-a",
            &[(plan("rust", "cargo test"), "input-specimen".to_string())],
            "tool",
            CheckExecMode::Shared,
            None,
            move |_, _, _| {
                let failure = failure.clone();
                async move { Err::<CheckExecResult, _>(failure) }
            },
            |_| {},
        )
        .await;

        assert_eq!(
            results[0].failure_kind,
            Some(CheckFailureKind::Infrastructure),
            "an infrastructure failure keeps its classification"
        );
        let row = crate::execution::cache::list_check_results(db, "project-a", "tree-specimen")
            .unwrap()
            .into_iter()
            .find(|row| row.check_name == "rust")
            .expect("the failure is recorded for the checks surface");

        for text in [results[0].output_tail.as_str(), row.output_tail.as_str()] {
            assert!(
                text.starts_with("Cairn's own storage for this check failed."),
                "the verdict text is the composed message: {text}"
            );
            for token in SUBSTRATE_VOCABULARY {
                assert!(
                    !text.contains(token),
                    "agent-facing verdict must not carry {token:?}: {text}"
                );
            }
            assert!(!text.contains("Directory not empty"));
        }
    }

    #[tokio::test]
    async fn remote_executor_without_environment_identity_is_not_reusable() {
        let db = cache_db().await;
        let provenance = cairn_common::executor_protocol::CellExecutionMeta {
            executor_id: "executor-a".to_string(),
            executor_device_id: "device-a".to_string(),
            executor_connection_generation: 3,
            cell_id: "slot-a".to_string(),
            cell_epoch: 4,
            started_at_unix_ms: 100,
            finished_at_unix_ms: 200,
            duration_ms: None,
            peak_rss_bytes: None,
            peak_physical_footprint_bytes: None,
            disk_delta_bytes: None,
            measurement_quality: None,
        };
        let results = run_planned_checks(
            db.clone(),
            "project-a",
            "tree-provenance",
            "job-a",
            &[(plan("rust", "cargo test"), "input-provenance".to_string())],
            "tool",
            CheckExecMode::Shared,
            None,
            move |_, _, _| {
                let provenance = provenance.clone();
                async move {
                    Ok::<_, String>(CheckExecResult {
                        exit_code: Some(0),
                        output: "ok".to_string(),
                        timed_out: false,
                        duration_ms: Some(12_345),
                        provenance: Some(provenance),
                        publication: None,
                    })
                }
            },
            |_| {},
        )
        .await;
        assert!(results[0].passed);
        assert_eq!(results[0].duration_ms, 12_345);
        assert!(
            get_exact_reusable_check_result(
                db,
                "project-a",
                "rust",
                "input-provenance",
                &current_check_environment_fingerprint(),
                crate::execution::check_identity::CHECK_RESULT_SCHEMA_VERSION as i64,
            )
            .unwrap()
            .is_none(),
            "a remote verdict with no executor-computed environment identity must miss"
        );
    }

    #[test]
    fn merge_gate_treats_command_timeout_as_named_check_failure() {
        let result = review_tree_gate_result(vec![CheckOutcome {
            name: "rust-full".to_string(),
            passed: false,
            exit_code: None,
            failure_kind: Some(CheckFailureKind::TimedOut),
            parsed: None,
            output_tail: "timed out after 30m".to_string(),
            cached: false,
            recorded: None,
            duration_ms: 1_800_000,
            suppressed_after: None,
        }]);
        assert!(matches!(
            result,
            ReviewTreeGateResult::CheckFailed { ref name, ref detail }
                if name == "rust-full" && detail.contains("timed out")
        ));
    }

    #[test]
    fn infrastructure_predicate_covers_operator_owned_failures() {
        assert!(CheckFailureKind::SpawnError.is_infrastructure());
        assert!(CheckFailureKind::Infrastructure.is_infrastructure());
        assert!(CheckFailureKind::RunnerError.is_infrastructure());
        assert!(!CheckFailureKind::TimedOut.is_infrastructure());
        assert!(!CheckFailureKind::Killed.is_infrastructure());
    }

    #[test]
    fn failure_kind_describe_names_each_death() {
        assert_eq!(
            CheckFailureKind::TimedOut.describe(600_000),
            "timed out after 10m"
        );
        assert_eq!(CheckFailureKind::SpawnError.describe(6), "failed to spawn");
        assert_eq!(CheckFailureKind::Killed.describe(67_000), "killed (signal)");
    }

    #[test]
    fn failure_kind_round_trips_through_its_string() {
        for kind in [
            CheckFailureKind::TimedOut,
            CheckFailureKind::SpawnError,
            CheckFailureKind::Killed,
        ] {
            assert_eq!(CheckFailureKind::from_stored(kind.as_str()), Some(kind));
        }
        assert_eq!(CheckFailureKind::from_stored("nonsense"), None);
    }

    /// String-level guard on the acceptance requirement: a timed-out check must
    /// render "timed out after …", never a bare exit code, so the wording cannot
    /// silently regress to a generic failure that sends an agent debugging tests
    /// that never failed.
    #[test]
    fn summary_renders_a_timeout_as_a_timeout_not_an_exit_code() {
        let mut o = outcome("rust-full", false, None);
        o.failure_kind = Some(CheckFailureKind::TimedOut);
        o.duration_ms = 1_800_000;
        let s = format_check_summary(&[o]);
        assert!(s.contains("timed out after 30m"), "got: {s}");
        assert!(!s.contains("failed to run"), "got: {s}");
        assert!(!s.contains("exit"), "got: {s}");
    }

    #[test]
    fn summary_renders_a_spawn_error_legibly() {
        let mut o = outcome("rust-lint", false, None);
        o.failure_kind = Some(CheckFailureKind::SpawnError);
        assert_eq!(
            format_check_summary(&[o]),
            "\u{2717} rust-lint (failed to spawn)"
        );
    }

    // --- cache hit / miss at the runner seam ------------------------------

    async fn cache_db() -> Arc<LocalDb> {
        let db = crate::storage::migrated_test_db("when-write-runner-test.db").await;
        db.execute_script(
            "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('project-a', 'default', 'Project A', 'PA', '/tmp/project-a', 1, 1);",
        )
        .await
        .unwrap();
        Arc::new(db)
    }

    fn plan(name: &str, command: &str) -> CheckPlan {
        let configured: CheckCommand =
            serde_yaml::from_str(&format!("command: {command:?}\n")).unwrap();
        CheckPlan {
            name: name.to_string(),
            applies: true,
            command: command.to_string(),
            scope: CheckScope::Full,
            resource_class: CheckResourceClass::Shared,
            verdict_environment_names: crate::execution::check_identity::verdict_environment_names(
                &configured,
            ),
            config_error: None,
        }
    }

    fn batch_item(command: &str, resource_class: CheckResourceClass) -> PlannedCheckBatchItem {
        PlannedCheckBatchItem {
            index: 0,
            name: "check".into(),
            input_hash: "hash".into(),
            resource_identity_key: "resource".into(),
            command: command.into(),
            stream_id: "stream".into(),
            env: Vec::new(),
            timeout_ms: 1,
            executor: None,
            resource_class,
        }
    }

    /// A `shared` check declares one unit of concurrency, and a tool that
    /// parallelizes internally is still a shared check.
    ///
    /// This is the reversal CAIRN-3345 turns on. Cargo, vitest, and bundlers
    /// used to be read out of the command text as whole-machine work, so two
    /// ordinary review batches reserved 32 units on a 16-unit host, admission
    /// reported `17 of 16` reserved at ~31% CPU, and five-second write-cadence
    /// checks queued until their deadlines and surfaced as verdictless red
    /// infrastructure failures. Opportunistic parallelism is not a requirement.
    #[test]
    fn a_shared_check_declares_one_unit_however_its_tool_parallelizes() {
        assert_eq!(
            declared_check_reservation(CheckResourceClass::Shared).concurrency_units,
            1
        );
        assert_eq!(
            declared_check_reservation(CheckResourceClass::Shared).source,
            ResourceReservationSource::Declared
        );
        for command in [
            "cargo test --workspace",
            "bun run check:rust",
            "bunx vitest run",
            "bun run build",
        ] {
            let items = [batch_item(command, CheckResourceClass::Shared)];
            assert_eq!(
                declared_batch_reservation(&items).concurrency_units,
                1,
                "{command} is co-runnable unless the project says otherwise"
            );
        }
    }

    /// An `exclusive` resource class is the project asserting whole-machine
    /// demand directly, and it remains the ONLY route to that charge.
    #[test]
    fn an_exclusive_check_declares_the_whole_machine() {
        assert_eq!(
            declared_check_reservation(CheckResourceClass::Exclusive).concurrency_units,
            ResourceReservation::WHOLE_MACHINE_CONCURRENCY
        );
    }

    /// Memory and disk stay unstated: they are learned per command identity from
    /// observed runs, and a number written down here would replace a better
    /// estimate with a worse one.
    #[test]
    fn a_declaration_states_concurrency_only() {
        let reservation = declared_check_reservation(CheckResourceClass::Exclusive);
        assert_eq!(reservation.memory_bytes, 0);
        assert_eq!(reservation.disk_growth_bytes, 0);
    }

    /// Items in a batch share one cell, so the batch is as heavy as its heaviest
    /// DECLARED class — one exclusive lane cannot hide behind lighter company,
    /// and a batch of heavyweight-looking shared commands stays co-runnable.
    #[test]
    fn a_batch_declares_its_heaviest_declared_class() {
        let light = batch_item("bun run check:migrations", CheckResourceClass::Shared);
        let parallel = batch_item("cargo clippy --workspace", CheckResourceClass::Shared);
        let exclusive = batch_item("./run-the-suite", CheckResourceClass::Exclusive);
        assert_eq!(
            declared_batch_reservation(std::slice::from_ref(&light)).concurrency_units,
            1
        );
        assert_eq!(
            declared_batch_reservation(&[light.clone(), parallel]).concurrency_units,
            1,
            "a shared batch stays one unit no matter what its tools do internally"
        );
        assert_eq!(
            declared_batch_reservation(&[light, exclusive]).concurrency_units,
            ResourceReservation::WHOLE_MACHINE_CONCURRENCY
        );
    }

    /// The cell's class must come from the commands it will run. Classifying the
    /// batch's display string — a join of check NAMES — matched no command
    /// pattern, so every batch reported `other`.
    #[test]
    fn a_batch_class_reads_its_commands_not_its_display_name() {
        let items = [
            batch_item("bun run check:migrations", CheckResourceClass::Shared),
            batch_item("cargo test --workspace", CheckResourceClass::Shared),
        ];
        assert_eq!(batch_command_class(&items), CellCommandClass::CargoTest);
        assert_eq!(
            CellCommandClass::classify("rust-lint · rust-full"),
            CellCommandClass::Other,
            "the display join this replaced classifies as nothing"
        );
    }

    /// A fake successful (or non-zero-exit) check run for the `run_planned_checks`
    /// harness: a completed process that did not time out. Timeout / spawn / signal
    /// cases build [`CheckExecResult`] / `Err` explicitly.
    fn exec_ok(
        exit_code: Option<i32>,
        output: impl Into<String>,
    ) -> Result<CheckExecResult, String> {
        Ok(CheckExecResult {
            exit_code,
            output: output.into(),
            timed_out: false,
            duration_ms: None,
            provenance: None,
            publication: None,
        })
    }

    fn reusable_observation_for(
        check_name: &str,
        input_hash: &str,
        environment_fingerprint: String,
    ) -> FreshCheckObservationWrite {
        FreshCheckObservationWrite {
            id: format!("obs-{check_name}"),
            project_id: "project-a".to_string(),
            commit_sha: "commit-source".to_string(),
            defined_by_commit_sha: "commit-source".to_string(),
            tree_hash: "tree-source".to_string(),
            check_name: check_name.to_string(),
            input_hash: input_hash.to_string(),
            environment_fingerprint,
            exit_code: 0,
            verdict: "passed".to_string(),
            failure_kind: None,
            complete: true,
            reusable: true,
            non_reusable_reason: None,
            parser_version: crate::execution::check_identity::CHECK_PARSER_VERSION as i64,
            result_schema_version: crate::execution::check_identity::CHECK_RESULT_SCHEMA_VERSION
                as i64,
            ran_at: 100,
            duration_ms: 1,
            job_id: Some("job-a".to_string()),
            run_id: None,
            cadence: "write".to_string(),
            executor_id: None,
            executor_device_id: None,
            executor_connection_generation: None,
            executor_cell_id: None,
            executor_lease_epoch: None,
            executor_started_at_unix_ms: None,
            executor_finished_at_unix_ms: None,
            runner_build_id: None,
            toolchain_fingerprint: None,
            output_tail: "cached".to_string(),
            target_results_json: None,
            tests: Vec::new(),
        }
    }

    fn reusable_observation(environment_fingerprint: String) -> FreshCheckObservationWrite {
        reusable_observation_for("frontend", "ih-frontend", environment_fingerprint)
    }

    /// A check run reported by a REMOTE executor — what an enrolled machine
    /// returns when a spill-eligible suite lands on it.
    fn exec_on_executor(
        exit_code: Option<i32>,
        output: impl Into<String>,
        executor_id: &str,
    ) -> Result<CheckExecResult, String> {
        Ok(CheckExecResult {
            exit_code,
            output: output.into(),
            timed_out: false,
            duration_ms: Some(1),
            provenance: Some(cairn_common::executor_protocol::CellExecutionMeta {
                executor_id: executor_id.to_string(),
                executor_device_id: "device-remote".to_string(),
                executor_connection_generation: 1,
                cell_id: "slot-1".to_string(),
                cell_epoch: 1,
                started_at_unix_ms: 1,
                finished_at_unix_ms: 2,
                duration_ms: Some(1),
                peak_rss_bytes: None,
                peak_physical_footprint_bytes: None,
                disk_delta_bytes: None,
                measurement_quality: None,
            }),
            publication: None,
        })
    }

    /// The specimen behind CAIRN-3413: a manual check that spilled to another
    /// machine.
    ///
    /// Its observation is recorded under an EMPTY environment fingerprint,
    /// because Cairn cannot identify a remote machine's verdict environment and
    /// must not publish its row under a coordinator-derived key. Both halves are
    /// pinned here: the coordinator's key still resolves nothing (reuse stays
    /// exactly as closed as CAIRN-3328 left it), and the caller still gets its
    /// verdict and the id of the row that was written.
    #[tokio::test]
    async fn a_remotely_executed_check_returns_the_observation_it_recorded() {
        let db = cache_db().await;
        let plans = vec![(
            plan("rust-tests", "bunx tsc --noEmit"),
            "ih-remote".to_string(),
        )];
        let results = run_planned_checks_at_commit(
            db.clone(),
            "project-a",
            CheckRunCommit {
                evaluated: "commit-target",
                defined_by: "commit-target",
            },
            "tree-target",
            "job-a",
            &plans,
            "manual-check:run-a:rust-tests",
            CheckExecMode::Shared,
            None,
            |_index, _command, _stream_id| async {
                exec_on_executor(Some(0), "remote green", "bglab-ub")
            },
            |_| {},
        )
        .await;

        let recorded = results[0]
            .recorded
            .clone()
            .expect("a remote run records the observation it wrote");
        assert!(
            recorded.environment_fingerprint.is_empty(),
            "a remote machine's verdict environment is unidentified"
        );
        assert!(
            !recorded.reusable,
            "a remote verdict must never suppress a later execution"
        );

        let schema = crate::execution::check_identity::CHECK_RESULT_SCHEMA_VERSION as i64;
        assert!(
            crate::execution::cache::get_check_result_observation(
                db.clone(),
                "project-a",
                "commit-target",
                "rust-tests",
                &plan_environment_fingerprint(&plans[0].0),
                schema,
                None,
                None,
                0,
                0,
            )
            .unwrap()
            .is_none(),
            "the requesting side's key must not resolve a remote row — which is why \
             the reply cannot be derived from a second lookup"
        );
        assert_eq!(
            crate::execution::cache::get_check_result_observation(
                db,
                "project-a",
                "commit-target",
                "rust-tests",
                "",
                schema,
                None,
                None,
                0,
                0,
            )
            .unwrap()
            .expect("the row exists under the identity it was recorded with")
            .observation_id,
            recorded.id,
            "the run recorded exactly the observation it reported"
        );

        let reply = manual_configured_check_result(
            "rust-tests",
            "commit-target".to_string(),
            "tree-target".to_string(),
            "ih-remote".to_string(),
            results.into_iter().next().unwrap(),
        );
        assert!(reply.passed, "a remote green reaches the caller as a green");
        assert_eq!(reply.observation_id.as_deref(), Some(recorded.id.as_str()));
        assert_eq!(reply.disposition, "fresh");
        assert!(!reply.reusable);
        assert!(reply.environment_fingerprint.is_empty());
        assert!(reply.no_verdict.is_none());
    }

    /// A local run is unchanged: the recorded identity carries this machine's
    /// fingerprint, the coordinator's own key resolves it, and it stays reusable.
    #[tokio::test]
    async fn a_local_verdict_reports_a_reusable_observation_under_this_machine() {
        let db = cache_db().await;
        let plans = vec![(
            plan("frontend", "bunx tsc --noEmit"),
            "ih-local".to_string(),
        )];
        let results = run_planned_checks_at_commit(
            db.clone(),
            "project-a",
            CheckRunCommit {
                evaluated: "commit-target",
                defined_by: "commit-target",
            },
            "tree-target",
            "job-a",
            &plans,
            "manual-check:run-a:frontend",
            CheckExecMode::Shared,
            None,
            |_index, _command, _stream_id| async { exec_ok(Some(0), "ok") },
            |_| {},
        )
        .await;

        let recorded = results[0].recorded.clone().expect("a local run records");
        assert_eq!(
            recorded.environment_fingerprint,
            plan_environment_fingerprint(&plans[0].0)
        );
        assert!(recorded.reusable);
        assert_eq!(
            crate::execution::cache::get_check_result_observation(
                db,
                "project-a",
                "commit-target",
                "frontend",
                &recorded.environment_fingerprint,
                crate::execution::check_identity::CHECK_RESULT_SCHEMA_VERSION as i64,
                None,
                None,
                0,
                0,
            )
            .unwrap()
            .expect("a local row is addressable by this machine's key")
            .observation_id,
            recorded.id
        );

        let reply = manual_configured_check_result(
            "frontend",
            "commit-target".to_string(),
            "tree-target".to_string(),
            "ih-local".to_string(),
            results.into_iter().next().unwrap(),
        );
        assert!(reply.passed && reply.reusable);
        assert_eq!(reply.disposition, "fresh");
        assert!(reply.no_verdict.is_none());
    }

    /// Cairn's own machinery failing is not a result about the tree. It renders
    /// as the named substrate cause, never as a red and never as a complaint
    /// about recording.
    #[tokio::test]
    async fn an_infrastructure_failure_reports_its_substrate_cause() {
        let db = cache_db().await;
        let plans = vec![(
            plan("rust-tests", "bunx tsc --noEmit"),
            "ih-infra".to_string(),
        )];
        let results = run_planned_checks_at_commit(
            db,
            "project-a",
            CheckRunCommit {
                evaluated: "commit-target",
                defined_by: "commit-target",
            },
            "tree-target",
            "job-a",
            &plans,
            "manual-check:run-a:rust-tests",
            CheckExecMode::Shared,
            None,
            |_index, _command, _stream_id| async {
                Err::<CheckExecResult, CheckExecutionFailure>(CheckExecutionFailure::substrate(
                    SubstrateFailureShape::MachineUnreachable,
                    "cell link dropped mid-execution",
                ))
            },
            |_| {},
        )
        .await;

        let outcome = results.into_iter().next().unwrap();
        assert_eq!(outcome.failure_kind, Some(CheckFailureKind::Infrastructure));
        let reply = manual_configured_check_result(
            "rust-tests",
            "commit-target".to_string(),
            "tree-target".to_string(),
            "ih-infra".to_string(),
            outcome,
        );
        let no_verdict = reply
            .no_verdict
            .expect("an infrastructure failure produced no verdict");
        assert_eq!(no_verdict.kind, "infrastructure");
        assert_eq!(no_verdict.after_failures, None);
        assert!(
            no_verdict.cause.contains("lost contact with the machine"),
            "the reader gets the named cause: {}",
            no_verdict.cause
        );
        assert!(
            !no_verdict.cause.contains("cell link dropped"),
            "the substrate diagnostic stays in the operator log: {}",
            no_verdict.cause
        );
    }

    /// A check Cairn declined to run says so, and an ordinary red stays an
    /// ordinary red rather than being laundered into a no-verdict.
    #[test]
    fn a_suppressed_check_reports_that_cairn_declined_to_run_it() {
        let suppressed = CheckOutcome {
            name: "rust-tests".to_string(),
            passed: false,
            exit_code: None,
            failure_kind: Some(CheckFailureKind::Infrastructure),
            parsed: None,
            output_tail: "the last failure was a spawn error".to_string(),
            cached: false,
            duration_ms: 0,
            suppressed_after: Some(3),
            recorded: None,
        };
        let reply = manual_configured_check_result(
            "rust-tests",
            "commit-target".to_string(),
            "tree-target".to_string(),
            "ih-suppressed".to_string(),
            suppressed,
        );
        let no_verdict = reply.no_verdict.expect("a suppressed check has no verdict");
        assert_eq!(no_verdict.kind, "suppressed");
        assert_eq!(no_verdict.after_failures, Some(3));
        assert!(
            reply.observation_id.is_none(),
            "nothing ran, so nothing was observed"
        );

        let red = CheckOutcome {
            name: "rust-tests".to_string(),
            passed: false,
            exit_code: Some(1),
            failure_kind: None,
            parsed: None,
            output_tail: "2 tests failed".to_string(),
            cached: false,
            duration_ms: 10,
            suppressed_after: None,
            recorded: Some(crate::execution::cache::RecordedCheckObservation {
                id: "obs-red".to_string(),
                environment_fingerprint: "env-local".to_string(),
                reusable: false,
            }),
        };
        let reply = manual_configured_check_result(
            "rust-tests",
            "commit-target".to_string(),
            "tree-target".to_string(),
            "ih-red".to_string(),
            red,
        );
        assert!(
            reply.no_verdict.is_none(),
            "a failing check IS a verdict about the tree"
        );
        assert_eq!(reply.observation_id.as_deref(), Some("obs-red"));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn configured_verdict_environment_requires_an_exact_cache_hit() {
        let db = cache_db().await;
        let configured: CheckCommand = serde_yaml::from_str(
            "command: bun run test:rust\nverdictEnvironment:\n  - CAIRN_CUSTOM_VERDICT_MODE\n",
        )
        .unwrap();
        let mut rust = plan("rust", &configured.command);
        rust.verdict_environment_names =
            crate::execution::check_identity::verdict_environment_names(&configured);

        let old_optional = std::env::var_os("CAIRN_SYNC_TESTS_OPTIONAL");
        let old_custom = std::env::var_os("CAIRN_CUSTOM_VERDICT_MODE");
        std::env::set_var("CAIRN_SYNC_TESTS_OPTIONAL", "enabled");
        std::env::set_var("CAIRN_CUSTOM_VERDICT_MODE", "strict");
        let recorded = plan_environment_fingerprint(&rust);
        record_fresh_check_observation(
            db.clone(),
            reusable_observation_for("rust", "input-env", recorded.clone()),
        )
        .unwrap();
        assert!(!needs_execution(
            db.clone(),
            "project-a",
            &rust,
            "input-env"
        ));

        std::env::set_var("CAIRN_SYNC_TESTS_OPTIONAL", "disabled");
        assert!(needs_execution(db.clone(), "project-a", &rust, "input-env"));
        std::env::set_var("CAIRN_SYNC_TESTS_OPTIONAL", "enabled");
        std::env::set_var("CAIRN_CUSTOM_VERDICT_MODE", "relaxed");
        assert!(needs_execution(db.clone(), "project-a", &rust, "input-env"));
        std::env::set_var("CAIRN_CUSTOM_VERDICT_MODE", "strict");
        assert_eq!(plan_environment_fingerprint(&rust), recorded);
        assert!(!needs_execution(db, "project-a", &rust, "input-env"));

        match old_optional {
            Some(value) => std::env::set_var("CAIRN_SYNC_TESTS_OPTIONAL", value),
            None => std::env::remove_var("CAIRN_SYNC_TESTS_OPTIONAL"),
        }
        match old_custom {
            Some(value) => std::env::set_var("CAIRN_CUSTOM_VERDICT_MODE", value),
            None => std::env::remove_var("CAIRN_CUSTOM_VERDICT_MODE"),
        }
    }

    #[tokio::test]
    async fn manual_observation_is_reused_by_review_without_spawning() {
        let db = cache_db().await;
        let environment = current_check_environment_fingerprint();
        record_fresh_check_observation(db.clone(), reusable_observation(environment.clone()))
            .unwrap();

        let plans = vec![(
            plan("frontend", "bunx vitest run"),
            "ih-frontend".to_string(),
        )];
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = calls.clone();
        let results = run_planned_checks_at_commit(
            db.clone(),
            "project-a",
            CheckRunCommit {
                evaluated: "commit-target",
                defined_by: "commit-target",
            },
            "tree-target",
            "job-a",
            &plans,
            "manual-check:run-a:frontend",
            CheckExecMode::Shared,
            None,
            move |_index, _command, _stream_id| {
                let counted = counted.clone();
                async move {
                    counted.fetch_add(1, Ordering::SeqCst);
                    exec_ok(Some(0), "ran")
                }
            },
            |_| {},
        )
        .await;

        assert_eq!(calls.load(Ordering::SeqCst), 0, "a hit must spawn nothing");
        assert!(results[0].cached);
        assert_eq!(results[0].output_tail, "cached");
        let reused = results[0]
            .recorded
            .clone()
            .expect("a hit reports the observation it reused");
        assert_eq!(reused.id, "obs-frontend");
        assert_eq!(reused.environment_fingerprint, environment);
        assert!(reused.reusable);
        let alias = crate::execution::cache::get_check_result_observation(
            db.clone(),
            "project-a",
            "commit-target",
            "frontend",
            &environment,
            crate::execution::check_identity::CHECK_RESULT_SCHEMA_VERSION as i64,
            None,
            None,
            100,
            0,
        )
        .unwrap()
        .expect("cache hit must create a target-commit alias");
        assert_eq!(alias.disposition, "cached");
        assert_eq!(alias.observation_id, "obs-frontend");
        assert_eq!(alias.source_commit_sha, "commit-source");
        assert_eq!(alias.evaluated_tree_hash, "tree-target");

        let review_calls = Arc::new(AtomicUsize::new(0));
        let counted = review_calls.clone();
        let review = run_planned_checks_at_commit(
            db,
            "project-a",
            CheckRunCommit {
                evaluated: "commit-review",
                defined_by: "commit-review",
            },
            "tree-review",
            "job-a",
            &plans,
            "turn-checks:job-a",
            CheckExecMode::Shared,
            None,
            move |_index, _command, _stream_id| {
                let counted = counted.clone();
                async move {
                    counted.fetch_add(1, Ordering::SeqCst);
                    exec_ok(Some(0), "must not run")
                }
            },
            |_| {},
        )
        .await;
        assert_eq!(review_calls.load(Ordering::SeqCst), 0);
        assert!(review[0].cached);
    }

    #[tokio::test]
    async fn fleet_backed_execution_ignores_coordinator_local_hit() {
        let db = cache_db().await;
        let environment = current_check_environment_fingerprint();
        record_fresh_check_observation(db.clone(), reusable_observation(environment)).unwrap();

        let config = TempDir::new().unwrap();
        let orch = test_orchestrator(config.path()).await;
        let plans = vec![(
            plan("frontend", "bunx vitest run"),
            "ih-frontend".to_string(),
        )];
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = calls.clone();
        let results = run_planned_checks_at_commit(
            db,
            "project-a",
            CheckRunCommit {
                evaluated: "commit-target",
                defined_by: "commit-target",
            },
            "tree-target",
            "job-a",
            &plans,
            "tool",
            CheckExecMode::Shared,
            Some(&orch),
            move |_index, _command, _stream_id| {
                let counted = counted.clone();
                async move {
                    counted.fetch_add(1, Ordering::SeqCst);
                    exec_ok(Some(0), "remote-selected execution")
                }
            },
            |_| {},
        )
        .await;

        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "a pre-selection local hit must not suppress fleet execution"
        );
        assert!(!results[0].cached);
        assert_eq!(results[0].output_tail, "remote-selected execution");
    }

    #[tokio::test]
    async fn exact_environment_mismatch_spawns_instead_of_reusing() {
        let db = cache_db().await;
        record_fresh_check_observation(
            db.clone(),
            reusable_observation("definitely-not-the-current-environment".to_string()),
        )
        .unwrap();

        let plans = vec![(
            plan("frontend", "bunx vitest run"),
            "ih-frontend".to_string(),
        )];
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = calls.clone();
        let results = run_planned_checks_at_commit(
            db,
            "project-a",
            CheckRunCommit {
                evaluated: "commit-target",
                defined_by: "commit-target",
            },
            "tree-target",
            "job-a",
            &plans,
            "tool",
            CheckExecMode::Shared,
            None,
            move |_index, _command, _stream_id| {
                let counted = counted.clone();
                async move {
                    counted.fetch_add(1, Ordering::SeqCst);
                    exec_ok(Some(0), "fresh execution")
                }
            },
            |_| {},
        )
        .await;

        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "mismatched evidence must run"
        );
        assert!(!results[0].cached);
        assert_eq!(results[0].output_tail, "fresh execution");
    }

    /// One evaluation performed the way a cadence actually performs it: RESERVE
    /// at submission, launch only what the reservation admits, and only then
    /// settle the outcomes through the engine.
    ///
    /// The ordering is the whole point. Both real cadences build a batch, run it
    /// on a build cell, and hand the finished results to `run_planned_checks`
    /// afterwards — so a test that drives the engine with a closure that
    /// executes inline models an ordering that does not exist, and cannot
    /// observe an over-execution at all. `launches` counts commands actually
    /// started, which is the number the bound is about.
    async fn evaluate_infra_suite(
        db: &Arc<LocalDb>,
        plans: &[(CheckPlan, String)],
        tree: &str,
        launches: &Arc<AtomicUsize>,
    ) -> Vec<CheckOutcome> {
        let items: Vec<PlannedCheckBatchItem> = plans
            .iter()
            .enumerate()
            .map(|(index, (plan, input_hash))| PlannedCheckBatchItem {
                index,
                name: plan.name.clone(),
                input_hash: input_hash.clone(),
                resource_identity_key: String::new(),
                command: plan.command.clone(),
                stream_id: String::new(),
                env: Vec::new(),
                timeout_ms: 1_000,
                executor: None,
                resource_class: CheckResourceClass::Shared,
            })
            .collect();
        let (admitted, mut results) = reserve_batch_items(db.clone(), "project-a", items);
        for item in &admitted {
            launches.fetch_add(1, Ordering::SeqCst);
            results.insert(
                item.index,
                Ok(CheckExecResult {
                    exit_code: Some(254),
                    // The sccache adoption shape: a real exit code with positive
                    // infrastructure evidence in the output.
                    output: "error: could not compile `serde`: process didn't exit successfully: \
                             `rustc` (exit status: 254)"
                        .to_string(),
                    timed_out: false,
                    duration_ms: None,
                    provenance: None,
                    publication: None,
                }),
            );
        }

        let results = Arc::new(std::sync::Mutex::new(results));
        run_planned_checks(
            db.clone(),
            "project-a",
            tree,
            "job-a",
            plans,
            "tool",
            CheckExecMode::Shared,
            None,
            move |index, _command, _stream_id| {
                let results = results.clone();
                async move {
                    results.lock().unwrap().remove(&index).unwrap_or_else(|| {
                        Err(CheckExecutionFailure::substrate(
                            SubstrateFailureShape::Result,
                            format!("missing batch outcome for plan index {index}"),
                        ))
                    })
                }
            },
            |_| {},
        )
        .await
    }

    /// The whole point of the kill switch, driven through the real submission
    /// ordering. Three consecutive infrastructure failures at one input hash, and
    /// the fourth evaluation LAUNCHES NOTHING.
    ///
    /// Note the shape: the first three evaluations must each still launch. A
    /// guard that stopped executing on the first infrastructure failure would
    /// also pass a test that only asserted the fourth, and would strip every
    /// genuinely transient failure of its retries.
    #[tokio::test]
    async fn repeated_infrastructure_failure_stops_being_executed_at_the_bound() {
        let db = cache_db().await;
        let plans = vec![(plan("rust", "cargo test"), "ih-rust".to_string())];
        let launches = Arc::new(AtomicUsize::new(0));

        let mut last = Vec::new();
        for evaluation in 1..=(crate::execution::cache::OBSERVED_INFRA_FAILURE_BOUND + 1) {
            last =
                evaluate_infra_suite(&db, &plans, &format!("tree-{evaluation}"), &launches).await;
            let expected_launches =
                evaluation.min(crate::execution::cache::OBSERVED_INFRA_FAILURE_BOUND) as usize;
            assert_eq!(
                launches.load(Ordering::SeqCst),
                expected_launches,
                "evaluation {evaluation} must {} launch the command",
                if evaluation > crate::execution::cache::OBSERVED_INFRA_FAILURE_BOUND {
                    "NOT"
                } else {
                    "still"
                }
            );
        }

        // The suppressed evaluation reports itself as what it is: not run.
        assert_eq!(last.len(), 1);
        assert!(!last[0].passed, "a suppression is not a pass");
        assert_eq!(
            last[0].suppressed_after,
            Some(crate::execution::cache::OBSERVED_INFRA_FAILURE_BOUND)
        );
        assert!(!last[0].is_genuine_failure(), "and it is not a red either");
        assert!(last[0].output_tail.contains("no longer running this check"));
        assert!(
            last[0]
                .output_tail
                .contains("not a result about your change"),
            "got: {}",
            last[0].output_tail
        );

        // The row follows the current tree, so the checklist still shows the
        // check rather than dropping it silently.
        let listed = crate::execution::cache::list_check_results(
            db.clone(),
            "project-a",
            &format!(
                "tree-{}",
                crate::execution::cache::OBSERVED_INFRA_FAILURE_BOUND + 1
            ),
        )
        .unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(
            listed[0].infra_failure_streak,
            crate::execution::cache::OBSERVED_INFRA_FAILURE_BOUND,
            "the re-stamp must not advance the counter"
        );
    }

    /// Two cadences submitting the same triple at the same moment. The
    /// reservation lives at submission, so only one batch ever carries the item
    /// and only one command is ever launched.
    ///
    /// This is the ordering a post-execution check cannot fix: by the time the
    /// engine sees either result, both cells would already have run.
    #[tokio::test]
    async fn concurrent_submissions_admit_the_item_to_only_one_batch() {
        let db = cache_db().await;
        let plans = vec![(plan("rust", "cargo test"), "ih-rust".to_string())];
        let launches = Arc::new(AtomicUsize::new(0));

        // Spend every retry but the last.
        for evaluation in 1..crate::execution::cache::OBSERVED_INFRA_FAILURE_BOUND {
            evaluate_infra_suite(&db, &plans, &format!("tree-{evaluation}"), &launches).await;
        }
        assert_eq!(
            launches.load(Ordering::SeqCst),
            (crate::execution::cache::OBSERVED_INFRA_FAILURE_BOUND - 1) as usize
        );

        let item = PlannedCheckBatchItem {
            index: 0,
            name: "rust".to_string(),
            input_hash: "ih-rust".to_string(),
            resource_identity_key: String::new(),
            command: "cargo test".to_string(),
            stream_id: String::new(),
            env: Vec::new(),
            timeout_ms: 1_000,
            executor: None,
            resource_class: CheckResourceClass::Shared,
        };
        let racers: Vec<_> = (0..2)
            .map(|_| {
                let db = db.clone();
                let item = item.clone();
                tokio::task::spawn_blocking(move || {
                    reserve_batch_items(db, "project-a", vec![item]).0.len()
                })
            })
            .collect();
        let mut carried = 0;
        for racer in racers {
            carried += racer.await.unwrap();
        }

        assert_eq!(
            carried, 1,
            "exactly one of two simultaneous submissions may carry the item to a cell"
        );
    }

    /// A genuine verdict at the same input hash restores execution immediately —
    /// suppression can never be the reason a real result goes unmeasured.
    #[tokio::test]
    async fn a_genuine_verdict_restores_execution() {
        let db = cache_db().await;
        // Drive the triple to the bound the way execution does — reserve, then
        // record. Storing alone would only ever OPEN the streak at 1, because a
        // retry is counted when it is admitted, not when it completes.
        for _ in 0..crate::execution::cache::OBSERVED_INFRA_FAILURE_BOUND {
            let _ = crate::execution::cache::claim_check_execution(
                db.clone(),
                "project-a",
                "rust",
                "ih-rust",
            );
            store_check_result(
                db.clone(),
                CheckResultCacheWrite {
                    project_id: "project-a".to_string(),
                    tree_hash: "tree-a".to_string(),
                    input_hash: "ih-rust".to_string(),
                    check_name: "rust".to_string(),
                    exit_code: 1,
                    passed: false,
                    output_tail: "sccache: server startup failed".to_string(),
                    duration_ms: 1,
                    target_results_json: None,
                    job_id: Some("job-a".to_string()),
                    cached: Some(false),
                    failure_kind: Some("infrastructure".to_string()),
                    executor_id: None,
                    executor_device_id: None,
                    executor_connection_generation: None,
                    executor_cell_id: None,
                    executor_lease_epoch: None,
                    executor_started_at_unix_ms: None,
                    executor_finished_at_unix_ms: None,
                    toolchain_fingerprint: None,
                    defined_by_commit_sha: Some("commit-a".to_string()),
                },
            )
            .unwrap();
        }

        // An ordinary red lands at the same hash (e.g. a sibling cadence got a
        // real answer out of the same command).
        store_check_result(
            db.clone(),
            CheckResultCacheWrite {
                project_id: "project-a".to_string(),
                tree_hash: "tree-a".to_string(),
                input_hash: "ih-rust".to_string(),
                check_name: "rust".to_string(),
                exit_code: 101,
                passed: false,
                output_tail: "assertion failed".to_string(),
                duration_ms: 1,
                target_results_json: None,
                job_id: Some("job-a".to_string()),
                cached: Some(false),
                failure_kind: None,
                executor_id: None,
                executor_device_id: None,
                executor_connection_generation: None,
                executor_cell_id: None,
                executor_lease_epoch: None,
                executor_started_at_unix_ms: None,
                executor_finished_at_unix_ms: None,
                toolchain_fingerprint: None,
                defined_by_commit_sha: Some("commit-a".to_string()),
            },
        )
        .unwrap();

        let plans = vec![(plan("rust", "cargo test"), "ih-rust".to_string())];
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = calls.clone();
        let results = run_planned_checks(
            db.clone(),
            "project-a",
            "tree-b",
            "job-a",
            &plans,
            "tool",
            CheckExecMode::Shared,
            None,
            move |_index, _command, _stream_id| {
                let counted = counted.clone();
                async move {
                    counted.fetch_add(1, Ordering::SeqCst);
                    exec_ok(Some(0), "ok")
                }
            },
            |_| {},
        )
        .await;

        assert_eq!(calls.load(Ordering::SeqCst), 1, "the check runs again");
        assert!(results[0].passed);
        assert_eq!(results[0].suppressed_after, None);
    }

    /// A suite abandoned mid-flight — the shape a superseded review wave takes
    /// when its cancellation lever fires — must leave no verdict behind. A
    /// half-finished check is not a result, and persisting one would let a tree
    /// nobody validated read as checked.
    #[tokio::test]
    async fn abandoning_a_running_miss_stores_nothing() {
        let db = cache_db().await;
        let started = Arc::new(AtomicUsize::new(0));
        let task = {
            let db = db.clone();
            let started = started.clone();
            tokio::spawn(async move {
                let plans = vec![(plan("queued", "run-queued"), "ih-queued".to_string())];
                run_planned_checks(
                    db,
                    "project-a",
                    "tree-cancelled",
                    "job-a",
                    &plans,
                    "tool",
                    CheckExecMode::Isolated,
                    None,
                    move |_index, _command, _stream_id| {
                        let started = started.clone();
                        async move {
                            started.fetch_add(1, Ordering::SeqCst);
                            // Outlives the abort below, so the check is still
                            // running when its suite is dropped.
                            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                            exec_ok(Some(0), "never completes")
                        }
                    },
                    |_| {},
                )
                .await
            })
        };
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert_eq!(
            started.load(Ordering::SeqCst),
            1,
            "the check must be in flight when the suite is abandoned"
        );
        task.abort();
        let _ = task.await;
        assert!(
            crate::execution::cache::list_check_results(db, "project-a", "tree-cancelled")
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn cache_miss_runs_then_stores() {
        let db = cache_db().await;
        // A command with no recognized runner, so this stays a test about cache
        // write-through rather than about failure classification (which
        // `miss_classifies_and_persists_timeout_spawn_and_signal` covers).
        let plans = vec![(plan("frontend", "bun run check:web"), "ih-b".to_string())];
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = calls.clone();
        let results = run_planned_checks(
            db.clone(),
            "project-a",
            "tree-b",
            "job-a",
            &plans,
            "tool",
            CheckExecMode::Shared,
            None,
            move |_index, _command, _stream_id| {
                let counted = counted.clone();
                async move {
                    counted.fetch_add(1, Ordering::SeqCst);
                    exec_ok(Some(1), "check failed")
                }
            },
            |_| {},
        )
        .await;

        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "a miss runs the check once"
        );
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name, "frontend");
        assert!(!results[0].passed);
        assert_eq!(results[0].exit_code, Some(1));

        assert!(
            get_check_result(db.clone(), "project-a", "frontend", "ih-b")
                .unwrap()
                .is_none(),
            "a failed verdict is stored for visibility but is not reusable"
        );
        let stored =
            crate::execution::cache::list_check_results(db, "project-a", "tree-b").unwrap();
        assert_eq!(stored.len(), 1, "a miss stores exactly one visible result");
        let stored = &stored[0];
        assert_eq!(stored.input_hash, "ih-b");
        assert_eq!(stored.check_name, "frontend");
        assert_eq!(stored.exit_code, 1);
        assert!(!stored.passed);
        assert_eq!(stored.failure_kind, None);
        assert_eq!(stored.output_tail, "check failed");
        assert_eq!(stored.job_id.as_deref(), Some("job-a"));
        assert_eq!(stored.cached, Some(false));
        assert_eq!(stored.executor_id, None);
        assert_eq!(stored.executor_device_id, None);
        assert_eq!(stored.executor_connection_generation, None);
        assert_eq!(stored.executor_cell_id, None);
        assert_eq!(stored.executor_lease_epoch, None);
        assert_eq!(stored.executor_started_at_unix_ms, None);
        assert_eq!(stored.executor_finished_at_unix_ms, None);
        assert_eq!(
            stored.toolchain_fingerprint.as_deref(),
            Some(check_toolchain_identity())
        );
    }

    #[tokio::test]
    async fn cache_miss_persists_structured_results() {
        let db = cache_db().await;
        let plans = vec![(
            plan("rust", "bun run test:rust:nextest"),
            "ih-structured".to_string(),
        )];
        let nextest_output = "     Summary [   0.1s] 3 tests run: 1 passed, 2 failed, 0 skipped\n\
            \x20       FAIL [   0.0s] (1/3) mycrate mod::test_a\n\
            \x20       FAIL [   0.0s] (2/3) mycrate mod::test_b"
            .to_string();
        let results = run_planned_checks(
            db.clone(),
            "project-a",
            "tree-structured",
            "job-a",
            &plans,
            "tool",
            CheckExecMode::Shared,
            None,
            move |_index, _command, _stream_id| {
                let out = nextest_output.clone();
                async move { exec_ok(Some(100), out) }
            },
            |_| {},
        )
        .await;

        // The outcome carries the parsed per-test detail.
        let parsed = results[0].parsed.as_ref().expect("nextest output parses");
        assert_eq!(parsed.parser, "nextest");
        assert_eq!(parsed.failed, 2);
        assert_eq!(parsed.failures.len(), 2);

        // The failed verdict is not reusable, but its structured evidence remains
        // visible on the tree-scoped result row for diagnostics and baseline work.
        assert!(
            get_check_result(db.clone(), "project-a", "rust", "ih-structured")
                .unwrap()
                .is_none(),
            "a failed verdict is stored for visibility but is not reusable"
        );
        let stored =
            crate::execution::cache::list_check_results(db, "project-a", "tree-structured")
                .unwrap();
        assert_eq!(stored.len(), 1, "a miss stores exactly one visible result");
        let stored = &stored[0];
        assert_eq!(stored.input_hash, "ih-structured");
        assert_eq!(stored.check_name, "rust");
        assert_eq!(stored.exit_code, 100);
        assert!(!stored.passed);
        assert_eq!(stored.failure_kind, None);
        assert_eq!(stored.job_id.as_deref(), Some("job-a"));
        assert_eq!(stored.cached, Some(false));
        assert_eq!(stored.executor_id, None);
        assert_eq!(stored.executor_device_id, None);
        assert_eq!(stored.executor_connection_generation, None);
        assert_eq!(stored.executor_cell_id, None);
        assert_eq!(stored.executor_lease_epoch, None);
        assert_eq!(stored.executor_started_at_unix_ms, None);
        assert_eq!(stored.executor_finished_at_unix_ms, None);
        assert_eq!(
            stored.toolchain_fingerprint.as_deref(),
            Some(check_toolchain_identity())
        );
        let json = stored
            .target_results_json
            .as_deref()
            .expect("structured results persisted");
        assert!(json.contains("\"parser\":\"nextest\""));
        assert!(json.contains("mycrate mod::test_a"));
        assert!(json.contains("mycrate mod::test_b"));
    }

    /// The repro this whole change fixes. A src-tauri commit runs the rust check
    /// and records an immutable reusable observation keyed by the src-tauri input hash. A following
    /// doc-only commit moves the WHOLE-tree hash but leaves that input hash
    /// unchanged, so the verdict is a cache HIT — rust does not re-run — and the
    /// commit receives a cached alias rather than mutating its source evidence.
    #[tokio::test]
    async fn doc_only_commit_reuses_impact_scoped_verdict() {
        let db = cache_db().await;
        let calls = Arc::new(AtomicUsize::new(0));

        // Commit 1 touches src-tauri and produced a complete immutable reusable
        // observation for input hash IH1 at whole-tree tree-1.
        let plans = vec![(plan("rust", "bun run test:rust"), "IH1".to_string())];
        let environment = plan_environment_fingerprint(&plans[0].0);
        let mut source = reusable_observation_for("rust", "IH1", environment.clone());
        source.commit_sha = "commit-1".to_string();
        source.tree_hash = "tree-1".to_string();
        record_fresh_check_observation(db.clone(), source).unwrap();

        // Commit 2 is doc-only: the whole tree changes to tree-2, but the rust
        // input hash is UNCHANGED (still IH1), so the verdict is a cache hit and
        // the check does not re-run.
        let counted = calls.clone();
        let r2 = run_planned_checks_at_commit(
            db.clone(),
            "project-a",
            CheckRunCommit {
                evaluated: "commit-2",
                defined_by: "commit-2",
            },
            "tree-2",
            "job-a",
            &plans,
            "tool",
            CheckExecMode::Shared,
            None,
            move |_index, _command, _stream_id| {
                let counted = counted.clone();
                async move {
                    counted.fetch_add(1, Ordering::SeqCst);
                    exec_ok(Some(0), "ran")
                }
            },
            |_| {},
        )
        .await;
        assert_eq!(r2.len(), 1);
        assert_eq!(r2[0].name, "rust");
        assert!(r2[0].passed);
        assert_eq!(r2[0].exit_code, Some(0));
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "a doc-only commit must not re-run the rust check"
        );

        let alias = crate::execution::cache::get_check_result_observation(
            db,
            "project-a",
            "commit-2",
            "rust",
            &environment,
            crate::execution::check_identity::CHECK_RESULT_SCHEMA_VERSION as i64,
            None,
            None,
            1,
            0,
        )
        .unwrap()
        .expect("the doc commit must receive a cached alias");
        assert_eq!(alias.disposition, "cached");
        assert_eq!(alias.evaluated_tree_hash, "tree-2");
    }

    /// The three abnormal deaths a check can suffer are each classified,
    /// persisted durably, and rendered as themselves — the core of this change.
    #[tokio::test]
    async fn miss_classifies_and_persists_timeout_spawn_and_signal() {
        let db = cache_db().await;
        let plans = vec![
            (plan("slow", "run-slow"), "ih-slow".to_string()),
            (plan("nogo", "run-nogo"), "ih-nogo".to_string()),
            (plan("crash", "run-crash"), "ih-crash".to_string()),
        ];
        let results = run_planned_checks(
            db.clone(),
            "project-a",
            "tree-cls",
            "job-a",
            &plans,
            "tool",
            CheckExecMode::Shared,
            None,
            move |index, _command, _stream_id| async move {
                match index {
                    // Killed at its budget, with a nextest SLOW line naming the
                    // test in flight at the kill.
                    0 => Ok(CheckExecResult {
                        exit_code: None,
                        output: "     SLOW [>  60.000s] mycrate mod::hangs\nstill going"
                            .to_string(),
                        timed_out: true,
                        duration_ms: None,
                        provenance: None,
                        publication: None,
                    }),
                    // The process could not be spawned.
                    1 => Err("Failed to spawn command: No such file or directory".to_string()),
                    // Died by signal mid-run (no exit code, not a timeout).
                    _ => Ok(CheckExecResult {
                        exit_code: None,
                        output: "segfault".to_string(),
                        timed_out: false,
                        duration_ms: None,
                        provenance: None,
                        publication: None,
                    }),
                }
            },
            |_| {},
        )
        .await;

        assert_eq!(results[0].failure_kind, Some(CheckFailureKind::TimedOut));
        assert_eq!(results[1].failure_kind, Some(CheckFailureKind::SpawnError));
        assert_eq!(results[2].failure_kind, Some(CheckFailureKind::Killed));
        assert!(results.iter().all(|o| !o.passed));

        // The classification is persisted, so every downstream surface can render
        // the real death rather than re-deriving it from exit -1.
        let stored =
            crate::execution::cache::list_check_results(db.clone(), "project-a", "tree-cls")
                .unwrap();
        let kind_of = |name: &str| {
            stored
                .iter()
                .find(|row| row.check_name == name)
                .and_then(|row| row.failure_kind.as_deref())
        };
        assert_eq!(kind_of("slow"), Some("timed_out"));
        assert_eq!(kind_of("nogo"), Some("spawn_error"));
        assert_eq!(kind_of("crash"), Some("killed"));
        assert!(get_check_result(db.clone(), "project-a", "slow", "ih-slow")
            .unwrap()
            .is_none());

        // The timeout summary names the timeout AND the still-running test; the
        // spawn error names itself.
        let summary = format_check_summary(&results);
        assert!(summary.contains("timed out after"), "got: {summary}");
        assert!(summary.contains("mycrate mod::hangs"), "got: {summary}");
        assert!(summary.contains("failed to spawn"), "got: {summary}");
    }

    // --- live status snapshots + sequential ordering ----------------------

    fn find<'a>(snap: &'a [CheckStatusEntry], name: &str) -> &'a CheckStatusEntry {
        snap.iter()
            .find(|e| e.name == name)
            .unwrap_or_else(|| panic!("no `{name}` entry in snapshot"))
    }

    /// The notify callback receives a full checklist snapshot on every
    /// transition: the planned set is all-pending, a cache hit jumps straight to
    /// its final state (annotated `cached`, never `running`), and a miss passes
    /// through `running` before its annotated final state.
    #[tokio::test]
    async fn notify_emits_planned_running_and_final_snapshots() {
        let db = cache_db().await;
        // frontend is already cached (passing); typecheck is a fresh miss.
        record_fresh_check_observation(
            db.clone(),
            reusable_observation(current_check_environment_fingerprint()),
        )
        .unwrap();

        let plans = vec![
            (plan("frontend", "run-frontend"), "ih-frontend".to_string()),
            (
                plan("typecheck", "run-typecheck"),
                "ih-typecheck".to_string(),
            ),
        ];
        let snapshots: Arc<std::sync::Mutex<Vec<Vec<CheckStatusEntry>>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let captured = snapshots.clone();
        run_planned_checks(
            db.clone(),
            "project-a",
            "tree-a",
            "job-a",
            &plans,
            "tool",
            CheckExecMode::Shared,
            None,
            // typecheck misses and fails with a bare exit code.
            move |_index, _command, _stream_id| async move { exec_ok(Some(1), "boom") },
            move |checks| captured.lock().unwrap().push(checks),
        )
        .await;

        let snaps = snapshots.lock().unwrap();
        assert!(
            snaps.len() >= 4,
            "planned + hit + running + final, got {}",
            snaps.len()
        );

        // First snapshot is the planned set: everything pending, unannotated.
        let planned = &snaps[0];
        assert!(planned
            .iter()
            .all(|e| e.state == "pending" && e.annotation.is_none()));

        // frontend was a cache hit: it reaches `passed` annotated `cached` and is
        // NEVER seen in a `running` state (no run phase for a hit).
        assert!(
            snaps.iter().all(|s| find(s, "frontend").state != "running"),
            "a cache hit must never pass through `running`"
        );
        let frontend_final = find(snaps.last().unwrap(), "frontend");
        assert_eq!(frontend_final.state, "passed");
        assert_eq!(frontend_final.annotation.as_deref(), Some("cached"));

        // typecheck (a miss) passes through `running` (unannotated) then `failed`
        // with the same annotation the final summary uses.
        assert!(
            snaps.iter().any(|s| {
                let e = find(s, "typecheck");
                e.state == "running" && e.annotation.is_none()
            }),
            "a miss must surface a `running` snapshot"
        );
        let typecheck_final = find(snaps.last().unwrap(), "typecheck");
        assert_eq!(typecheck_final.state, "failed");
        assert_eq!(typecheck_final.annotation.as_deref(), Some("exit 1"));
    }

    /// Outcomes — and the summary built from them — come back in plan order.
    #[tokio::test]
    async fn checks_return_and_summarize_in_plan_order() {
        let db = cache_db().await;
        let plans = vec![
            (plan("a", "cmd-a"), "ih-a".to_string()),
            (plan("b", "cmd-b"), "ih-b".to_string()),
            (plan("c", "cmd-c"), "ih-c".to_string()),
        ];
        let results = run_planned_checks(
            db.clone(),
            "project-a",
            "tree-p",
            "job-a",
            &plans,
            "tool",
            CheckExecMode::Shared,
            None,
            move |_index, _command, _stream_id| async move { exec_ok(Some(0), String::new()) },
            |_| {},
        )
        .await;

        let names: Vec<&str> = results.iter().map(|o| o.name.as_str()).collect();
        assert_eq!(names, vec!["a", "b", "c"], "outcomes must be in plan order");
        // The summary follows the same plan order (each name carries a duration
        // annotation, so match on relative position rather than exact text).
        let summary = format_check_summary(&results);
        let pos = |name: &str| summary.find(name).expect("name present in summary");
        assert!(
            pos("a") < pos("b") && pos("b") < pos("c"),
            "the summary must reflect plan order: {summary}"
        );
    }

    /// `Shared` is the canonical write-check mode and must never overlap two check
    /// commands in the one checkout: a mutating check (a formatter / `--fix` lint)
    /// has to settle before the next check observes the shared sealed worktree.
    /// Even when each executor yields at an await, the concurrent-invocation
    /// high-water mark stays 1.
    #[tokio::test]
    async fn shared_mode_checks_stay_sequential() {
        let db = cache_db().await;
        let plans = vec![
            (plan("x", "cmd-x"), "ih-x".to_string()),
            (plan("y", "cmd-y"), "ih-y".to_string()),
        ];
        let active = Arc::new(AtomicUsize::new(0));
        let high_water = Arc::new(AtomicUsize::new(0));
        let a = active.clone();
        let hw = high_water.clone();
        run_planned_checks(
            db.clone(),
            "project-a",
            "tree-c",
            "job-a",
            &plans,
            "tool",
            CheckExecMode::Shared,
            None,
            move |_index, _command, _stream_id| {
                let a = a.clone();
                let hw = hw.clone();
                async move {
                    let now = a.fetch_add(1, Ordering::SeqCst) + 1;
                    hw.fetch_max(now, Ordering::SeqCst);
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                    a.fetch_sub(1, Ordering::SeqCst);
                    exec_ok(Some(0), String::new())
                }
            },
            |_| {},
        )
        .await;

        assert_eq!(
            high_water.load(Ordering::SeqCst),
            1,
            "shared-mode checks must not overlap; exactly one may run at a time"
        );
    }

    #[tokio::test]
    async fn shared_mode_exposes_earlier_mutations_across_cache_hit_and_failure() {
        let db = cache_db().await;
        let plans = vec![
            (plan("formatter", "format"), "ih-format".to_string()),
            (plan("cached", "cached"), "ih-cached".to_string()),
            (plan("reader", "read"), "ih-read".to_string()),
        ];
        record_fresh_check_observation(
            db.clone(),
            reusable_observation_for(
                "cached",
                "ih-cached",
                current_check_environment_fingerprint(),
            ),
        )
        .unwrap();

        let checkout = Arc::new(tempfile::tempdir().unwrap());
        let marker = checkout.path().join("formatted.txt");
        let observed = Arc::new(std::sync::Mutex::new(Vec::new()));
        let marker_for_run = marker.clone();
        let observed_for_run = observed.clone();
        let results = run_planned_checks(
            db,
            "project-a",
            "tree-shared",
            "job-a",
            &plans,
            "tool",
            CheckExecMode::Shared,
            None,
            move |index, _command, _stream_id| {
                let marker = marker_for_run.clone();
                let observed = observed_for_run.clone();
                async move {
                    observed.lock().unwrap().push(index);
                    match index {
                        0 => {
                            std::fs::write(&marker, "settled formatter output").unwrap();
                            exec_ok(Some(1), "formatter reported a failure")
                        }
                        2 => {
                            let contents = std::fs::read_to_string(&marker)
                                .expect("later check sees the earlier in-place mutation");
                            assert_eq!(contents, "settled formatter output");
                            exec_ok(Some(0), contents)
                        }
                        _ => panic!("the cached plan must not execute"),
                    }
                }
            },
            |_| {},
        )
        .await;

        assert_eq!(*observed.lock().unwrap(), vec![0, 2]);
        assert!(
            !results[0].passed,
            "a failed mutating check is still recorded"
        );
        assert!(results[1].cached, "the middle plan remains a cache hit");
        assert!(results[2].passed);
        assert_eq!(
            std::fs::read_to_string(marker).unwrap(),
            "settled formatter output",
            "the mutation remains in the real checkout for the canonical fold"
        );
    }

    /// In `Isolated` mode the misses run CONCURRENTLY. Each executor signals
    /// entry into a 2-party barrier and then awaits the other; both must be
    /// in-flight at once for the barrier to release. Sequential execution would
    /// park the first executor forever and trip the timeout, so a pass proves
    /// genuine overlap.
    #[tokio::test]
    async fn isolated_checks_run_concurrently() {
        let db = cache_db().await;
        let plans = vec![
            (plan("x", "cmd-x"), "ih-x".to_string()),
            (plan("y", "cmd-y"), "ih-y".to_string()),
        ];
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let b = barrier.clone();
        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            run_planned_checks(
                db.clone(),
                "project-a",
                "tree-iso",
                "job-a",
                &plans,
                "tool",
                CheckExecMode::Isolated,
                None,
                move |_index, _command, _stream_id| {
                    let b = b.clone();
                    async move {
                        b.wait().await;
                        exec_ok(Some(0), String::new())
                    }
                },
                |_| {},
            ),
        )
        .await;

        let results = outcome.expect(
            "isolated checks must run concurrently; the rendezvous timed out (ran sequentially?)",
        );
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|o| o.passed));
    }

    /// Concurrent misses may complete out of plan order (here the later-in-plan
    /// checks finish FIRST), but the runner reassembles outcomes into plan order.
    #[tokio::test]
    async fn isolated_results_reassemble_into_plan_order() {
        let db = cache_db().await;
        let plans = vec![
            (plan("a", "cmd-a"), "ih-a".to_string()),
            (plan("b", "cmd-b"), "ih-b".to_string()),
            (plan("c", "cmd-c"), "ih-c".to_string()),
        ];
        let results = run_planned_checks(
            db.clone(),
            "project-a",
            "tree-rev",
            "job-a",
            &plans,
            "tool",
            CheckExecMode::Isolated,
            None,
            // index 0 sleeps longest, so completion order reverses plan order.
            move |index, _command, _stream_id| async move {
                let delay = (3 - index as u64) * 20;
                tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                exec_ok(Some(0), String::new())
            },
            |_| {},
        )
        .await;

        let names: Vec<&str> = results.iter().map(|o| o.name.as_str()).collect();
        assert_eq!(
            names,
            vec!["a", "b", "c"],
            "results must reassemble into plan order despite reversed completion"
        );
    }

    /// Concurrent transitions still emit FULL snapshots, and the last snapshot has
    /// every entry in a final (passed/failed) state.
    #[tokio::test]
    async fn isolated_concurrent_transitions_end_all_final() {
        let db = cache_db().await;
        let plans = vec![
            (plan("x", "cmd-x"), "ih-x".to_string()),
            (plan("y", "cmd-y"), "ih-y".to_string()),
            (plan("z", "cmd-z"), "ih-z".to_string()),
        ];
        let snapshots: Arc<std::sync::Mutex<Vec<Vec<CheckStatusEntry>>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let captured = snapshots.clone();
        run_planned_checks(
            db.clone(),
            "project-a",
            "tree-snap",
            "job-a",
            &plans,
            "tool",
            CheckExecMode::Isolated,
            None,
            // z fails, the others pass — mixed final states under concurrency.
            move |index, _command, _stream_id| async move {
                let code = if index == 2 { 1 } else { 0 };
                exec_ok(Some(code), String::new())
            },
            move |checks| captured.lock().unwrap().push(checks),
        )
        .await;

        let snaps = snapshots.lock().unwrap();
        assert!(
            snaps.iter().all(|s| s.len() == 3),
            "every snapshot carries the full checklist"
        );
        let last = snaps.last().expect("at least the planned snapshot emitted");
        assert!(
            last.iter()
                .all(|e| e.state == "passed" || e.state == "failed"),
            "the final snapshot must have every entry final: {last:?}"
        );
        assert_eq!(find(last, "z").state, "failed");
    }

    /// `Isolated` mode runs its misses concurrently. Capping that concurrency is
    /// the executor's job, reached through the reservation each submission
    /// declares — there is deliberately no second cap here, so all three misses
    /// overlap.
    #[tokio::test]
    async fn isolated_misses_run_concurrently() {
        let db = cache_db().await;
        let plans = vec![
            (plan("a", "cmd-a"), "ih-a".to_string()),
            (plan("b", "cmd-b"), "ih-b".to_string()),
            (plan("c", "cmd-c"), "ih-c".to_string()),
        ];
        let active = Arc::new(AtomicUsize::new(0));
        let high_water = Arc::new(AtomicUsize::new(0));
        let a = active.clone();
        let hw = high_water.clone();
        let results = run_planned_checks(
            db.clone(),
            "project-a",
            "tree-bounded",
            "job-a",
            &plans,
            "tool",
            CheckExecMode::Isolated,
            None,
            move |_index, _command, _stream_id| {
                let a = a.clone();
                let hw = hw.clone();
                async move {
                    let now = a.fetch_add(1, Ordering::SeqCst) + 1;
                    hw.fetch_max(now, Ordering::SeqCst);
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                    a.fetch_sub(1, Ordering::SeqCst);
                    exec_ok(Some(0), String::new())
                }
            },
            |_| {},
        )
        .await;

        assert_eq!(results.len(), 3);
        assert_eq!(
            high_water.load(Ordering::SeqCst),
            3,
            "isolated misses must not be serialized by a runner-local gate"
        );
    }

    fn parsed(parser: &str, passed: usize, failed: usize) -> ParsedCheckResult {
        ParsedCheckResult {
            schema_version: 1,
            complete: false,
            selection: "unknown".to_string(),
            tests: vec![],
            undeclared_skips: 0,
            parser: parser.to_string(),
            passed,
            failed,
            skipped: 0,
            suite_failures: 0,
            failures: Vec::new(),
        }
    }

    #[test]
    fn active_build_service_failure_preflights_without_relabeling_spawn_outcomes() {
        let config_fingerprint = "active-config".to_string();
        let snapshot = crate::orchestrator::build_services::BuildServiceDiagnosticSnapshot {
            name: "sccache".to_string(),
            configured: true,
            enabled: true,
            supervised_child: false,
            config_fingerprint: Some(config_fingerprint.clone()),
            state_dir: None,
            error_log_tail: Some("historical log entry".to_string()),
            runtime: crate::orchestrator::build_services::BuildServiceRuntimeDiagnostic {
                current_failure: Some(
                    "sccache: error: Address already in use (os error 48)".to_string(),
                ),
                failure_config: Some(config_fingerprint),
                ..Default::default()
            },
        };
        assert_eq!(
            active_build_service_failure(&snapshot).as_deref(),
            Some("sccache port conflict: sccache: error: Address already in use (os error 48)")
        );

        let mut disabled = snapshot.clone();
        disabled.enabled = false;
        assert_eq!(active_build_service_failure(&disabled), None);

        let mut replaced = snapshot.clone();
        replaced.config_fingerprint = Some("replacement-config".to_string());
        assert_eq!(active_build_service_failure(&replaced), None);

        let outcome = CellOutcome::Unavailable {
            reason: cairn_common::executor_protocol::CellUnavailableReason::Spawn,
            diagnostic: "sandbox denial cannot be adjudicated without runner context".to_string(),
        };
        let failure = check_result_from_cell_outcome(outcome, None).unwrap_err();
        assert_eq!(
            failure,
            CheckExecutionFailure::substrate(
                SubstrateFailureShape::Dispatch,
                "Spawn: sandbox denial cannot be adjudicated without runner context"
            )
        );
    }

    #[test]
    fn failure_classifier_requires_positive_evidence_and_preserves_precedence() {
        // A command with no recognized runner, so the Vitest arms stay clear of
        // the infrastructure-evidence precedence under test.
        let opaque = "bun run check:web";
        let warning = "sccache: warning: The server looks like it shut down unexpectedly";
        assert_eq!(
            classify_check_failure(opaque, Some(0), false, false, None, warning),
            None
        );
        assert_eq!(
            classify_check_failure(opaque, Some(254), false, false, None, "script exited 254"),
            None
        );

        let abnormal = "error: process didn't exit successfully: sccache rustc --crate-name bytes (exit status: 254)";
        assert_eq!(
            classify_check_failure(opaque, Some(254), false, false, None, abnormal)
                .map(|classification| classification.kind),
            Some(CheckFailureKind::Infrastructure)
        );
        for signature in [
            "Failed to send data to or receive data from server",
            "failed client/server communication",
            "failed to fill whole buffer",
            "server looks like it shut down unexpectedly",
        ] {
            assert_eq!(
                classify_check_failure(opaque, Some(1), false, false, None, signature)
                    .map(|classification| classification.kind),
                Some(CheckFailureKind::Infrastructure),
                "signature: {signature}"
            );
        }
        let missing =
            "couldn't read target/debug/build/tree/out/generated.txt: No such file or directory";
        assert_eq!(
            classify_check_failure(opaque, Some(1), false, false, None, missing)
                .map(|classification| classification.kind),
            Some(CheckFailureKind::Infrastructure)
        );
        assert_eq!(
            classify_check_failure(
                VITEST_COMMAND,
                Some(1),
                false,
                false,
                Some(&parsed("vitest", 2, 1)),
                warning
            ),
            None,
            "real assertion failures outrank incidental infrastructure text"
        );
    }

    const VITEST_COMMAND: &str = "bunx vitest related --reporter=default --reporter=json src/a.ts";

    /// A parse shaped like Vitest's when a test FILE failed to collect: no
    /// assertion failed, so `failed` stays zero while the file itself is a named
    /// failure site.
    fn suite_failed(passed: usize, suites: usize) -> ParsedCheckResult {
        let mut result = parsed("vitest", passed, 0);
        result.suite_failures = suites;
        result.failures = (0..suites)
            .map(|i| crate::execution::check_parsers::CheckFailure {
                name: format!("src/s{i}.test.tsx"),
                message: Some("Cannot find module './missing'".to_string()),
            })
            .collect();
        result
    }

    #[test]
    fn runner_error_is_vitest_only_and_reports_progress() {
        let vitest = parsed("vitest", 12, 0);
        let classification = classify_check_failure(
            VITEST_COMMAND,
            Some(1),
            false,
            false,
            Some(&vitest),
            "report complete",
        )
        .expect("abnormal Vitest exit");
        assert_eq!(classification.kind, CheckFailureKind::RunnerError);
        assert!(classification.reason.contains("12 tests passed"));
        assert_eq!(
            classify_check_failure(
                "bun run test:rust",
                Some(101),
                false,
                false,
                Some(&parsed("nextest", 0, 0)),
                "compile error"
            ),
            None
        );
    }

    #[test]
    fn a_suite_that_failed_to_collect_is_an_ordinary_named_failure() {
        // The file failed; no assertion did. Calling this a runner error would
        // bury the only thing that explains the red check (the file name and the
        // resolver error under it) behind "no assertion failures".
        assert_eq!(
            classify_check_failure(
                VITEST_COMMAND,
                Some(1),
                false,
                false,
                Some(&suite_failed(881, 1)),
                "FAIL src/s0.test.tsx"
            ),
            None
        );
    }

    #[test]
    fn an_error_escaping_a_test_file_is_named_as_such() {
        // Vitest's own JSON calls this run a success (every count clean,
        // `success` true) while the process exits 1. The only evidence is in the
        // output, so the classification has to point the excerpt at it.
        let output = concat!(
            "881 passed\n",
            " Unhandled Errors \n",
            "Vitest caught 2 unhandled errors during the test run.\n",
            "ReferenceError: window is not defined\n",
            "  at resolveUpdatePriority react-dom-client.development.js:1308:7\n"
        );
        let classification = classify_check_failure(
            VITEST_COMMAND,
            Some(1),
            false,
            false,
            Some(&parsed("vitest", 881, 0)),
            output,
        )
        .expect("unhandled errors fail the run");
        assert_eq!(classification.kind, CheckFailureKind::RunnerError);
        assert!(classification.reason.contains("escaped a test file"));
        let excerpt = classified_output_excerpt(output, Some(&classification));
        assert!(
            excerpt.contains("window is not defined"),
            "the excerpt must carry the escaping error: {excerpt}"
        );
    }

    #[test]
    fn vitest_exiting_without_a_report_is_named_a_runner_error() {
        // No JSON object at all: Vitest died loading its config or its deps, so
        // nothing was ever collected. Left unclassified this renders as a bare
        // `exit 1` beside a tail of module-resolution noise.
        let output = concat!(
            "failed to load config from /slot-176/vitest.config.ts\n",
            "Error [ERR_MODULE_NOT_FOUND]: Cannot find package 'vitest'\n",
            "Exit code: 1\n"
        );
        let classification =
            classify_check_failure(VITEST_COMMAND, Some(1), false, false, None, output)
                .expect("a Vitest run that produced no report");
        assert_eq!(classification.kind, CheckFailureKind::RunnerError);
        assert!(classification.reason.contains("without producing a report"));
    }

    #[test]
    fn review_batch_selectors_union_toolchains_and_reject_scalar_conflicts() {
        let item = |index, executor| PlannedCheckBatchItem {
            index,
            name: format!("check-{index}"),
            input_hash: format!("hash-{index}"),
            resource_identity_key: format!("resource-{index}"),
            command: "true".into(),
            stream_id: format!("stream-{index}"),
            env: Vec::new(),
            timeout_ms: 1,
            executor: Some(executor),
            resource_class: CheckResourceClass::Shared,
        };
        let merged = merge_batch_executor(&[
            item(
                0,
                ExecutorSelector {
                    os: Some("linux".into()),
                    required_toolchains: vec!["rust".into()],
                    ..ExecutorSelector::default()
                },
            ),
            item(
                1,
                ExecutorSelector {
                    os: Some("linux".into()),
                    required_toolchains: vec!["bun".into(), "rust".into()],
                    ..ExecutorSelector::default()
                },
            ),
        ])
        .unwrap()
        .unwrap();
        assert_eq!(merged.os.as_deref(), Some("linux"));
        assert_eq!(merged.required_toolchains, vec!["bun", "rust"]);

        let conflict = merge_batch_executor(&[
            item(
                0,
                ExecutorSelector {
                    name: Some("bglab-ub".into()),
                    ..ExecutorSelector::default()
                },
            ),
            item(
                1,
                ExecutorSelector {
                    name: Some("bglab-mac".into()),
                    ..ExecutorSelector::default()
                },
            ),
        ])
        .unwrap_err();
        assert!(conflict.contains("conflicting review check executor selector name"));

        // A named machine and a bare platform are different questions, and one
        // batch cannot honor both.
        let mixed = merge_batch_executor(&[
            item(
                0,
                ExecutorSelector {
                    name: Some("bglab-ub".into()),
                    ..ExecutorSelector::default()
                },
            ),
            item(
                1,
                ExecutorSelector {
                    os: Some("linux".into()),
                    ..ExecutorSelector::default()
                },
            ),
        ])
        .unwrap_err();
        assert!(mixed.contains("conflicting executor selectors"), "{mixed}");
    }

    #[test]
    fn pure_verdict_batches_keep_untargeted_and_targeted_checks_separate() {
        let item = |index, executor| PlannedCheckBatchItem {
            index,
            name: format!("check-{index}"),
            input_hash: format!("hash-{index}"),
            resource_identity_key: format!("resource-{index}"),
            command: "true".into(),
            stream_id: format!("stream-{index}"),
            env: Vec::new(),
            timeout_ms: 1,
            executor,
            resource_class: CheckResourceClass::Shared,
        };
        let groups = partition_check_items_by_executor(vec![
            item(0, None),
            item(
                1,
                Some(ExecutorSelector {
                    os: Some("linux".into()),
                    ..Default::default()
                }),
            ),
            item(2, None),
        ]);
        assert_eq!(groups.len(), 2);
        assert_eq!(
            groups[0]
                .1
                .iter()
                .map(|item| item.index)
                .collect::<Vec<_>>(),
            vec![0, 2]
        );
        assert_eq!(
            groups[1]
                .1
                .iter()
                .map(|item| item.index)
                .collect::<Vec<_>>(),
            vec![1]
        );
    }

    #[test]
    fn classified_excerpt_keeps_early_evidence_and_final_tail_bounded() {
        let output = format!(
            "failed to fill whole buffer\n{}\nFINAL-MARKER",
            "noise\n".repeat(2000)
        );
        let classification =
            classify_check_failure("bun run check:web", Some(1), false, false, None, &output);
        let excerpt = classified_output_excerpt(&output, classification.as_ref());
        assert!(excerpt.contains("failed to fill whole buffer"));
        assert!(excerpt.contains("FINAL-MARKER"));
        assert!(excerpt.chars().count() <= OUTPUT_TAIL_CHARS);
    }

    #[test]
    fn build_slot_deadline_formats_truthful_capacity_and_stall_evidence() {
        let now = unix_time_ms_for_checks();
        let outcome = |state, last_progress_unix_ms| CellOutcome::Unavailable {
            reason: cairn_common::executor_protocol::CellUnavailableReason::Deadline {
                host_pressure: None,
                substrate: Some(cairn_common::executor_protocol::ExecutorSubstrateEvidence {
                    state,
                    since_unix_ms: last_progress_unix_ms,
                    last_progress_unix_ms,
                    diagnostic: None,
                    queue_depth: Some(4),
                    queue_position: Some(3),
                    active_cell_count: Some(2),
                    oldest_running_started_at_unix_ms: Some(now.saturating_sub(500)),
                }),
            },
            diagnostic: "acquisition deadline elapsed".into(),
        };

        let fresh = check_result_from_cell_outcome(
            outcome(
                cairn_common::executor_protocol::ExecutorSubstrateState::CapacityBusy,
                now,
            ),
            None,
        )
        .unwrap_err();
        let CheckExecutionFailure::Substrate(fresh) = fresh else {
            panic!("deadline must be a substrate failure");
        };
        let fresh = fresh.diagnostic();
        assert!(fresh.contains("substrate=CapacityBusy"));
        assert!(!fresh.contains("ConnectedStalled"));

        let stale = check_result_from_cell_outcome(
            outcome(
                cairn_common::executor_protocol::ExecutorSubstrateState::ConnectedStalled,
                now.saturating_sub(
                    cairn_common::executor_protocol::EXECUTOR_PROGRESS_FRESHNESS_MS + 1,
                ),
            ),
            None,
        )
        .unwrap_err();
        let CheckExecutionFailure::Substrate(stale) = stale else {
            panic!("deadline must be a substrate failure");
        };
        // Every capacity fact survives — in the operator half. The agent half is
        // authored text that names none of it.
        let agent = stale.agent_message();
        let stale = stale.diagnostic();
        assert!(stale.contains("substrate=ConnectedStalled"));
        assert!(stale.contains("lastProgressAge="));
        assert!(stale.contains("queueDepth=4"));
        assert!(stale.contains("queuePosition=3"));
        assert!(stale.contains("activeSlots=2"));
        for fact in ["substrate=", "queueDepth", "activeSlots", "build-slot"] {
            assert!(
                !agent.contains(fact),
                "agent message must not carry {fact}: {agent}"
            );
        }
    }

    fn reusable_test_result() -> ParsedCheckResult {
        ParsedCheckResult {
            schema_version: 1,
            complete: true,
            selection: "full".to_string(),
            tests: vec![crate::execution::check_parsers::CheckTestResult {
                id: "suite::passes".to_string(),
                status: "passed".to_string(),
                duration_ms: Some(1),
                attempts: 1,
                retried: false,
                flaky: false,
                failure_message: None,
                skip_reason: None,
                skip_declaration: None,
                skip_declaration_source: None,
            }],
            undeclared_skips: 0,
            parser: "nextest".to_string(),
            passed: 1,
            failed: 0,
            suite_failures: 0,
            skipped: 0,
            failures: Vec::new(),
        }
    }

    #[test]
    fn strict_reuse_policy_accepts_only_complete_stable_test_green() {
        let result = reusable_test_result();
        assert!(check_reuse_decision("bun run test:rust", true, None, Some(&result)).reusable);

        let mut incomplete = result.clone();
        incomplete.complete = false;
        assert!(!check_reuse_decision("bun run test:rust", true, None, Some(&incomplete)).reusable);

        let mut empty = result.clone();
        empty.selection = "empty".to_string();
        empty.passed = 0;
        assert!(!check_reuse_decision("bun run test:rust", true, None, Some(&empty)).reusable);

        let mut retried = result.clone();
        retried.tests[0].attempts = 2;
        retried.tests[0].retried = true;
        assert!(!check_reuse_decision("bun run test:rust", true, None, Some(&retried)).reusable);

        let mut flaky = result.clone();
        flaky.tests[0].flaky = true;
        assert!(!check_reuse_decision("bun run test:rust", true, None, Some(&flaky)).reusable);

        let mut undeclared = result;
        undeclared.undeclared_skips = 1;
        assert!(!check_reuse_decision("bun run test:rust", true, None, Some(&undeclared)).reusable);
    }

    #[test]
    fn non_test_green_reuses_but_red_and_infrastructure_never_do() {
        assert!(check_reuse_decision("bunx tsc --noEmit", true, None, None).reusable);
        assert!(!check_reuse_decision("bunx tsc --noEmit", false, None, None).reusable);
        assert!(
            !check_reuse_decision(
                "bunx tsc --noEmit",
                true,
                Some(CheckFailureKind::Infrastructure),
                None,
            )
            .reusable
        );
        assert!(!check_reuse_decision("bun run test:rust", true, None, None).reusable);
    }
}
