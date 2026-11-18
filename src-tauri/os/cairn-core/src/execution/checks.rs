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
//! ## One commit: the one the write sealed
//!
//! Everything a wave reads comes from the commit the triggering call sealed: the
//! `checks` contract that commit declares in its own `.cairn/config.yaml`
//! ([`checks_contract_at_commit`], CAIRN-3333), the changed-file set, impact-glob
//! matching, the cache tree hash, and the tree the slot materializes. The caller
//! names that commit through [`WriteCheckTarget`]; a `write` prepares against the
//! branch's pre-write tip, so its resolution alone would evaluate the parent.
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
use std::sync::Arc;
use std::time::Instant;

// Isolated checks carry their configured executor selector into the immutable request.
use crate::config::project_settings::{
    CheckCommand, CheckPolicy, CheckResourceClass, CheckWhen, ChecksContract,
};
use crate::execution::cache::{
    claim_check_execution, claim_infra_escalation, get_check_result, get_reusable_check_result,
    infra_suppression_scope, list_latest_passing_check_results_for_job,
    record_cached_check_observation, record_fresh_check_observation, store_check_result,
    CachedCheckObservationWrite, CheckExecutionClaim, CheckResultCacheEntry, CheckResultCacheWrite,
    CheckTestResultRow, FreshCheckObservationWrite, InfraRepairDomain, InfraScopes,
    ReusableCheckLookup, SuppressedCheck,
};
use crate::execution::inputs::{
    any_check_declares_inputs, InputSelector, ResolvedInputs, TreeBlobs, TreeSnapshot,
};
#[cfg(test)]
use crate::execution::wire::{
    batch_command_class, check_resource_identity, declared_batch_reservation,
};
use crate::execution::wire::{
    batch_placement_mobility, build_check_batch_wire, build_review_check_request,
    declared_check_reservation, CheckResourceIdentityInput, ReviewCheckWireInput,
};
use crate::fleet::{CellOutcome, CellPriority, CellRequest, MutationPolicy};
use crate::mcp::handlers::branch::{CapturedRevision, TypedRevision};
#[cfg(test)]
use cairn_common::executor_protocol::{CellCommandClass, ResourceReservationSource};
use cairn_common::executor_protocol::{
    ExecutorSelector, PlacementMobility, ProjectCapability, RepositoryLocator,
};
use cairn_platform::stack::off_frame;
use cairn_vcs::PublicationIdentity;

mod board;
mod failure;
mod manual;
mod planning;
mod render;
pub(crate) mod requests;
mod write_wave;

#[cfg(test)]
use board::wave_progress;
use board::{check_phase_status, wave_status_notify, WaveProgress, WaveStage};
pub(crate) use board::{BatchProgress, CheckPhaseStatus, CheckStatusBoard};
pub use failure::CheckFailureKind;
use failure::{
    active_build_service_failure, append_sandbox_denial_evidence,
    append_tracked_modification_evidence, check_result_from_delivery, classified_output_excerpt,
    classify_check_failure, failure_cause_and_evidence, infra_repair_domain,
    infrastructure_failure_log_line, stopped_command_failure, substrate_failure_kind, tail_lines,
    with_build_service_advisory, FailureClassification, OUTPUT_TAIL_CHARS,
};
#[cfg(test)]
use failure::{
    bounded_diagnostic, format_timeout_budget, no_start, omission_marker, ImpliedRequirement,
    NoStart, INLINE_DIAGNOSTIC_HEAD_CHARS, NO_SUBSTRATE_DIAGNOSTIC, SUBSTRATE_AGENT_MESSAGE_CHARS,
    SUBSTRATE_FAILURE_CONSEQUENCE,
};
pub(crate) use failure::{
    check_result_from_cell_outcome, host_ended_compile, CheckExecutionFailure, HostEnding,
    SubstrateFailure, SubstrateFailureShape,
};
#[cfg(test)]
#[cfg(not(test))]
pub(crate) use manual::ManualCaptureEvidence;
#[cfg(test)]
use manual::{
    base_relative_transport, base_relative_transport_for, load_manual_run_row,
    manual_check_batch_item, manual_check_no_verdict, manual_configured_check_result,
    manual_progress_update, require_snapshot_command, resolve_base_relative_change,
    thread_session_coordinate_refusal,
};
pub(crate) use manual::{
    classify_manual_check_revision, execute_adopted_check_item, execute_check_contract,
    immutable_read_jj, recover_manual_check_result, resolve_check_contract_at_commit,
    run_manual_configured_check_with_progress, CheckContractSnapshot, CheckRunProducer,
    CheckRunTarget, ManualCheckError, ManualCheckRequest, ManualCheckRevision,
    NO_VERDICT_SUPPRESSED,
};
pub use manual::{
    manual_check_cache_context, CheckCapture, CheckMachine, ManualCheckCacheContext,
    ManualCheckNoVerdict, ManualCheckProgressUpdate, ManualConfiguredCheckResult,
};
use manual::{uncached_retry_command, UNCACHED_RETRY_PREFIX};
#[cfg(test)]
pub(crate) use manual::{AdoptedAttempt, BaseRelativeChange, NO_VERDICT_LEFT_RUNNING};
#[cfg(test)]
use planning::{
    attribute_capacity_wait, capacity_retry_decision, describe_capacity_waits,
    key_review_tree_plans, merge_batch_avoid, merge_batch_executor, record_cancelled_check,
    review_tree_gate_result, run_planned_checks_at_commit, run_planned_checks_at_commit_with_env,
    CapacityRetry, SLOT_CHECK_DEV_DEBUG_ENV,
};
pub(crate) use planning::{
    cancel_stale_review_on_branch_advance, cancel_stale_review_on_branch_advance_except,
    execute_job_verdict, head_infra_scopes, jobs_sharing_branch, plan_infra_scopes,
    record_suppressed_check, review_placement_occupancy, run_planned_checks_selected,
    run_planned_checks_with_board, submit_check_lease, submit_planned_check, verify_review_tree,
    BranchAdvance, CheckPlacementQuestion, CheckRunCommit, InfraScopeInputs, ReviewTreeGateResult,
};
#[cfg(test)]
pub(crate) use planning::{
    complete_each_member_once, refuse_unplaceable_items, reserve_batch_items, run_planned_checks,
    run_planned_checks_at_commit_with_coordinates,
};
use planning::{
    effective_verdict_environment_hash, holds_no_verdict, needs_execution, sealed_coordinates,
    slot_check_env, unmet_placement, GroupWait,
};
use render::{
    check_dispatch, clock_utc, describe_ms, live_node_base, narrate_dispatch, summary_annotation,
    tail, write_check_attributions,
};
pub(crate) use render::{
    describe_decline, describe_queued_hold, format_check_summary, lane_breaker_sentence,
    suppressed_check_message,
};
#[cfg(test)]
use render::{describe_placement_pass, DispatchNarrator};
#[cfg(feature = "test-utils")]
pub use write_wave::expire_write_check_wait_for_test;
#[cfg(test)]
use write_wave::{
    await_write_check_wave, claim_offered_outcome, classify_expired_wait,
    fix_is_attributed_to_declared_fixers, fixer_first_submission_order,
    fixers_superseded_by_a_later_fixer, format_answered_elsewhere, format_fixed_batch_summary,
    hand_off_wave_outcome, lane_key, lanes_left_to_run, late_write_check_message,
    pending_wave_reason, pending_write_checks_summary, plan_write_wave, replan_onto_fix,
    run_write_checks_after_seal_inner, split_write_wave, verdict_survives_fix, FixReplanning,
    InlineWaitBounds, InlineWaitOutcome, OfferedOutcome, PendingWave, WriteChecksInFlightGuard,
    WriteWavePlanning, WriteWaveSplit, WRITE_CHECK_INLINE_WAIT,
};
pub(crate) use write_wave::{run_write_checks_after_seal, WriteCheckTarget};
use write_wave::{split_write_check_batch_outcome, stamp_substrate_elapsed};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CheckExecMode {
    Isolated,
    Shared,
}

fn resolve_full_tree_plans(
    plans: Vec<CheckPlan>,
    checks: &HashMap<String, CheckCommand>,
) -> Vec<CheckPlan> {
    plans
        .into_iter()
        .map(|mut plan| {
            let configured = checks
                .get(&plan.name)
                .expect("planned check must retain its configured definition");
            match whole_suite_command(configured) {
                Ok(command) => {
                    plan.command = command;
                    plan.scope = CheckScope::Full;
                }
                Err(error) => {
                    plan.config_error = Some(format!(
                        "check {:?} cannot resolve a whole-suite command: {error}",
                        plan.name
                    ));
                }
            }
            plan
        })
        .collect()
}

/// The plan a manual run of `check_name` executes at a sealed tree, and the key
/// its verdict is looked up, reused, and recorded under.
///
/// A manual run evaluates a sealed tree, not a change set, so it runs the
/// check's WHOLE suite and keys the verdict by the command it executed — the
/// same rule main-health applies (see [`key_review_tree_plans`]). A narrower,
/// diff-derived selection cannot be keyed honestly here: the key is the tree's
/// input content, which names no base, so a partial verdict recorded under it
/// would later answer for the same content measured against a different base.
/// And a `--branch main` baseline diffs main against itself, which selects
/// nothing. For a placeholder-free check the executed command is the configured
/// one, so its key equals [`check_input_hash`]; for a selector check it differs,
/// which keeps a turn-end partial verdict from answering a whole-suite question.
fn manual_check_plan(
    checks: &HashMap<String, CheckCommand>,
    inputs: &ResolvedInputs,
    entries: Option<&[(String, String)]>,
    tree_hash: &str,
    check_name: &str,
    repo_root: &Path,
    basis: ChangeBasis<'_>,
) -> Result<(CheckPlan, String), String> {
    let check = checks
        .get(check_name)
        .ok_or_else(|| format!("configured check {check_name:?} was not found"))?;
    let one = HashMap::from([(check_name.to_string(), check.clone())]);
    let plan = resolve_full_tree_plans(plan_checks(&one, inputs, &[], repo_root), &one)
        .pop()
        .ok_or_else(|| format!("configured check {check_name:?} could not be planned"))?;
    if let Some(error) = plan.config_error.as_deref() {
        return Err(format!(
            "configured check {check_name:?} is unrunnable: {error}"
        ));
    }
    let key = check_result_key_for_executed_command(
        check,
        &plan.command,
        inputs.for_check(check_name),
        entries,
        tree_hash,
        &check_platform_identity(),
        check_toolchain_identity(),
        CheckChange {
            name: check_name,
            basis,
        },
    );
    Ok((plan, key))
}

/// The line a producer script wrote to say how its process failed
/// (`nonDiagnosticFailureMessage` in scripts/lib/rust-process-outcome.ts):
/// `<MARKER>: <tool> exited <code> on <host>` at the start of a line, then
/// `Cause (verbatim):`. A marker quoted anywhere else (a test named after it,
/// a fixture echoed to stdout) is evidence about nothing.
///
/// A prerequisite step exits with the code its line states
/// (`prerequisiteFailureReport`), so its marker also needs the run's exit code
/// to be that code, as `CAIRN_INFRASTRUCTURE_EXIT` needs exit 75: a quoted
/// prerequisite failure must never make a red unreusable. The other markers
/// state the inner tool's code, which the script may exit past (a targeted
/// selection that matched nothing exits its own code), so only their shape
/// binds them.
fn producer_failure_marker(
    lines: &[&str],
    exit_code: Option<i32>,
) -> Option<(usize, CheckFailureKind)> {
    lines.windows(2).enumerate().find_map(|(index, pair)| {
        if pair[1] != "Cause (verbatim):" {
            return None;
        }
        let (prefix, kind) = [
            ("CAIRN_ENVIRONMENT_EXIT: ", CheckFailureKind::Environment),
            ("CAIRN_UNCLASSIFIED_EXIT: ", CheckFailureKind::Unclassified),
            ("CAIRN_SELECTION_EXIT: ", CheckFailureKind::Selection),
            ("CAIRN_PREREQUISITE_EXIT: ", CheckFailureKind::Prerequisite),
        ]
        .into_iter()
        .find(|(prefix, _)| pair[0].starts_with(prefix))?;
        let (_, after_exited) = pair[0][prefix.len()..].split_once(" exited ")?;
        let (stated, _host) = after_exited.split_once(" on ")?;
        if kind == CheckFailureKind::Prerequisite && stated.parse::<i32>().ok() != exit_code {
            return None;
        }
        Some((index, kind))
    })
}

fn attach_executor_provenance(
    classification: &mut Option<FailureClassification>,
    executor_id: Option<&str>,
) {
    let Some(classification) = classification.as_mut() else {
        return;
    };
    // Every kind that is a fact about the machine rather than the tree names
    // the machine, so the reader knows where to look.
    if matches!(
        classification.kind,
        CheckFailureKind::Infrastructure
            | CheckFailureKind::Environment
            | CheckFailureKind::Unclassified
            | CheckFailureKind::Selection
            | CheckFailureKind::Prerequisite
    ) {
        if let Some(executor_id) = executor_id.filter(|id| !id.is_empty()) {
            classification
                .reason
                .push_str(&format!("{EXECUTOR_PROVENANCE} `{executor_id}`"));
        }
    }
}

/// What [`attach_executor_provenance`] appends before the machine's name.
const EXECUTOR_PROVENANCE: &str = " on executor";

/// A reason without the machine [`attach_executor_provenance`] named. The lane
/// breaker signs how a run failed, and the same deterministic failure on two
/// machines must sign the same, or it never reaches the breaker's bound.
fn without_executor_provenance(reason: &str) -> &str {
    reason
        .rsplit_once(EXECUTOR_PROVENANCE)
        .filter(|(_, machine)| {
            machine
                .strip_prefix(" `")
                .and_then(|rest| rest.strip_suffix('`'))
                .is_some_and(|name| !name.contains('`'))
        })
        .map_or(reason, |(reason, _)| reason)
}

/// The base-relative inputs a check script needs, carried in the request itself.
///
/// Every cadence ships these rather than leaving a script to ask the checkout for
/// its integration base: the cell a check lands on may be a remote detached
/// checkout with no base ref at all, and a script that resolves its base from git
/// can then only answer for the colocated cell. The fleet turns each `*_CONTENT`
/// entry into a file named by the script-facing variable
/// (`Fleet::check_process_batch`), which `scripts/lib/check-base.ts` reads.
///
/// The changed-file entry is present even when the diff is empty. The runner
/// computed that diff, and an empty one is an answer, not an absence: omitting it
/// would send the script back to the checkout's refs, which is the gap this
/// transport exists to close.
pub(crate) fn check_transport_env(
    changed: &[GraphFileChange],
    base_content: Option<String>,
) -> Vec<(String, String)> {
    let changed_body = transport_paths(changed)
        .into_iter()
        .collect::<Vec<_>>()
        .join("\n");
    let mut env = vec![(CHANGED_FILES_CONTENT_ENV.to_string(), changed_body)];
    if let Some(base_content) = base_content {
        env.push((BASE_CONTENT_CONTENT_ENV.to_string(), base_content));
    }
    env
}

/// Every path a change set names, sorted and unique: what the changed-file
/// transport carries, and what a base-relative key folds ([`change_identity`]).
///
/// A rename names its destination AND its source. A check judging what the
/// change added needs the new path, and one judging what it removed needs the
/// old: the diff from a fork point reports a rename only when the source
/// existed at the base, so that source is a file the change deleted.
pub(crate) fn transport_paths(changed: &[GraphFileChange]) -> BTreeSet<&str> {
    changed
        .iter()
        .flat_map(|change| {
            std::iter::once(change.path.as_str()).chain(change.previous_path.as_deref())
        })
        .filter(|path| !path.is_empty())
        .collect()
}

/// Every `*_CONTENT` transport a check batch may carry, paired with the
/// script-facing variable naming the file the fleet writes its content to.
/// The fleet and the impact slice both read this one list, so a transport added
/// here reaches the script whichever way the lane runs.
pub(crate) const CHECK_CONTENT_TRANSPORTS: [(&str, &str); 3] = [
    (CHANGED_FILES_CONTENT_ENV, "CAIRN_CHECK_CHANGED_FILES"),
    (BASE_CONTENT_CONTENT_ENV, "CAIRN_CHECK_BASE_CONTENT"),
    (
        crate::execution::lint_baseline::LINT_BASELINE_CONTENT_ENV,
        crate::execution::lint_baseline::LINT_BASELINE_ENV,
    ),
];

/// Whether a check compares the executor protocol against its base, and so needs
/// the base-content bundle in its transport.
fn needs_executor_base_content(check_name: &str) -> bool {
    check_name == EXECUTOR_PROTOCOL_CHECK
}

/// The environment of each process in a write-check batch: the build slot's own
/// environment plus the change transport.
///
/// The transport belongs to the processes alone. Every lane that runs a check
/// reads it from the item (the fleet spills it into batch files, the impact
/// slice into scratch files), and the cell request's environment is launch
/// context the executor never hands a batch process. A second copy on the
/// request is megabytes of executor base content carried for nothing, and on a
/// lane that overlays the two it is a second copy to spill.
fn write_check_transport_env(
    cell_env: &[(String, String)],
    transport: Vec<(String, String)>,
) -> Vec<(String, String)> {
    let mut env = cell_env.to_vec();
    env.extend(transport);
    env
}

fn write_check_planning_infrastructure_failure(error: &str) -> String {
    format!(
        "Write-check planning infrastructure failure: could not build authoritative executor base-content bundle: {error}"
    )
}

#[derive(Debug)]
pub(crate) enum CommitChecksContractError {
    Missing,
    NoChecks,
    Read(String),
    InvalidUtf8(String),
    Parse(String),
    BlockingTask(String),
}

impl CommitChecksContractError {
    pub(crate) fn manual_diagnostic(&self, commit: &str) -> String {
        match self {
            Self::Missing | Self::NoChecks => {
                format!("commit {commit} declares no configured checks")
            }
            Self::Read(error) => {
                format!("failed to read {CHECKS_CONFIG_PATH} at commit {commit}: {error}")
            }
            Self::InvalidUtf8(error) => {
                format!("{CHECKS_CONFIG_PATH} at commit {commit} is not valid UTF-8: {error}")
            }
            Self::Parse(error) => {
                format!("{CHECKS_CONFIG_PATH} at commit {commit} failed to parse: {error}")
            }
            Self::BlockingTask(error) => {
                format!("failed to load {CHECKS_CONFIG_PATH} at commit {commit}: {error}")
            }
        }
    }
}

#[cfg(test)]
#[test]
fn base_bundle_serializes_absent_wanted_paths_as_null() {
    let wanted = BTreeSet::from([
        EXECUTOR_PROTOCOL_SOURCE.to_string(),
        "src-tauri/cairn-exec/src/new_wire.rs".to_string(),
    ]);
    let content: BTreeMap<String, Option<String>> =
        wanted.iter().map(|path| (path.clone(), None)).collect();
    let serialized = serde_json::to_value(content).unwrap();
    assert_eq!(
        serialized[EXECUTOR_PROTOCOL_SOURCE],
        serde_json::Value::Null
    );
    assert_eq!(
        serialized["src-tauri/cairn-exec/src/new_wire.rs"],
        serde_json::Value::Null
    );
}

#[cfg(test)]
#[test]
fn write_transport_carries_changed_paths_and_base_content_together() {
    let env = write_check_transport_env(
        &[],
        check_transport_env(
            &[GraphFileChange {
                path: "src-tauri/cairn-exec/src/new_wire.rs".to_string(),
                previous_path: None,
                status: "added".to_string(),
                additions: 1,
                deletions: 0,
            }],
            Some("{\"src-tauri/cairn-exec/src/new_wire.rs\":null}".to_string()),
        ),
    );
    assert!(env
        .iter()
        .any(|(key, value)| key == CHANGED_FILES_CONTENT_ENV
            && value == "src-tauri/cairn-exec/src/new_wire.rs"));
    assert!(env
        .iter()
        .any(|(key, value)| key == BASE_CONTENT_CONTENT_ENV
            && value == "{\"src-tauri/cairn-exec/src/new_wire.rs\":null}"));
}

#[cfg(test)]
#[test]
fn base_bundle_failure_is_surfaced_as_planning_infrastructure() {
    let result = write_check_planning_infrastructure_failure("jj failed");
    assert!(result.contains("planning infrastructure failure"));
    assert!(result.contains("jj failed"));
}

#[cfg(test)]
#[test]
fn queued_phase_label_never_carries_its_blocker_list() {
    let diagnostic = "Queued behind dead-code · frontend-build · rust-lint.".to_string();
    let status = check_phase_status(
        Some("queued"),
        Some("queued behind dead-code · frontend-build · rust-lint"),
        Some(diagnostic.clone()),
    );

    assert_eq!(status.label.as_deref(), Some("queued"));
    assert_eq!(status.diagnostic.as_deref(), Some(diagnostic.as_str()));
    let label = status.label.unwrap();
    assert!(!label.contains('·') && !label.to_ascii_lowercase().contains("behind"));
}

#[cfg(test)]
mod capture_boundary_tests;

#[cfg(test)]
mod unmet_placement_tests;

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

/// The `refs/cairn/checks/*` refs a check batch in this process still needs.
///
/// A check request's id is minted per batch and never persisted, so the
/// database cannot say whether a check ref is still in use. This set can: a ref
/// enters it when its batch publishes it and leaves when the batch retires it
/// or gives up retiring it. The ref-store sweep retires only refs outside it.
static HELD_CHECK_REFS: std::sync::LazyLock<std::sync::Mutex<std::collections::HashSet<String>>> =
    std::sync::LazyLock::new(Default::default);

pub(crate) fn hold_check_ref(reference: &str) {
    HELD_CHECK_REFS
        .lock()
        .unwrap()
        .insert(reference.to_string());
}

fn release_check_ref(reference: &str) {
    HELD_CHECK_REFS.lock().unwrap().remove(reference);
}

/// Whether a check batch in this process still holds `reference`.
pub(crate) fn check_ref_is_held(reference: &str) -> bool {
    HELD_CHECK_REFS.lock().unwrap().contains(reference)
}

impl TemporaryCheckRef {
    /// Hand the ref to the retirement sweeper. It stays held, and so out of
    /// the ref-store sweep's reach, until it is actually deleted or the
    /// sweeper gives up on it.
    fn retire(&mut self) {
        if !self.armed {
            return;
        }
        self.armed = false;
        retire_check_ref(
            &self.orch,
            &self.store_dir,
            CheckRefRetirement {
                repository: self.repository.clone(),
                reference: self.reference.clone(),
                commit: self.commit.clone(),
                attempts: 0,
            },
        );
    }
}

impl Drop for TemporaryCheckRef {
    fn drop(&mut self) {
        self.retire();
    }
}

/// One released check ref waiting to be deleted.
#[derive(Debug, Clone)]
struct CheckRefRetirement {
    repository: std::path::PathBuf,
    reference: String,
    commit: String,
    attempts: u32,
}

/// Released check refs per project store, and the sweep that will delete them.
#[derive(Default)]
struct PendingRetirements {
    refs: Vec<CheckRefRetirement>,
    /// Alive exactly as long as the store's sweep is (see [`CheckRefSweepLease`]).
    /// Dead means no sweep will see `refs`, so the next release starts one.
    sweep: std::sync::Weak<()>,
}

static RETIRING_CHECK_REFS: std::sync::LazyLock<
    std::sync::Mutex<HashMap<std::path::PathBuf, PendingRetirements>>,
> = std::sync::LazyLock::new(Default::default);

/// The pending-retirement table, whoever panicked while holding it: the table
/// is valid between every statement that touches it, and the sweep's lease
/// must reach it while unwinding.
fn retiring_check_refs(
) -> std::sync::MutexGuard<'static, HashMap<std::path::PathBuf, PendingRetirements>> {
    RETIRING_CHECK_REFS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// A store's one live sweep of released check refs.
///
/// Made before the sweep is spawned and moved into it, so it is dropped with
/// the sweep however the sweep ends: finished, panicked, or dropped unpolled or
/// mid-await because the runtime it ran on went away. Dropping it gives every
/// ref the sweep took but did not settle back to the store's pending list and
/// marks the store as having no sweep, so the next release starts one. Without
/// it a dropped sweep left its store marked as swept for the life of the
/// process: every later release queued behind a sweep that would never come,
/// each ref still held and so out of the ref-store sweep's reach as well.
struct CheckRefSweepLease {
    store_dir: std::path::PathBuf,
    alive: Arc<()>,
    /// Taken from pending and neither deleted nor given up on yet.
    in_flight: Vec<CheckRefRetirement>,
}

impl Drop for CheckRefSweepLease {
    fn drop(&mut self) {
        let unfinished = std::mem::take(&mut self.in_flight);
        let mut all = retiring_check_refs();
        let Some(pending) = all.get_mut(&self.store_dir) else {
            // A sweep that ran out of work removed the entry itself.
            return;
        };
        if !unfinished.is_empty() {
            log::warn!(
                "the sweep of released check refs in {} ended before settling {} of them; \
                 they are queued again for the next sweep",
                self.store_dir.display(),
                unfinished.len()
            );
            pending.refs.extend(unfinished);
        }
        if std::ptr::eq(pending.sweep.as_ptr(), Arc::as_ptr(&self.alive)) {
            pending.sweep = std::sync::Weak::new();
        }
    }
}

/// How long released refs accumulate before one deletion retires them all.
/// Every deletion takes `packed-refs.lock`, which jj exports inside the store
/// lock contend for, so a burst of releases is better paid for once.
#[cfg(not(test))]
const CHECK_REF_RETIRE_DEBOUNCE: std::time::Duration = std::time::Duration::from_secs(3);
#[cfg(test)]
const CHECK_REF_RETIRE_DEBOUNCE: std::time::Duration = std::time::Duration::from_millis(150);

/// Rounds a ref is retried before it is left for the ref-store sweep. Each
/// round is already patient with `packed-refs.lock`, so a ref that fails five
/// has met something waiting cannot fix.
const CHECK_REF_RETIRE_ATTEMPTS: u32 = 5;

#[cfg(test)]
static CHECK_REF_RETIRE_TRANSACTIONS: std::sync::LazyLock<
    std::sync::Mutex<HashMap<std::path::PathBuf, usize>>,
> = std::sync::LazyLock::new(Default::default);

fn retire_check_ref(orch: &Orchestrator, store_dir: &Path, retirement: CheckRefRetirement) {
    if let Some(sweep) = queue_check_ref_retirement(orch, store_dir, retirement) {
        // On the orchestrator's runtime, never the caller's. A ref is often
        // released from a destructor while the task holding it is dropped, and
        // the caller's runtime may be the very one going away.
        orch.spawn_detached(sweep);
    }
}

/// Queue `retirement` for deletion, returning the sweep to start when the store
/// has no live one.
fn queue_check_ref_retirement(
    orch: &Orchestrator,
    store_dir: &Path,
    retirement: CheckRefRetirement,
) -> Option<impl std::future::Future<Output = ()> + Send + 'static> {
    let lease = {
        let mut all = retiring_check_refs();
        let pending = all.entry(store_dir.to_path_buf()).or_default();
        pending.refs.push(retirement);
        if pending.sweep.strong_count() > 0 {
            return None;
        }
        let alive = Arc::new(());
        pending.sweep = Arc::downgrade(&alive);
        CheckRefSweepLease {
            store_dir: store_dir.to_path_buf(),
            alive,
            in_flight: Vec::new(),
        }
    };
    Some(sweep_retiring_check_refs(orch.clone(), lease))
}

/// Delete the store's released check refs in batches until none are left.
///
/// Not a store-lock operation in substance — jj never sees `refs/cairn/checks/*`
/// — but deleting takes `packed-refs.lock`, and taking it under one Background
/// acquisition keeps the deletion from contending with the jj exports that run
/// inside the lock, while letting any foreground work go first.
async fn sweep_retiring_check_refs(orch: Orchestrator, mut lease: CheckRefSweepLease) {
    let store_dir = lease.store_dir.clone();
    loop {
        tokio::time::sleep(CHECK_REF_RETIRE_DEBOUNCE).await;
        {
            let mut all = retiring_check_refs();
            let pending = all.entry(store_dir.clone()).or_default();
            if pending.refs.is_empty() {
                all.remove(&store_dir);
                return;
            }
            lease.in_flight = std::mem::take(&mut pending.refs);
        }
        let guard = orch
            .acquire_jj_store_lock(
                &store_dir,
                crate::orchestrator::StoreLockClass::Background,
                "retire sealed check commit references",
            )
            .await;
        let mut failed = Vec::new();
        let mut by_repository: HashMap<std::path::PathBuf, Vec<CheckRefRetirement>> =
            HashMap::new();
        for retirement in &lease.in_flight {
            by_repository
                .entry(retirement.repository.clone())
                .or_default()
                .push(retirement.clone());
        }
        for (repository, refs) in by_repository {
            let _phase = guard.phase(format!("delete {} check refs", refs.len()));
            let failures = delete_check_refs(&repository, refs).await;
            // What this repository's deletion settled leaves the lease now; the
            // failures stay in it until they are queued again below.
            lease.in_flight.retain(|retirement| {
                retirement.repository != repository
                    || failures
                        .iter()
                        .any(|(failure, _)| failure.reference == retirement.reference)
            });
            failed.extend(failures);
        }
        drop(guard);
        #[cfg(test)]
        {
            *CHECK_REF_RETIRE_TRANSACTIONS
                .lock()
                .unwrap()
                .entry(store_dir.clone())
                .or_default() += 1;
        }
        let mut retry = Vec::new();
        for (mut retirement, error) in failed {
            retirement.attempts += 1;
            if retirement.attempts < CHECK_REF_RETIRE_ATTEMPTS {
                log::warn!(
                    "sealed check ref retirement attempt {}/{CHECK_REF_RETIRE_ATTEMPTS} failed: \
                     reference={}, commit={}, error={error}",
                    retirement.attempts,
                    retirement.reference,
                    retirement.commit
                );
                retry.push(retirement);
            } else {
                // Released rather than leaked: once no batch holds the ref, the
                // ref-store maintenance sweep (`vcs_maintenance`) retires it on
                // its next prune of this store.
                release_check_ref(&retirement.reference);
                log::warn!(
                    "sealed check ref {} was not removed after {CHECK_REF_RETIRE_ATTEMPTS} \
                     attempts and still pins commit {}; it is left for the ref-store \
                     maintenance sweep, which retires check refs no batch holds: {error}",
                    retirement.reference,
                    retirement.commit
                );
            }
        }
        // Queue the retries and let go of the batch in one step, so no instant
        // has a ref in neither place.
        let mut all = retiring_check_refs();
        all.entry(store_dir.clone()).or_default().refs.extend(retry);
        lease.in_flight.clear();
    }
}

/// Delete `refs` from `repository` in one ref transaction, each against the
/// commit it was published at. One stale entry fails the whole transaction,
/// so a failed batch falls back to deleting each ref alone. Returns the refs
/// that could not be deleted, with why.
async fn delete_check_refs(
    repository: &Path,
    refs: Vec<CheckRefRetirement>,
) -> Vec<(CheckRefRetirement, String)> {
    let input: String = refs
        .iter()
        .map(|retirement| format!("delete {} {}\n", retirement.reference, retirement.commit))
        .collect();
    let batched = git_check_ref_write(
        repository,
        &["update-ref", "--stdin"],
        Some(&input),
        "retire sealed check commit references",
    )
    .await;
    let refs = match batched {
        Ok(_) => {
            for retirement in &refs {
                release_check_ref(&retirement.reference);
            }
            return Vec::new();
        }
        Err(_) if refs.len() > 1 => refs,
        Err(error) => return refs.into_iter().map(|r| (r, error.clone())).collect(),
    };
    let mut failed = Vec::new();
    for retirement in refs {
        match git_check_ref_write(
            repository,
            &[
                "update-ref",
                "-d",
                &retirement.reference,
                &retirement.commit,
            ],
            None,
            "delete sealed check commit reference",
        )
        .await
        {
            Ok(_) => release_check_ref(&retirement.reference),
            Err(error) => failed.push((retirement, error)),
        }
    }
    failed
}

/// Write a ref into the check repository, patiently.
///
/// Every ref transaction against a project's shared Git repository contends for
/// one `packed-refs.lock`. This pair — publishing a sealed check commit's
/// reference and retiring it — is the leg whose lost race discards verdicts that
/// were already computed, so it waits that contention out and heals a lock it
/// can prove abandoned. The ceremony lives in `jj/packed_refs.rs`.
///
/// Read-only plumbing (`rev-parse`, `cat-file -e`) deliberately keeps calling
/// [`git_check_output`] directly: it never takes the lock, so patience there
/// would only delay reporting a genuine failure.
///
/// Callers here already hold the managed store lease, acquired with a 600s
/// timeout. Spending up to 30s of that lease waiting for `packed-refs.lock` is
/// deliberate: the lease bounds how long one check batch may own the store, the
/// patience bounds how long a single ref write inside it waits, and the lease is
/// twenty times the wait. A ref write that waits can never outlive its lease.
async fn git_check_ref_write(
    repository: &Path,
    args: &[&str],
    input: Option<&str>,
    context: &str,
) -> Result<String, String> {
    crate::jj::with_packed_refs_patience_async(
        crate::jj::PackedRefs::Checkout(repository),
        context,
        || git_check_run(repository, args, input, context),
    )
    .await
}

fn git_check_output(repository: &Path, args: &[&str], context: &str) -> Result<String, String> {
    git_check_run(repository, args, None, context)
}

fn git_check_run(
    repository: &Path,
    args: &[&str],
    input: Option<&str>,
    context: &str,
) -> Result<String, String> {
    use std::io::Write as _;
    let mut child = std::process::Command::new("git")
        .args(args)
        .current_dir(repository)
        .stdin(if input.is_some() {
            std::process::Stdio::piped()
        } else {
            std::process::Stdio::null()
        })
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|error| format!("{context}: {error}"))?;
    if let (Some(input), Some(mut stdin)) = (input, child.stdin.take()) {
        stdin
            .write_all(input.as_bytes())
            .map_err(|error| format!("{context}: write git input: {error}"))?;
    }
    let output = child
        .wait_with_output()
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
    // No store lock: jj never imports `refs/cairn/checks/*`, creating a loose
    // ref takes no `packed-refs.lock`, and the ref-store sweep and git gc both
    // already run outside the store lock. The create is an old-value compare,
    // so it cannot clobber anything.
    git_check_output(
        repository,
        &["cat-file", "-e", &format!("{commit}^{{commit}}")],
        "verify sealed check commit in the managed Git backend",
    )?;
    let reference = format!("refs/cairn/checks/{request_id}");
    let absent = "0".repeat(commit.len());
    git_check_ref_write(
        repository,
        &["update-ref", &reference, commit, &absent],
        None,
        "publish sealed check commit reference",
    )
    .await?;
    // Arm the cleanup obligation immediately after publication. Every later
    // path, including Drop, hands the ref to the retirement sweeper.
    hold_check_ref(&reference);
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
            temporary_ref.retire();
            return Err(error);
        }
    };
    if resolved != commit {
        temporary_ref.retire();
        return Err(format!(
            "sealed check commit reference resolved to {resolved}, expected {commit}"
        ));
    }
    Ok(temporary_ref)
}

#[derive(Clone, Copy, Debug, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CheckAdoptionResultShape {
    #[default]
    Batch,
    Single,
}

/// The immutable recording contract of an item actually granted to a machine.
/// Recording a retained partial run must not replan it as a whole-suite run.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct CheckAdoptionRecordingPlan {
    #[serde(default)]
    pub(crate) result_shape: CheckAdoptionResultShape,
    pub(crate) plan: CheckPlan,
    pub(crate) evaluated_commit: String,
    pub(crate) defined_by_commit: String,
    pub(crate) tree_hash: String,
    pub(crate) input_hash: String,
    pub(crate) job_id: String,
    pub(crate) tool_use_id: String,
    // Process environments may contain credentials. A retained result carries
    // its own verdict-environment hash, so raw values never enter the journal.
    #[serde(skip)]
    pub(crate) effective_env: Vec<(String, String)>,
}

impl CheckAdoptionRecordingPlan {
    pub(crate) fn new(
        plan: &CheckPlan,
        coordinate: &crate::execution::cache::SealedCheckCoordinate,
        job_id: &str,
        tool_use_id: &str,
        effective_env: &[(String, String)],
    ) -> Self {
        Self {
            result_shape: CheckAdoptionResultShape::Batch,
            plan: plan.clone(),
            evaluated_commit: coordinate.evaluated_commit().to_string(),
            defined_by_commit: coordinate.defined_by_commit().to_string(),
            tree_hash: coordinate.tree_hash().to_string(),
            input_hash: coordinate.input_hash().to_string(),
            job_id: job_id.to_string(),
            tool_use_id: tool_use_id.to_string(),
            effective_env: effective_env.to_vec(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct PlannedCheckBatchItem {
    pub recording_plan: Option<CheckAdoptionRecordingPlan>,
    pub index: usize,
    pub name: String,
    pub input_hash: String,
    pub resource_identity: CheckResourceIdentityInput,
    pub command: String,
    pub stream_id: String,
    pub env: Vec<(String, String)>,
    pub verdict_environment_names: Vec<String>,
    pub timeout_ms: u64,
    pub executor: Option<ExecutorSelector>,
    /// The platforms whose answer counts as this check's verdict, resolved by
    /// [`crate::execution::check_identity::verdict_platforms`]. Carried onto the
    /// submission so placement can only choose a machine the project gates on.
    pub verdict_platforms: Vec<String>,
    /// Legacy project metadata retained for configuration compatibility. CPU
    /// demand does not participate in admission; the learned memory profile does.
    pub resource_class: CheckResourceClass,
    /// Every suppression counter this item is live against, resolved by
    /// [`plan_infra_scopes`] alongside the array the result path is given.
    /// Carried on the item so the reservation that refuses it and the row a
    /// returning failure writes can only ever name the same counters.
    pub infra_scopes: InfraScopes,
    /// The machines this item must not land on: any its caller rules out (a
    /// publication leg steering away from machines of the wrong architecture),
    /// plus those whose environment counter for it is spent, which only
    /// [`reserve_batch_items`] knows because only the reservation reads the
    /// counters.
    pub avoid_executor_ids: Vec<String>,
}

pub(crate) type CheckGroupResults = HashMap<usize, Result<CheckExecResult, CheckExecutionFailure>>;
/// The members a completion failed to record durably. A completion wrapped by
/// `complete_each_member_once` offers them again with the next call that
/// carries them, rather than counting them answered.
pub(crate) type CheckGroupUnrecorded = std::collections::HashSet<usize>;
pub(crate) type CheckGroupCompletion = Arc<
    dyn for<'a> Fn(
            &'a CheckGroupResults,
        ) -> futures_util::future::BoxFuture<'a, CheckGroupUnrecorded>
        + Send
        + Sync,
>;

/// How a member's `not_recorded` reason begins when its observation row could
/// not be written: a failure to persist, not a decision about the verdict.
pub(crate) const OBSERVATION_WRITE_FAILED: &str = "the observation write failed: ";

/// One check suite, on one revision, asked of the fleet for a verdict.
///
/// A verdict request carries exactly one suite, so it is placed, priced and
/// recorded by itself: no caller can compose two suites into one cell, and a
/// suite's verdict lands when its own request settles rather than with a
/// slower sibling's. Callers that want several verdicts submit several
/// requests concurrently.
#[derive(Clone)]
pub(crate) struct PlannedCheckRequest {
    pub project_id: String,
    pub repository: String,
    pub store_dir: std::path::PathBuf,
    pub sealed_commit: String,
    pub requesting_job_id: String,
    pub owner: cairn_common::executor_protocol::CellOwnerRef,
    /// Which idle cell the executor prefers. A job's check requests key it by
    /// suite ([`check_affinity_key`]), so each suite returns to the cell
    /// holding its own previous build.
    pub affinity_key: Option<String>,
    pub priority: CellPriority,
    pub env: Vec<(String, String)>,
    pub item: PlannedCheckBatchItem,
    pub run_context: Option<RunContext>,
    pub status_board: Option<CheckStatusBoard>,
    /// Awaited with the request's result before it is returned.
    pub on_complete: Option<CheckGroupCompletion>,
    /// See [`PlannedCheckBatchRequest::purpose`].
    pub purpose: Option<crate::fleet::purpose::PurposeKey>,
    /// See [`PlannedCheckBatchRequest::publication`].
    pub publication: Option<crate::fleet::PublicationCustody>,
}

impl PlannedCheckRequest {
    fn into_batch(self) -> PlannedCheckBatchRequest {
        PlannedCheckBatchRequest {
            project_id: self.project_id,
            repository: self.repository,
            store_dir: self.store_dir,
            sealed_commit: self.sealed_commit,
            requesting_job_id: self.requesting_job_id,
            owner: self.owner,
            affinity_key: self.affinity_key,
            priority: self.priority,
            env: self.env,
            items: vec![self.item],
            run_context: self.run_context,
            mutation_policy: MutationPolicy::PureVerdict,
            status_board: self.status_board,
            on_group_complete: self.on_complete,
            purpose: self.purpose,
            publication: self.publication,
        }
    }
}

/// The cell-affinity key a job's request for one suite carries: the job and
/// the suite, so concurrent suites of one job never contend for one warm cell
/// and each comes back to the target directory its own last run left.
pub(crate) fn check_affinity_key(job_id: &str, suite: &str) -> String {
    format!("{job_id}:{suite}")
}

/// The carrier every submission is planned through. Built directly only for
/// a change-returning lease ([`MutationPolicy::AllowDelta`]), whose items run
/// in order in one cell because a later command must see what an earlier one
/// wrote. A verdict is asked for with a [`PlannedCheckRequest`].
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
    /// Awaited with the batch's results before they are returned.
    pub on_group_complete: Option<CheckGroupCompletion>,
    /// The question this batch answers, when it answers a standing one.
    ///
    /// Present for work whose whole point is to hold one answer current —
    /// default-branch attestation above all, where each new tip is a newer
    /// answer to the same question and must replace its predecessor rather than
    /// run beside it. Absent for one-off work, which asks its question once and
    /// is in competition with nothing.
    pub purpose: Option<crate::fleet::purpose::PurposeKey>,
    /// Set for a publication leg: placement offers it by role, and only to a
    /// measured publisher when it carries the release secret.
    pub publication: Option<crate::fleet::PublicationCustody>,
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

use crate::execution::check_parsers::{
    extract_running_tests, format_failure_excerpt, format_failure_names, is_lint_parser,
    parse_check_output, reported_test, stream_tail, ParsedCheckResult, ReportedTest, ESLINT_PARSER,
    MAX_FAILURE_NAMES, RESULT_ARTIFACT_JSON_MARKER, RUST_LINT_PARSER,
};
use crate::execution::lane_claims::{ClaimedElsewhere, LaneKey, WaveClaims};
use crate::execution::selection::{plan_checks, whole_suite_command, CheckPlan, CheckScope};
use crate::jj::{
    logical_changed_files, logical_tree_hash, read_blobs, tree_entries, GraphFileChange, JjEnv,
};
use crate::mcp::handlers::run::{CheckExecResult, CheckStatusEntry, CheckStatusPayload};
use crate::mcp::handlers::RunContext;
use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};

/// Legacy configured duration value for the mid-turn `when:write` cadence.
/// Retained while the schema still accepts `timeout`; check batches discard it
/// before execution because elapsed time has no termination authority.
const DEFAULT_WRITE_CHECK_TIMEOUT_MS: u32 = 600_000;
/// Legacy configured duration value for the turn-end `when:review` cadence.
/// It remains part of contract parsing but cannot kill a check.
pub(crate) const DEFAULT_REVIEW_CHECK_TIMEOUT_MS: u32 = 1_800_000;
/// Legacy normalization ceiling for the accepted `timeout` configuration value.
const MAX_CHECK_TIMEOUT_MS: u32 = 3_600_000;

const BASE_CONTENT_CONTENT_ENV: &str = "CAIRN_CHECK_BASE_CONTENT_CONTENT";
const CHANGED_FILES_CONTENT_ENV: &str = "CAIRN_CHECK_CHANGED_FILES_CONTENT";
const EXECUTOR_PROTOCOL_CHECK: &str = "executor-protocol";
#[cfg(test)]
const EXECUTOR_PROTOCOL_SOURCE: &str = "src-tauri/os/cairn-common/src/executor_protocol.rs";
const EXECUTOR_WIRE_POLICY_FILE: &str = "scripts/executor-wire-paths.json";

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ExecutorWirePolicy {
    protocol_source: String,
    wire_behavior_paths: Vec<String>,
    non_wire_runtime_paths: Vec<String>,
    process_path_renames: Vec<(String, String)>,
}

fn executor_wire_policy(
    objects: &cairn_vcs::jj::ObjectReader,
    head: &str,
    base: &str,
) -> Result<ExecutorWirePolicy, String> {
    let load = || -> Result<ExecutorWirePolicy, String> {
        for commit in [head, base] {
            let Some(bytes) = objects.read_commit_file(commit, EXECUTOR_WIRE_POLICY_FILE)? else {
                continue;
            };
            return serde_json::from_slice(&bytes).map_err(|error| error.to_string());
        }
        Err(format!("absent from head {head} and base {base}"))
    };
    load().map_err(|error| format!("{EXECUTOR_WIRE_POLICY_FILE}: {error}"))
}

#[cfg(test)]
const TEST_WIRE_POLICY: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../scripts/executor-wire-paths.json"
));

#[cfg(test)]
fn test_wire_policy() -> ExecutorWirePolicy {
    serde_json::from_str(TEST_WIRE_POLICY).unwrap()
}

fn executor_base_content_paths(
    policy: &ExecutorWirePolicy,
    changed: &[GraphFileChange],
) -> BTreeSet<String> {
    let mut paths = BTreeSet::from([policy.protocol_source.clone()]);
    for path in changed.iter().filter_map(|change| {
        let path = change.path.as_str();
        (path.ends_with(".rs")
            && !policy
                .non_wire_runtime_paths
                .iter()
                .any(|excluded| excluded == path)
            && policy
                .wire_behavior_paths
                .iter()
                .any(|wire| path == wire || path.starts_with(wire)))
        .then(|| path.to_string())
    }) {
        if let Some((current, legacy)) = policy
            .process_path_renames
            .iter()
            .find(|(current, _)| path.starts_with(current))
        {
            paths.insert(path.replacen(current, legacy, 1));
        }
        paths.insert(path);
    }
    paths
}

/// The exact path/blob pairs the transport ships, including absent base paths.
fn executor_base_content_entries(
    _jj: &JjEnv,
    repository: &Path,
    base: &str,
    head: &str,
    changed: &[GraphFileChange],
) -> Result<Vec<(String, Option<String>)>, String> {
    let objects = cairn_vcs::jj::ObjectReader::open(repository)
        .map_err(|error| format!("{EXECUTOR_WIRE_POLICY_FILE}: {error}"))?;
    let policy = executor_wire_policy(&objects, head, base)?;
    let paths: Vec<_> = executor_base_content_paths(&policy, changed)
        .into_iter()
        .collect();
    let names: Vec<_> = paths.iter().map(String::as_str).collect();
    let ids = objects.named_path_blob_ids(base, &names)?;
    Ok(paths.into_iter().zip(ids).collect())
}

fn executor_base_content_bundle(
    jj: &JjEnv,
    repository: &Path,
    base: &str,
    head: &str,
    changed: &[GraphFileChange],
) -> Result<String, String> {
    let selected = executor_base_content_entries(jj, repository, base, head, changed)?;
    let ids: Vec<_> = selected
        .iter()
        .filter_map(|(_, id)| id.as_deref())
        .collect();
    let blobs = read_blobs(jj, repository, &ids)?;
    let mut content = BTreeMap::new();
    for (path, id) in selected {
        let source = match id {
            Some(id) => {
                let bytes = blobs
                    .get(&id)
                    .ok_or_else(|| format!("base content for {path} is unreadable"))?;
                Some(
                    std::str::from_utf8(bytes)
                        .map_err(|_| format!("base content for {path} is not UTF-8"))?
                        .to_string(),
                )
            }
            None => None,
        };
        content.insert(path, source);
    }
    serde_json::to_string(&content).map_err(|error| format!("serialize base content: {error}"))
}

/// Normalize the legacy `timeout` configuration value. This value may still
/// travel through planning for mixed-version compatibility, but
/// `Fleet::check_process_batch` replaces it with the unbounded sentinel.
pub(crate) fn resolve_check_timeout_ms(check: Option<&CheckCommand>, default_ms: u32) -> u32 {
    match check.and_then(|c| c.timeout) {
        Some(secs) => secs.saturating_mul(1000).min(MAX_CHECK_TIMEOUT_MS),
        None => default_ms,
    }
}

/// Host-ended compiles from the fleet, shortened, shared by the store's
/// classifier tests and the main cadence's so both halves are held to the
/// same specimens.
#[cfg(test)]
pub(crate) mod host_ended_compile_specimens;

/// Git dependency fetches that failed for want of a credential, shared by the
/// classifier's tests so every signature is held to the same assertions.
#[cfg(test)]
pub(crate) mod git_credential_fetch_specimens;

/// Where a project declares its checks, inside whatever tree is being read.
pub(crate) const CHECKS_CONFIG_PATH: &str = ".cairn/config.yaml";

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
fn checks_contract_at_commit_detailed(
    object_repository: &Path,
    commit: &str,
) -> Result<CommitChecksContract, CommitChecksContractError> {
    let bytes = match crate::mcp::handlers::read::file_at_commit(
        object_repository.to_path_buf(),
        commit.to_string(),
        CHECKS_CONFIG_PATH,
    ) {
        Ok(Some(bytes)) => bytes,
        Ok(None) => return Err(CommitChecksContractError::Missing),
        Err(error) => return Err(CommitChecksContractError::Read(error.to_string())),
    };
    let content = match String::from_utf8(bytes) {
        Ok(content) => content,
        Err(error) => return Err(CommitChecksContractError::InvalidUtf8(error.to_string())),
    };
    // The migration flag is ignored on purpose: a sealed commit is read, never
    // rewritten. Only the checkout loader migrates.
    let (settings, _needs_migration) =
        match crate::config::project_settings::parse_project_settings(&content) {
            Ok(parsed) => parsed,
            Err(error) => return Err(CommitChecksContractError::Parse(error)),
        };
    let contract = crate::config::project_settings::checks_contract_from(settings)
        .ok_or(CommitChecksContractError::NoChecks)?;
    Ok(CommitChecksContract {
        contract,
        defined_by_commit: commit.to_string(),
    })
}

/// Automatic cadences intentionally treat every invalid or absent commit config
/// as selecting no checks. Manual runs use the detailed loader below instead.
pub(crate) fn checks_contract_at_commit(
    object_repository: &Path,
    commit: &str,
) -> Option<CommitChecksContract> {
    checks_contract_at_commit_detailed(object_repository, commit)
        .map_err(|error| {
            log::warn!(
                "checks: {}; no checks are selected for it",
                error.manual_diagnostic(commit)
            );
        })
        .ok()
}

pub(crate) async fn load_checks_contract_at_commit_detailed(
    object_repository: &Path,
    commit: &str,
) -> Result<CommitChecksContract, CommitChecksContractError> {
    let object_repository = object_repository.to_path_buf();
    let commit = commit.to_string();
    tokio::task::spawn_blocking(move || {
        checks_contract_at_commit_detailed(&object_repository, &commit)
    })
    .await
    .map_err(|error| CommitChecksContractError::BlockingTask(error.to_string()))?
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
/// every turn-end; `when:write` never runs here (it is the mid-turn cadence),
/// and neither does `when:main`, which no branch wave runs. An
/// impact-scoped check that no changed file matches has `applies == false`. Pure,
/// so the cadence gate is unit-tested.
pub(crate) fn applicable_turn_end_checks(
    checks: &HashMap<String, CheckCommand>,
    inputs: &ResolvedInputs,
    changed: &[GraphFileChange],
    repo_root: &Path,
) -> Vec<CheckPlan> {
    turn_end_plans(plan_checks(checks, inputs, changed, repo_root), checks)
}

/// The turn-end cadence's share of a full plan set ([`applicable_turn_end_checks`]),
/// for a caller that also needs the plans of every other cadence.
pub(crate) fn turn_end_plans(
    plans: Vec<CheckPlan>,
    checks: &HashMap<String, CheckCommand>,
) -> Vec<CheckPlan> {
    plans
        .into_iter()
        .filter(|plan| plan.applies)
        .filter(|plan| {
            checks
                .get(&plan.name)
                .is_some_and(|check| match check.when {
                    CheckWhen::Review => true,
                    CheckWhen::Write | CheckWhen::Main => false,
                })
        })
        .collect()
}

/// Whether this runner's platform is allowed to produce the check's verdict.
///
/// Platform-scoped checks outside the runner's gate are not batch members. They
/// remain visible as not applicable through the status surface, but must never
/// constrain or conflict with the placement of checks this runner does gate on.
fn check_applies_to_gating_platform(check: &CheckCommand, platform: &str) -> bool {
    let platforms = crate::execution::check_identity::verdict_platforms(check);
    platforms.is_empty() || platforms.iter().any(|candidate| candidate == platform)
}

fn retain_gating_platform_plans(
    plans: &mut Vec<CheckPlan>,
    checks: &HashMap<String, CheckCommand>,
    platform: &str,
) {
    plans.retain(|plan| {
        checks
            .get(&plan.name)
            .is_some_and(|check| check_applies_to_gating_platform(check, platform))
    });
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
///
/// A base-relative check's row also keyed the change it was measured against,
/// which the row does not record, so its key is recomputed against an unknown
/// change and never matches: such a check always takes the cumulative diff.
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
        CheckChange::unknown(&baseline.check_name),
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
#[derive(Clone)]
pub(crate) struct CheckOutcome {
    pub(crate) consumer_guard: Option<Arc<crate::fleet::CheckConsumerGuard>>,
    pub(crate) left_running: Option<crate::fleet::LeftRunningAttempt>,
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
    /// re-deriving a key: a legacy remote verdict can be keyed by an empty
    /// fingerprint, and a coalesced sibling's row was never this caller's to
    /// compute. `None` is the honest
    /// answer when nothing was recorded — a suppressed triple never ran, and a
    /// failed write leaves the verdict standing without a durable row.
    pub(crate) recorded: Option<crate::execution::cache::RecordedCheckObservation>,
    /// Concrete cause when this verdict could not become reusable recorded
    /// evidence. This is producer-owned evidence; callers must not infer it from
    /// an empty environment fingerprint.
    pub(crate) not_recorded: Option<String>,
}

impl CheckOutcome {
    /// Whether this outcome is a verdict about the agent's own change. An
    /// infrastructure failure is a fact about Cairn; a suppression is the absence
    /// of a run. Neither is the agent's business, and this is the predicate that
    /// keeps both out of the wake path.
    pub(crate) fn is_genuine_failure(&self) -> bool {
        !self.passed
            && self.left_running.is_none()
            && self.suppressed_after.is_none()
            && self
                .failure_kind
                .is_none_or(CheckFailureKind::is_verdict_about_the_code)
    }
}

/// Versioned identity of the host tools that can affect project-check outcomes.
pub(crate) fn check_platform_identity() -> String {
    format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH)
}

/// The host toolchain as every check key on this process is made with: the
/// keyed instruments only (`cairn_checkenv::keyed_toolchain_identity`), so a
/// key never moves with a recorded-only instrument such as the host's Node.
pub fn check_toolchain_identity() -> &'static str {
    cairn_checkenv::keyed_toolchain_identity()
}

fn check_environment_fingerprint(variable_names: impl IntoIterator<Item = String>) -> String {
    cairn_checkenv::local_fingerprint(variable_names)
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
    failure_kind: Option<CheckFailureKind>,
    parsed: Option<&ParsedCheckResult>,
) -> CheckReuseDecision {
    let reject = |reason: &str| CheckReuseDecision {
        reusable: false,
        reason: Some(reason.to_string()),
    };
    if failure_kind == Some(CheckFailureKind::Prerequisite) {
        return reject("a prerequisite step failed before the suite ran");
    }
    if failure_kind.is_some() {
        return reject("check ended with an infrastructure or process failure");
    }
    // Never served again, even on a host whose environment fingerprint still
    // matches: an executor stamps its toolchain identity once per process, so a
    // host whose Bun was replaced under it would otherwise keep reusing the
    // disowned green instead of running on the runtime it now has.
    if parsed.is_some_and(|parsed| parsed.unverified.is_some()) {
        return reject("the run declared its own result unverified");
    }
    let Some(parsed) = parsed else {
        return if crate::execution::check_parsers::is_test_runner_command(command) {
            reject("test runner did not produce a structured result")
        } else {
            CheckReuseDecision {
                reusable: true,
                reason: None,
            }
        };
    };
    if parsed.is_incomplete_lint() {
        return reject("lint did not run to completion");
    }
    // An eslint verdict over a tree with findings depends on the base it was
    // read against: the same tree is red alone and green over a base holding
    // the same errors. Served by input key, one would answer the other's
    // question, and main's whole-tree red would land on a branch that added
    // nothing (cairn/5851). A clean tree is clean over every base.
    if parsed.parser == ESLINT_PARSER
        && !parsed
            .lint_tree_findings()
            .is_some_and(|tree| tree.is_empty())
    {
        return reject(
            "the linted tree has findings, so its verdict depends on the base it was read against",
        );
    }
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
    // A whole suite always explains statements, so its unjudged audit is a
    // breakdown worth a fresh run. A narrower selection can honestly reach no
    // statement at all, and reusing it repeats that row's own "not evaluated".
    if parsed
        .plan_audit
        .as_ref()
        .is_some_and(|audit| !audit.evaluated() && audit.selection == "full")
    {
        return reject("the plan audit did not evaluate this whole-suite run");
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

/// The machine a check's verdict came from. Every machine-derived field a
/// result row records (executor, platform, architecture, toolchain, verdict
/// environment) is read from this and nothing else, so a row can only be
/// labelled with the platform of the machine that ran it. The coordinator's own
/// platform and environment describe a row only when the coordinator *is* that
/// machine; for a lane placed elsewhere they are the requester's, which is
/// exactly the label that must never be recorded.
#[derive(Clone, Copy)]
enum ObservedOn<'a> {
    /// The executor's own account of the run.
    Executor(&'a cairn_common::executor_protocol::CellExecutionMeta),
    /// A machine claimed the lane and its account of the run was lost; what it
    /// advertised when it claimed is all that is known about where it ran.
    Claim(&'a crate::fleet::CellPlacement),
    /// The coordinator's own process produced the result, with no executor
    /// between them.
    Coordinator,
    /// The lane failed before any machine claimed it, so it ran nowhere.
    Unplaced,
}

impl<'a> ObservedOn<'a> {
    fn resolve(
        provenance: Option<&'a cairn_common::executor_protocol::CellExecutionMeta>,
        claim: Option<&'a crate::fleet::CellPlacement>,
        substrate_failed: bool,
    ) -> Self {
        match (provenance, claim) {
            (Some(meta), _) => Self::Executor(meta),
            (None, Some(placement)) => Self::Claim(placement),
            (None, None) if substrate_failed => Self::Unplaced,
            (None, None) => Self::Coordinator,
        }
    }

    fn attestation(self) -> Option<&'a cairn_common::executor_protocol::CellExecutionMeta> {
        match self {
            Self::Executor(meta) => Some(meta),
            _ => None,
        }
    }

    /// Whether the machine that ran this is the process recording it.
    fn is_coordinator(self) -> bool {
        match self {
            Self::Executor(meta) => meta.executor_id == crate::fleet::COLOCATED_EXECUTOR_ID,
            Self::Claim(placement) => placement.executor_id == crate::fleet::COLOCATED_EXECUTOR_ID,
            Self::Coordinator => true,
            Self::Unplaced => false,
        }
    }

    fn executor_id(self) -> Option<String> {
        match self {
            Self::Executor(meta) => Some(meta.executor_id.clone()),
            Self::Claim(placement) => Some(placement.executor_id.clone()),
            Self::Coordinator | Self::Unplaced => None,
        }
    }

    fn connection_generation(self) -> Option<i64> {
        match self {
            Self::Executor(meta) => Some(meta.executor_connection_generation as i64),
            Self::Claim(placement) => Some(placement.generation as i64),
            Self::Coordinator | Self::Unplaced => None,
        }
    }

    /// The verdict platform and architecture: what the executor attested, else
    /// what the claiming machine advertised, else this process's own when this
    /// process is the machine.
    fn verdict_platform(self) -> (Option<String>, Option<String>) {
        let (os, arch) = match self {
            Self::Executor(meta) => (meta.verdict_platform.clone(), meta.verdict_arch.clone()),
            Self::Claim(placement) => placement.platform.clone().map_or((None, None), |platform| {
                (Some(platform.os), Some(platform.arch))
            }),
            Self::Coordinator | Self::Unplaced => (None, None),
        };
        let here = self
            .is_coordinator()
            .then(crate::fleet::ExecutorPlatform::this_process);
        (
            os.or_else(|| here.as_ref().map(|platform| platform.os.clone())),
            arch.or_else(|| here.map(|platform| platform.arch)),
        )
    }

    fn toolchain_fingerprint(self) -> Option<String> {
        self.attestation()
            .and_then(|meta| meta.toolchain_fingerprint.clone())
            .or_else(|| {
                self.is_coordinator()
                    .then(|| cairn_checkenv::toolchain_identity().to_string())
            })
    }
}

#[allow(clippy::too_many_arguments)]
fn fresh_observation_write(
    project_id: &str,
    sealed_coordinate: &crate::execution::cache::SealedCheckCoordinate,
    plan: &CheckPlan,
    job_id: &str,
    tool_use_id: &str,
    exit_code: i32,
    failure_kind: Option<CheckFailureKind>,
    duration_ms: i64,
    output_tail: &str,
    observed_on: ObservedOn<'_>,
    parsed: Option<&ParsedCheckResult>,
    reuse: CheckReuseDecision,
    infra_reset_scopes: Vec<String>,
) -> FreshCheckObservationWrite {
    let provenance = observed_on.attestation();
    let coordinator_ran = observed_on.is_coordinator();
    let plan_audit = parsed.and_then(|result| result.plan_audit.as_ref());
    let tests = parsed
        .map(|result| result.tests.iter().map(CheckTestResultRow::from).collect())
        .unwrap_or_default();
    // A remote executor's verdict environment is not the coordinator's environment.
    // Modern executors report the identity they computed where the check ran; legacy
    // executors remain diagnostic-only rather than being keyed under a coordinator guess.
    let remote_environment_unknown = provenance.is_some_and(|meta| {
        meta.executor_id != crate::fleet::COLOCATED_EXECUTOR_ID
            && meta.environment_fingerprint.is_empty()
    });
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
    let environment_fingerprint = provenance
        .filter(|meta| !meta.environment_fingerprint.is_empty())
        .map(|meta| meta.environment_fingerprint.clone())
        .unwrap_or_else(|| {
            if coordinator_ran {
                plan_environment_fingerprint(plan)
            } else {
                String::new()
            }
        });
    let (verdict_platform, verdict_arch) = observed_on.verdict_platform();
    let toolchain_fingerprint = observed_on.toolchain_fingerprint();
    let declared_environment = provenance
        .and_then(|meta| meta.verdict_environment_hash.clone())
        .or_else(|| {
            coordinator_ran.then(|| {
                cairn_checkenv::verdict_environment_hash(
                    plan.verdict_environment_names.clone(),
                    |name| std::env::var(name).ok(),
                )
            })
        });
    FreshCheckObservationWrite {
        manual_handle: manual_check_handle(tool_use_id).map(str::to_string),
        publication_id: None,
        sealed_coordinate: sealed_coordinate.clone(),
        id: uuid::Uuid::new_v4().to_string(),
        public_handle: uuid::Uuid::new_v4().simple().to_string()[..24].to_string(),
        project_id: project_id.to_string(),
        commit_sha: sealed_coordinate.evaluated_commit().to_string(),
        defined_by_commit_sha: sealed_coordinate.defined_by_commit().to_string(),
        tree_hash: sealed_coordinate.tree_hash().to_string(),
        check_name: plan.name.clone(),
        input_hash: sealed_coordinate.input_hash().to_string(),
        environment_fingerprint,
        verdict_platform,
        verdict_arch,
        verdict_environment_hash: observed_verdict_environment_hash(
            plan,
            declared_environment,
            toolchain_fingerprint.as_deref(),
        ),
        exit_code,
        verdict: if exit_code == 0 { "passed" } else { "failed" }.to_string(),
        failure_kind: failure_kind.map(|kind| kind.as_str().to_string()),
        // A lint result vouches for itself: one that measured nothing must not
        // be stored as complete. tsc's `complete: false` means "not a test
        // enumeration", so other non-test parses keep counting as complete.
        complete: parsed
            .map(|result| {
                if is_lint_parser(&result.parser) {
                    result.complete
                } else {
                    !result.is_test_runner() || result.complete
                }
            })
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
        cadence: observation_cadence(tool_use_id).to_string(),
        executor_id: observed_on.executor_id(),
        executor_device_id: provenance.map(|meta| meta.executor_device_id.clone()),
        executor_connection_generation: observed_on.connection_generation(),
        executor_cell_id: provenance.map(|meta| meta.cell_id.clone()),
        executor_lease_epoch: provenance.map(|meta| meta.cell_epoch as i64),
        executor_started_at_unix_ms: provenance.map(|meta| meta.started_at_unix_ms as i64),
        executor_finished_at_unix_ms: provenance.map(|meta| meta.finished_at_unix_ms as i64),
        runner_build_id: Some(cairn_checkenv::implementation_identity().to_string()),
        toolchain_fingerprint,
        output_tail: output_tail.to_string(),
        target_results_json: parsed.and_then(|result| serde_json::to_string(result).ok()),
        selection: if plan.scope == CheckScope::Partial {
            Some("partial".to_string())
        } else {
            parsed.map(|result| result.selection.clone())
        },
        deferred_count: parsed.map(|result| result.deferred as i64).unwrap_or(0),
        plan_audit_state: plan_audit.map(|audit| audit.state.clone()),
        plan_audit_note: plan_audit.and_then(|audit| audit.note.clone()),
        audit_findings: plan_audit
            .map(|audit| {
                audit
                    .rows
                    .iter()
                    .map(|row| crate::execution::cache::CheckAuditFindingRow {
                        finding_id: row.id.clone(),
                        status: row.status.clone(),
                        rule: row.rule.clone(),
                        table_name: row.table.clone(),
                        file: row.file.clone(),
                        statement: row.statement.clone(),
                        issue: row.issue.clone(),
                        known_shape: row.known_shape.clone(),
                        excerpt: row.excerpt.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default(),
        phases: parsed.and_then(|result| result.phases.clone()),
        execution_warmth: provenance
            .and_then(|meta| meta.warmth)
            .map(|warmth| warmth.as_str().to_string()),
        cell_origin: provenance
            .and_then(|meta| meta.cell_origin.as_ref())
            .map(crate::execution::cache::ObservedCellOrigin::from_executor),
        build_jobs: provenance
            .and_then(|meta| meta.allocation.as_ref())
            .and_then(|allocation| allocation.build_jobs),
        tests,
        infra_reset_scopes,
        // Set by the runner, which holds the classification and the scopes.
        failure_signature: None,
        placement_facts: None,
        substrate_shape: None,
        nested_sandbox_lane: nested_sandbox_lane(provenance),
    }
}

/// The nested-Seatbelt state of the lane a macOS run executed in, from the
/// evidence its executor attested for that run. A lane that requires the
/// capability lands on a Mac without it only when placement waived the
/// requirement (`NestedSandboxWaiver`), so this is how the row itself says it
/// ran without it (cairn/4062 requirement 4). Elsewhere, and on evidence that
/// names no measured context, nothing.
fn nested_sandbox_lane(
    provenance: Option<&cairn_common::executor_protocol::CellExecutionMeta>,
) -> Option<String> {
    let meta = provenance?;
    meta.verdict_platform
        .as_deref()
        .filter(|platform| platform.eq_ignore_ascii_case("macos"))?;
    meta.sandbox.as_ref()?.check_lane_provenance()
}

/// The verdict environment a fresh observation is stored under: the declared
/// variables as the run saw them, extended by the versions its own run measured
/// for the plan's runtime instruments.
///
/// The versions come from the toolchain the command ran under, never from the
/// plan's expectation: a lane the planner expected on Bun 1.4.4 that landed on a
/// Bun 1.3.14 machine is a Bun 1.3.14 verdict, and only a lookup expecting
/// 1.3.14 may be answered by it. `None` when the run reported no declared
/// environment (an older executor, a lost report), which no lookup matches.
fn observed_verdict_environment_hash(
    plan: &CheckPlan,
    declared: Option<String>,
    toolchain_fingerprint: Option<&str>,
) -> Option<String> {
    declared.map(|declared| {
        let measured = cairn_checkenv::runtime_versions(
            plan.verdict_runtime
                .iter()
                .map(|(instrument, _)| instrument.as_str()),
            toolchain_fingerprint,
        );
        cairn_checkenv::verdict_environment_with_runtime(&declared, &measured)
    })
}

/// Stream prefix of a main-cadence lane. Its observations record
/// `cadence = 'main'` and `job_id = 'main:{project_id}'`; see
/// [`crate::execution::checks_main`].
pub(crate) const MAIN_CADENCE_STREAM_PREFIX: &str = "main-cadence:";

/// A manual request's own identity, not its suite or run's reusable coordinate.
pub(crate) fn manual_check_handle(tool_use_id: &str) -> Option<&str> {
    let handle = tool_use_id
        .strip_prefix("manual-check:")?
        .split(':')
        .nth(1)?
        .strip_prefix("handle=")?;
    uuid::Uuid::parse_str(handle).ok()?;
    Some(handle)
}

/// Consumer identity is independent of scheduling priority: a merge gate may
/// request review priority without being a turn-end review-wave consumer.
async fn check_consumer_identity(
    orch: &Orchestrator,
    job_id: &str,
    stream: &str,
    purpose: Option<&crate::fleet::purpose::PurposeKey>,
) -> Result<(&'static str, String), String> {
    if let Some(purpose) = purpose {
        Ok(("main_cadence", purpose.to_string()))
    } else if stream.starts_with("manual-check:") {
        Ok((
            "manual",
            manual_check_handle(stream).unwrap_or(stream).to_string(),
        ))
    } else if stream.starts_with("turn-checks:") {
        let attempt = orch
            .db
            .local
            .query_opt_text(
                "SELECT turn_end_check_attempt_id FROM jobs WHERE id=?1",
                (job_id.to_string(),),
            )
            .await
            .map_err(|error| error.to_string())?
            .unwrap_or_default();
        Ok(("turn_end", attempt))
    } else {
        Ok(("write", job_id.to_string()))
    }
}

/// The `cadence` an observation records, decided by its producer's stream prefix.
fn observation_cadence(tool_use_id: &str) -> &'static str {
    if tool_use_id.starts_with("turn-checks:") {
        "review"
    } else if tool_use_id.starts_with("manual-check:") {
        "manual"
    } else if tool_use_id.starts_with(MAIN_CADENCE_STREAM_PREFIX) {
        "main"
    } else {
        "write"
    }
}

/// The change a check's verdict is measured against, as its result key sees it.
///
/// Only a base-relative check's key reads it ([`change_identity`]); every other
/// key ignores it and stays byte for byte what it was.
#[derive(Clone, Copy, Debug)]
pub(crate) enum ChangeBasis<'a> {
    /// Measured against a resolved base.
    Against {
        /// Everything that changed since the base: what the transport ships.
        changed: &'a [GraphFileChange],
        /// The selected base path/blob pairs. Read only for a check Cairn ships base
        /// content to, selected by the same commit-owned policy as the bundle;
        /// `None` leaves such a check's key unformable.
        base_entries: Option<&'a [(String, Option<String>)]>,
        /// The base finding set this evaluation ships a base-subtracting check
        /// ([`crate::execution::lint_baseline::lint_baseline_for`]), which its
        /// key folds; `None` exactly when none ships (the base is the head).
        base_findings: Option<&'a crate::execution::lint_baseline::LintBaselineTransport>,
    },
    /// No base was resolved, so there is no change to name.
    Unknown,
}

/// A configured check, by name, and the change its key is measured against.
#[derive(Clone, Copy, Debug)]
pub(crate) struct CheckChange<'a> {
    /// Which transports Cairn ships a check, and so what its key has to name,
    /// is decided by its configured name ([`needs_executor_base_content`]).
    pub(crate) name: &'a str,
    pub(crate) basis: ChangeBasis<'a>,
}

impl<'a> CheckChange<'a> {
    pub(crate) fn unknown(name: &'a str) -> Self {
        Self {
            name,
            basis: ChangeBasis::Unknown,
        }
    }
}

/// Whether a check's verdict is a function of the change it is measured
/// against as well as of its tree: it declares `baseRelative`, or Cairn ships
/// it base content, which only a base-relative check could want.
pub(crate) fn is_base_relative(name: &str, check: &CheckCommand) -> bool {
    check.base_relative || needs_executor_base_content(name)
}

/// The change component of a base-relative check's result key, or `None` for a
/// check whose verdict is read off its tree alone.
///
/// It names the changed paths the check's own inputs select, as the transport
/// ships them, and, for a check Cairn ships base content to, the base blob each
/// bundled path came from. The inputs bound the change for the same reason they
/// bound the tree: a check depends on what it declares, so an edit elsewhere on
/// the branch neither moves its key nor costs it a re-run.
///
/// A base-relative check whose change cannot be named gets a key no other
/// evaluation produces. It neither reuses a verdict nor leaves one for another
/// evaluation to reuse: a verdict measured against an unknown change answers no
/// question anyone else asks.
fn change_identity(
    check: &CheckCommand,
    selector: &InputSelector,
    change: CheckChange<'_>,
) -> Option<String> {
    if !is_base_relative(change.name, check) {
        return None;
    }
    let unformable = || format!("unformable:{}", uuid::Uuid::new_v4().simple());
    let ChangeBasis::Against {
        changed,
        base_entries,
        ..
    } = change.basis
    else {
        return Some(unformable());
    };
    let paths: Vec<&str> = transport_paths(changed)
        .into_iter()
        .filter(|path| selector.matches(path))
        .collect();
    let base_content = if needs_executor_base_content(change.name) {
        let Some(base_entries) = base_entries else {
            return Some(unformable());
        };
        Some(base_entries.to_vec())
    } else {
        None
    };
    Some(crate::execution::check_identity::change_fingerprint(
        &paths,
        base_content.as_deref(),
    ))
}

/// The base-findings component of a base-subtracting check's result key, or
/// `None` for any other check and for a run that ships no base set.
///
/// The check is not base-relative: the change does not enter its key, and the
/// base enters only as the finding set its command subtracts. So the key names
/// that set and nothing else about the base, and a run with no base to
/// subtract keys exactly as the base's own whole-tree run does.
fn base_findings_identity(change: CheckChange<'_>) -> Option<String> {
    if !crate::execution::lint_baseline::subtracts_base_findings(change.name) {
        return None;
    }
    let ChangeBasis::Against {
        base_findings: Some(transport),
        ..
    } = change.basis
    else {
        return None;
    };
    Some(crate::execution::check_identity::base_findings_fingerprint(
        transport.fingerprints.as_deref(),
    ))
}

/// The selected base path/blob pairs, when some configured check's key names base blobs
/// ([`change_identity`]). Missing base paths have no blob id. `None` when no
/// check needs these pairs, or when the commit policy or base cannot be read;
/// the latter leaves that check's key unformable rather than wrong.
pub(crate) fn key_base_entries(
    checks: &HashMap<String, CheckCommand>,
    jj: &JjEnv,
    repository: &Path,
    base: &str,
    head: &str,
    changed: &[GraphFileChange],
) -> Option<Vec<(String, Option<String>)>> {
    checks
        .keys()
        .any(|name| needs_executor_base_content(name))
        .then(|| executor_base_content_entries(jj, repository, base, head, changed).ok())
        .flatten()
}

#[allow(clippy::too_many_arguments)]
fn check_command_identity(
    check: &CheckCommand,
    selector: &InputSelector,
    entries: Option<&[(String, String)]>,
    tree_hash: &str,
    platform: &str,
    toolchain: &str,
    change: CheckChange<'_>,
) -> String {
    let (os, arch) = platform.split_once('-').unwrap_or((platform, "unknown"));
    let base_findings = base_findings_identity(change);
    let change = change_identity(check, selector, change);
    let content = crate::execution::check_identity::content_identity(
        check,
        selector,
        entries,
        tree_hash,
        change.as_deref(),
        base_findings.as_deref(),
    );
    let environment = crate::execution::check_identity::environment_identity(
        crate::execution::check_identity::CheckEnvironmentInput {
            os: os.to_string(),
            arch: arch.to_string(),
            executor_id: None,
            device_id: None,
            // Projected here as well, so no caller can key a verdict by a
            // recorded-only instrument.
            capabilities: vec![cairn_checkenv::keyed_identity(toolchain)],
            runner_build_id: Some(cairn_checkenv::implementation_identity().to_string()),
            variable_names: crate::execution::check_identity::verdict_environment_names(check),
        },
        |name| std::env::var(name).ok(),
    );
    crate::execution::check_identity::combined_result_key(&content, &environment)
}

pub(crate) fn check_result_key(
    check: &CheckCommand,
    selector: &InputSelector,
    entries: Option<&[(String, String)]>,
    tree_hash: &str,
    platform: &str,
    toolchain: &str,
    change: CheckChange<'_>,
) -> String {
    check_command_identity(
        check, selector, entries, tree_hash, platform, toolchain, change,
    )
}

/// The input hash one configured check is evaluated, reused, and READ under at a
/// sealed tree, as this host computes it.
///
/// Every party asking "which verdict answers this check at this head?" must ask
/// it with the same key: the write and review cadences when they decide whether
/// to run or reuse, and the `/checks` projection when it decides what a lane
/// shows. A manual run of a placeholder-free check lands on this key too; a
/// manual or main-health run of a SELECTOR check does not, because it executes
/// the whole suite and execution breadth is evidence identity (see
/// `manual_check_plan`). When the projection keyed on the whole tree instead, a lane
/// answered by reuse — which records only a commit alias, and leaves the cache row
/// on whichever tree last wrote that input hash — rendered `pending` over a
/// stored verdict.
///
/// `entries` must be the sealed tree's listing whenever any configured check
/// declares inputs (see [`any_check_declares_inputs`]) and may be `None`
/// otherwise; every caller derives it the same way. `change` is the change the
/// evaluation is measured against, which a base-relative check's key names; a
/// caller that cannot resolve a base says so with [`ChangeBasis::Unknown`].
pub(crate) fn check_input_hash(
    check: &CheckCommand,
    selector: &InputSelector,
    entries: Option<&[(String, String)]>,
    tree_hash: &str,
    change: CheckChange<'_>,
) -> String {
    check_result_key(
        check,
        selector,
        entries,
        tree_hash,
        &check_platform_identity(),
        check_toolchain_identity(),
        change,
    )
}

/// [`check_input_hash`] for every configured check at one sealed tree, by name.
pub(crate) fn check_input_hashes(
    checks: &HashMap<String, CheckCommand>,
    inputs: &ResolvedInputs,
    entries: Option<&[(String, String)]>,
    tree_hash: &str,
    basis: ChangeBasis<'_>,
) -> HashMap<String, String> {
    checks
        .iter()
        .map(|(name, check)| {
            (
                name.clone(),
                check_input_hash(
                    check,
                    inputs.for_check(name),
                    entries,
                    tree_hash,
                    CheckChange { name, basis },
                ),
            )
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn check_result_key_for_executed_command(
    check: &CheckCommand,
    command: &str,
    selector: &InputSelector,
    entries: Option<&[(String, String)]>,
    tree_hash: &str,
    platform: &str,
    toolchain: &str,
    change: CheckChange<'_>,
) -> String {
    let mut executed = check.clone();
    executed.command = command.to_string();
    check_result_key(
        &executed, selector, entries, tree_hash, platform, toolchain, change,
    )
}

/// A verdict a planned lane may report instead of executing, with the
/// observation identity that reuse must cite.
pub(crate) struct ReusedCheckVerdict {
    pub(crate) entry: crate::execution::cache::CheckResultCacheEntry,
    pub(crate) source_observation_id: String,
    pub(crate) environment_fingerprint: String,
    pub(crate) public_handle: String,
}

/// Whether a planned lane already holds a verdict this run may report without
/// executing it, and that verdict if so.
///
/// The ONE predicate for that question, because two parties ask it and they must
/// not disagree: a caller deciding whether to submit a lane at all, and the
/// engine's cache phase deciding whether to consume that lane's batch result.
/// Disagreement in one direction runs a command whose answer is already stored;
/// in the other it skips a lane nothing will answer. Reuse also requires a
/// resolvable public handle, because a verdict that cannot be cited is not
/// evidence a reader can follow.
pub(crate) fn reusable_verdict_for_plan(
    db: Arc<LocalDb>,
    project_id: &str,
    plan: &CheckPlan,
    input_hash: &str,
    effective_env: &[(String, String)],
) -> Option<ReusedCheckVerdict> {
    reusable_verdict(
        db,
        project_id,
        &plan.name,
        input_hash,
        &plan.verdict_platforms,
        plan.verdict_executor_ids.as_deref(),
        &effective_verdict_environment_hash(plan, effective_env),
    )
}

/// [`reusable_verdict_for_plan`] for a caller holding the configured check
/// rather than a plan, under no environment overrides: exactly the question a
/// turn-end wave asks (`TURN_END_EFFECTIVE_ENV` is empty). A plan's trust set,
/// verdict-environment names, and runtime are derived from its configured check
/// by these same functions, so the two entry points cannot disagree.
/// `expected_toolchain` is the toolchain identity the plan would have been
/// planned under (`cairn_checkenv::expected_toolchain_identity_in`).
///
/// It reads on a connection the caller already holds, so a projection asking
/// it for every check makes all of its reads in one transaction.
pub(crate) async fn reusable_verdict_for_check_conn(
    conn: &crate::storage::TrackedConnection,
    project_id: &str,
    name: &str,
    check: &CheckCommand,
    input_hash: &str,
    expected_toolchain: &str,
) -> Option<ReusedCheckVerdict> {
    let environment_hash = cairn_checkenv::verdict_environment_with_runtime(
        &cairn_checkenv::verdict_environment_hash(
            crate::execution::check_identity::verdict_environment_names(check),
            |variable| std::env::var(variable).ok(),
        ),
        &crate::execution::check_identity::expected_verdict_runtime(check, expected_toolchain),
    );
    reusable_verdict_conn(
        conn,
        project_id,
        name,
        input_hash,
        &crate::execution::check_identity::verdict_platforms(check),
        None,
        &environment_hash,
    )
    .await
}

fn reusable_verdict(
    db: Arc<LocalDb>,
    project_id: &str,
    check_name: &str,
    input_hash: &str,
    verdict_platforms: &[String],
    executor_ids: Option<&[String]>,
    verdict_environment_hash: &str,
) -> Option<ReusedCheckVerdict> {
    let project_id = project_id.to_string();
    let check_name = check_name.to_string();
    let input_hash = input_hash.to_string();
    let verdict_platforms = verdict_platforms.to_vec();
    let executor_ids = executor_ids.map(<[String]>::to_vec);
    let verdict_environment_hash = verdict_environment_hash.to_string();
    crate::execution::cache::run_checkpoint_cache_db(async move {
        db.read(move |conn| {
            Box::pin(async move {
                Ok(reusable_verdict_conn(
                    conn,
                    &project_id,
                    &check_name,
                    &input_hash,
                    &verdict_platforms,
                    executor_ids.as_deref(),
                    &verdict_environment_hash,
                )
                .await)
            })
        })
        .await
        .map_err(|error| error.to_string())
    })
    .ok()
    .flatten()
}

/// The reusable verdict and its citation, read in one transaction. A lookup
/// that fails reads as no reuse, which runs the check rather than trusting a
/// verdict that could not be read.
async fn reusable_verdict_conn(
    conn: &crate::storage::TrackedConnection,
    project_id: &str,
    check_name: &str,
    input_hash: &str,
    verdict_platforms: &[String],
    executor_ids: Option<&[String]>,
    verdict_environment_hash: &str,
) -> Option<ReusedCheckVerdict> {
    let reused = crate::execution::cache::get_reusable_check_result_conn(
        conn,
        ReusableCheckLookup {
            project_id,
            check_name,
            input_hash,
            verdict_platforms,
            executor_ids,
            implementation_identity: cairn_checkenv::implementation_identity(),
            verdict_environment_hash,
            result_schema_version: crate::execution::check_identity::CHECK_RESULT_SCHEMA_VERSION
                as i64,
        },
    )
    .await
    .ok()
    .flatten()?;
    let public_handle = crate::execution::cache::get_check_observation_public_handle_conn(
        conn,
        project_id,
        &reused.source_observation_id,
    )
    .await
    .ok()
    .flatten()?;
    Some(ReusedCheckVerdict {
        entry: reused.entry,
        source_observation_id: reused.source_observation_id,
        environment_fingerprint: reused.environment_fingerprint,
        public_handle,
    })
}

/// The verdict that answers each configured check at one head, as the `/checks`
/// projection renders it.
///
/// Two sources, and only two. This head's OWN evaluations — any verdict row
/// written at its tree, and infrastructure rows scoped to it — are its own
/// evidence and always count. Evidence from any OTHER tree counts only when it
/// is the verdict the engine would serve for the check's input hash without
/// executing it, decided by the engine's own reuse predicate
/// ([`reusable_verdict_for_check_conn`]): reusable, complete, inside the check's
/// verdict-platform trust set, same runner and verdict environment, produced
/// under the runtime the head expects. A weaker
/// cross-tree rule would mark a lane answered that the next wave would
/// actually run, and a waiter would settle on it while that wave was running.
///
/// When both sources hold a row, [`select_lane_row`] chooses: an answer
/// outranks an attempt that produced none, then the newer row wins. A reusable
/// verdict is an answer by construction, so a later run at this head that could
/// not produce one never hides it. Reuse records only a commit alias and leaves
/// the source row on the tree that produced it, so a row whose tree differs
/// from the head's was reused here, not produced here.
///
/// [`select_lane_row`]: crate::execution::checks_status::select_lane_row
///
/// This entry point is for synchronous callers: every read happens in one
/// transaction on one off-runtime hop. An async caller uses
/// [`head_verdicts_conn`] directly and makes no hop at all.
pub(crate) fn head_verdicts(
    db: Arc<LocalDb>,
    project_id: &str,
    checks: &HashMap<String, CheckCommand>,
    head: crate::execution::cache::HeadVerdictKeys<'_>,
) -> Result<Vec<crate::execution::checks_status::LaneRow>, String> {
    let project_id = project_id.to_string();
    let checks = checks.clone();
    let head = crate::execution::cache::OwnedHeadVerdictKeys::from(head);
    crate::execution::cache::run_checkpoint_cache_db(async move {
        db.read(move |conn| {
            Box::pin(
                async move { head_verdicts_conn(conn, &project_id, &checks, head.keys()).await },
            )
        })
        .await
        .map_err(|error| format!("Failed to read head verdicts: {error}"))
    })
}

/// [`head_verdicts`] on a connection the caller already holds.
pub(crate) async fn head_verdicts_conn(
    conn: &crate::storage::TrackedConnection,
    project_id: &str,
    checks: &HashMap<String, CheckCommand>,
    head: crate::execution::cache::HeadVerdictKeys<'_>,
) -> crate::storage::DbResult<Vec<crate::execution::checks_status::LaneRow>> {
    use crate::execution::checks_status::{select_lane_row, LaneRow};
    let mut by_name: HashMap<String, LaneRow> =
        crate::execution::cache::list_check_results_for_inputs_conn(conn, project_id, head)
            .await?
            .into_iter()
            .map(|lane| (lane.row.check_name.clone(), lane))
            .collect();
    for (name, input_hash) in head.input_hashes {
        let Some(check) = checks.get(name) else {
            continue;
        };
        let Some(reused) = reusable_verdict_for_check_conn(
            conn,
            project_id,
            name,
            check,
            input_hash,
            head.expected_toolchain,
        )
        .await
        else {
            continue;
        };
        // The head's own row leads, so it keeps the lane on a tie.
        let candidates = match by_name.remove(name) {
            Some(own) => [own.row, reused.entry]
                .into_iter()
                .chain(own.newer_attempt)
                .collect(),
            None => vec![reused.entry],
        };
        if let Some(lane) = select_lane_row(candidates) {
            by_name.insert(name.clone(), lane);
        }
    }
    let mut rows: Vec<LaneRow> = by_name.into_values().collect();
    // A lane shown by an infrastructure failure may be one the breaker holds;
    // only then is it worth the observation reads that say so.
    for lane in &mut rows {
        if lane.row.failure_kind.as_deref() == Some(CheckFailureKind::Infrastructure.as_str()) {
            lane.held = crate::execution::lane_breaker::held_lane_conn(
                conn,
                project_id,
                &lane.row.check_name,
                &lane.row.input_hash,
                lane.row.cached == Some(true),
            )
            .await?;
        }
    }
    rows.sort_by(|left, right| left.row.check_name.cmp(&right.row.check_name));
    Ok(rows)
}

fn unix_time_ms_for_checks() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// Every test module stays declared here under its pre-split name, whatever
// file holds it: the per-test outcome ledger and open incidents key tests by
// their full module path, so moving a declaration renames its tests.
#[cfg(test)]
mod tests;

#[cfg(test)]
mod executor_base_content_tests;

#[cfg(test)]
mod manual_transport_tests;

#[cfg(test)]
mod write_wave_target_tests;

/// A base-relative check's verdict is a function of its tree AND the change it
/// was measured against, so its key names both.
#[cfg(test)]
mod base_relative_key_tests;

/// The write-check submission path fits in half a default thread stack.
///
/// A write wave runs as its own task, so its poll chain is rooted at the task
/// harness rather than under a tool call, and it reaches placement through a
/// dozen async layers: the wave, the batch submission spine, the fleet's
/// queue entry, and the placement pass. No other test runs that chain with an
/// executor attached to answer it, so this one does, end to end, inside the
/// shared stack budget.
#[cfg(test)]
#[path = "checks/write_check_stack_headroom_tests.rs"]
mod write_check_stack_headroom;

#[cfg(test)]
pub(crate) fn notify_adoption_retry(id: &str, error: &str) {
    adoption_tests::notify_observation_retry(id, error);
}

#[cfg(test)]
#[path = "checks_adoption_tests.rs"]
mod adoption_tests;

#[cfg(test)]
#[path = "checks_cross_commit_tests.rs"]
mod cross_commit_tests;

/// A manual check captured, then queued: everything about it was fixed at the
/// capture, so moving the branch, its base, and the contract while it waits for
/// a machine changes nothing it materializes, runs, or records.
#[cfg(test)]
mod queued_capture_tests;
