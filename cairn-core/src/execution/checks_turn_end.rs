//! Turn-end (`when:idle` / `when:review`) project-check cadence.
//!
//! Where the `when:write` runner ([`crate::execution::checks`]) fires mid-turn
//! against a just-sealed commit and streams into the live transcript, this cadence
//! fires at TURN-END — when the agent goes idle — where there is no running turn
//! and no `RunContext` to stream into. It is invoked from the two turn-end hooks in
//! `orchestrator::lifecycle` (`finalize_run` and `transition_to_warm_state`),
//! detached onto a background task so the minutes-long suite never blocks the turn
//! from ending.
//!
//! ## Cadence gate
//!
//! `when:review` checks (including the `idle` legacy alias) run at every
//! turn-end; `when:write` never runs here (it is the mid-turn cadence). "Every
//! turn-end" means every turn that actually ENDS: a turn that yields by
//! self-suspension (`waitFor`, a durably-suspended `run` batch, a blocking
//! question or task append) leaves the job mid-work, and the trigger
//! (`lifecycle::review_push::spawn_turn_end_checks`) skips it — see the gate
//! there for why, and `docs/checks.md` for the trade it makes.
//! Selection reuses the write cadence's machinery
//! ([`crate::execution::selection::plan_checks`], the impact gate, placeholder
//! substitution) via [`crate::execution::checks::applicable_turn_end_checks`], and
//! results share the `check_result_cache` keyed by each check's input hash.
//!
//! ## Unsandboxed by design
//!
//! At turn-end the agent is idle, so an interactive fence permission prompt would
//! hang with no one to answer. The suite therefore runs UNCONFINED, with host
//! permissions: these are trusted, system-driven project-config commands read
//! from the live main checkout — the identical trust basis as the write cadence.
//! The decision itself lives with the batch submission, in
//! [`crate::fleet::Fleet::CHECK_CADENCE_SANDBOX_MODE`], which both cadences share.
//!
//! This is load-bearing rather than incidental. macOS sandboxes do not nest, so a
//! confined suite exits `71` the moment a test spawns its own `sandbox-exec`, and
//! the `CAIRN_SANDBOXED=1` that accompanies any policy makes fence-sensitive tests
//! self-skip — which is how this lane came to be structurally red on every branch
//! (CAIRN-3124). Confinement is not what keeps a review check from publishing:
//! [`crate::fleet::MutationPolicy::PureVerdict`] is.
//!
//! ## No fold
//!
//! The turn is over and there is no commit to amend, so check-made changes are NOT
//! folded (unlike the write cadence). Turn-end checks are pure verifies (tests),
//! not fixers; a verify that dirties tracked files would leave the worktree != HEAD
//! and is out of contract for this cadence.
//!
//! ## Slot-backed concurrency
//!
//! Cache-miss review checks run sequentially through one persistent cell lease. The
//! slot scheduler owns admission and backpressure; the shared check engine still
//! owns caching, parsing, ordered results, cancellation, and wake delivery. There
//! is no clone or in-place fallback. Substrate failures become infrastructure
//! verdicts and the command is never invoked elsewhere.
//!
//! ## Two guards keep it from looping
//!
//! - Single-flight (`Orchestrator::try_begin_turn_end_checks`): a rapid re-idle
//!   never stacks a second suite for the same job.
//! - Delivery-state dampening: red checks may intentionally execute again, but an
//!   unchanged sealed tree plus unchanged canonical outcome fingerprint wakes the
//!   author only once. Distinct failures and post-green regressions wake normally.

use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use cairn_common::uri::{build_node_checks_uri, build_task_checks_uri};
use sha2::{Digest, Sha256};

use crate::execution::checks::{
    applicable_turn_end_checks, check_platform_identity, check_result_key,
    check_toolchain_identity, load_checks_contract_at_commit, resolve_check_timeout_ms,
    run_planned_checks_at_commit, submit_planned_check_batch, CheckExecMode, CheckFailureKind,
    CheckOutcome, PlannedCheckBatchItem, PlannedCheckBatchRequest, DEFAULT_REVIEW_CHECK_TIMEOUT_MS,
};
use crate::execution::inputs::{
    any_check_declares_inputs, ResolvedInputs, TreeBlobs, TreeSnapshot,
};
use crate::execution::selection::CheckPlan;
use crate::execution::wire::CheckResourceIdentityInput;
use crate::fleet::CellPriority;
use crate::jj::{logical_changed_files, logical_tree_hash, tree_entries, JjEnv};
use crate::orchestrator::{attention_push, Orchestrator, TurnEndCancel};
use crate::storage::{LocalDb, RowExt};

/// Internal batch input carrying newline-delimited changed paths to the executor,
/// which materializes them and sets `CAIRN_CHECK_CHANGED_FILES` to the local path.
const CHANGED_FILES_CONTENT_ENV: &str = "CAIRN_CHECK_CHANGED_FILES_CONTENT";
/// Chars of the live log file surfaced in the "running" render.
const LOG_TAIL_CHARS: usize = 2_000;

/// Background entry point: run the affected turn-end checks for a job, then
/// release the single-flight slot. The caller ([`spawn_turn_end_checks`] in
/// lifecycle) has already claimed the slot via `try_begin_turn_end_checks`; this
/// function is responsible for releasing it on every path.
pub(crate) async fn run_turn_end_checks(orch: Orchestrator, job_id: String, cancel: TurnEndCancel) {
    if let Err(e) = run_turn_end_checks_inner(&orch, &job_id, &cancel).await {
        log::warn!(
            "turn-end checks for job {}: {}",
            &job_id[..job_id.len().min(8)],
            e
        );
    }

    // Release the single-flight slot before the idempotent readiness recovery
    // edge. Review creation no longer waits for detached checks, but completion
    // remains a useful re-evaluation point if another semantic gate settled too.
    orch.end_turn_end_checks(&job_id);
    // Every exit path lands here, so a green completion, an all-cached exit, an
    // empty changed set, or an inner error re-evaluates whether the reviewed
    // issue has settled. Fingerprint dedupe makes this recovery edge harmless
    // when an earlier semantic transition already created the wake.
    if let Some(issue_id) = issue_id_for_job(&orch.db.local, &job_id).await {
        crate::orchestrator::lifecycle::evaluate_review_readiness(&orch, &issue_id).await;
    }
    // Every exit path lands here too, which is the point: a wave that died
    // verdictless settles its node just as truly as one that produced a full
    // green, and a subscriber waiting on the latter must not be stranded by the
    // former (CAIRN-3437).
    crate::orchestrator::wakes::route_checks_settled_edge(&orch, &job_id).await;
}

async fn persist_turn_check_delivery(
    db: &LocalDb,
    recipient: &str,
    checks_uri: &str,
    fingerprint: Option<&str>,
    any_genuine_failed: bool,
) -> Result<(TurnCheckDelivery, Option<attention_push::Wake>), String> {
    let key = format!("turn-checks:{checks_uri}");
    let latest = attention_push::latest_push_fingerprint(db, recipient, &key)
        .await
        .map_err(|error| format!("failed to read turn-check fingerprint: {error}"))?;
    let delivery = turn_check_delivery(
        latest.as_ref().map(|value| value.as_deref()),
        fingerprint,
        any_genuine_failed,
    );
    if delivery == TurnCheckDelivery::Suppress {
        return Ok((delivery, None));
    }
    let (_, effective_wake) = attention_push::push_with_fingerprint(
        db,
        recipient,
        checks_uri,
        delivery_wake(any_genuine_failed),
        attention_push::Boundary::Event,
        &key,
        fingerprint,
    )
    .await
    .map_err(|error| format!("failed to queue turn-check results push: {error}"))?;
    Ok((delivery, Some(effective_wake)))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TurnCheckDelivery {
    Suppress,
    Deliver(&'static str),
}

fn turn_check_delivery(
    latest: Option<Option<&str>>,
    fingerprint: Option<&str>,
    any_genuine_failed: bool,
) -> TurnCheckDelivery {
    let Some(fingerprint) = fingerprint else {
        return TurnCheckDelivery::Deliver("ambiguous/fail-open");
    };
    if latest.flatten() == Some(fingerprint) {
        return TurnCheckDelivery::Suppress;
    }
    if !any_genuine_failed {
        TurnCheckDelivery::Deliver("green")
    } else if latest.is_none() {
        TurnCheckDelivery::Deliver("new")
    } else {
        TurnCheckDelivery::Deliver("changed")
    }
}

/// Signal every in-flight turn-end (`when:review`) check suite belonging to
/// `issue_id` to quit. Fired when the issue reaches a terminal (merged/closed)
/// state: the PR the suite was validating is resolved, so a minutes-long review
/// run against it is wasted work (CAIRN-2648). Best-effort — enumerates the issue's
/// jobs from `db` (the issue's owning database) and pulls each one's cancellation
/// lever, a no-op for any job with no suite in flight.
pub(crate) async fn cancel_turn_end_checks_for_issue(
    orch: &Orchestrator,
    db: &LocalDb,
    issue_id: &str,
) {
    let issue_id_owned = issue_id.to_string();
    let job_ids = db
        .read(|conn| {
            let issue_id = issue_id_owned.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT id FROM jobs WHERE issue_id = ?1",
                        (issue_id.as_str(),),
                    )
                    .await?;
                let mut ids = Vec::new();
                while let Some(row) = rows.next().await? {
                    ids.push(row.text(0)?);
                }
                Ok(ids)
            })
        })
        .await;
    match job_ids {
        Ok(ids) => {
            for job_id in &ids {
                orch.cancel_turn_end_checks(job_id);
            }
            log::debug!(
                "cancel_turn_end_checks_for_issue({}): signalled {} job(s)",
                short_id(issue_id),
                ids.len()
            );
        }
        Err(e) => log::warn!(
            "cancel_turn_end_checks_for_issue({}): failed to enumerate jobs: {}",
            short_id(issue_id),
            e
        ),
    }
}

/// Whether a turn-end (`when:review`) wave may still be launched for a job.
///
/// One decision, taken from the owning database and the live fleet, asked at
/// both moments that matter: before the detached suite is spawned at all, and
/// again immediately before any cell request is submitted. Two calls because the
/// window between them is minutes wide (branch resolution, changed-file diffs,
/// input planning, tree hashing), and an issue that merges inside it has already
/// made the wave pointless. Discovering that only after taking admission has
/// already spent the machine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WaveLaunchability {
    /// Launch: the issue is live, the job is not cancelled, and the workspace it
    /// would check is not on its way out.
    Ready,
    /// Launch nothing and record nothing. A wave withdrawn here never asked for
    /// capacity, so there is no verdict to explain and no red infrastructure
    /// check to explain it with.
    Withdrawn(WaveWithdrawal),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WaveWithdrawal {
    /// The issue reached a terminal state: nobody will review this tree again.
    IssueResolved(String),
    /// The job's own work was cancelled.
    JobCancelled,
    /// The cell this job works in is being reclaimed, so the workspace the
    /// checks would run against is being taken apart underneath them.
    OwnerReclaiming(cairn_common::executor_protocol::ResidencyPhase),
    /// The job's coordinates no longer resolve at all.
    JobUnknown,
}

impl WaveWithdrawal {
    pub(crate) fn describe(&self) -> String {
        match self {
            Self::IssueResolved(status) => format!("its issue is already {status}"),
            Self::JobCancelled => "the job was cancelled".to_string(),
            Self::OwnerReclaiming(phase) => format!(
                "the workspace it would check is being reclaimed ({})",
                match phase {
                    cairn_common::executor_protocol::ResidencyPhase::AwaitingReclaim =>
                        "awaiting reclaim",
                    cairn_common::executor_protocol::ResidencyPhase::Releasing => "releasing",
                    cairn_common::executor_protocol::ResidencyPhase::Active => "active",
                }
            ),
            Self::JobUnknown => "the job no longer resolves".to_string(),
        }
    }
}

/// Ask whether a turn-end wave for `job_id` may launch right now.
///
/// Fails OPEN on every fault it meets. This guard exists to stop work nobody
/// wants, not to become one more way a review gate disappears on a shipping
/// branch: an unreadable database says nothing about whether the issue resolved,
/// and treating it as a withdrawal would silently drop the wave.
pub(crate) async fn turn_end_launchability(orch: &Orchestrator, job_id: &str) -> WaveLaunchability {
    let db = match crate::execution::routing::owning_db_for_job(&orch.db, job_id).await {
        Ok(db) => db,
        Err(error) => {
            log::warn!(
                "turn-end launchability for job {}: owning database unresolved ({error}); \
                 treating the wave as launchable",
                short_id(job_id)
            );
            return WaveLaunchability::Ready;
        }
    };
    // Both statuses come from ONE read, so the answer describes a single instant
    // rather than two that a resolution could land between.
    let owned_job_id = job_id.to_string();
    let statuses = db
        .read(|conn| {
            let job_id = owned_job_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT i.status, j.status
                         FROM jobs j JOIN issues i ON i.id = j.issue_id
                         WHERE j.id = ?1 LIMIT 1",
                        (job_id.as_str(),),
                    )
                    .await?;
                match rows.next().await? {
                    Some(row) => Ok(Some((row.text(0)?, row.text(1)?))),
                    None => Ok(None),
                }
            })
        })
        .await;
    match statuses {
        Ok(Some((issue_status, job_status))) => {
            if matches!(issue_status.as_str(), "merged" | "closed") {
                return WaveLaunchability::Withdrawn(WaveWithdrawal::IssueResolved(issue_status));
            }
            if job_status == "cancelled" {
                return WaveLaunchability::Withdrawn(WaveWithdrawal::JobCancelled);
            }
        }
        // A job row that is not there is not a job whose tree anyone reviews. A
        // project-level job has no issue and never arms this cadence.
        Ok(None) => return WaveLaunchability::Withdrawn(WaveWithdrawal::JobUnknown),
        Err(error) => {
            log::warn!(
                "turn-end launchability for job {}: status read failed ({error}); \
                 treating the wave as launchable",
                short_id(job_id)
            );
            return WaveLaunchability::Ready;
        }
    }
    match reclaiming_owner_phase(&orch.fleet.snapshot().cells, job_id) {
        Some(phase) => WaveLaunchability::Withdrawn(WaveWithdrawal::OwnerReclaiming(phase)),
        None => WaveLaunchability::Ready,
    }
}

/// The phase of this job's own cell residency, when that residency is on its way
/// out. `None` covers both the healthy case and the job that holds no cell at
/// all, because neither is a reason to withhold a wave.
fn reclaiming_owner_phase(
    cells: &[cairn_common::executor_protocol::PersistentCellState],
    job_id: &str,
) -> Option<cairn_common::executor_protocol::ResidencyPhase> {
    use cairn_common::executor_protocol::{ResidencyHolder, ResidencyPhase};
    cells.iter().find_map(|cell| {
        let residency = cell.residency.as_ref()?;
        let ResidencyHolder::Job { job_id: holder } = &residency.holder else {
            return None;
        };
        (holder == job_id && residency.phase != ResidencyPhase::Active).then_some(residency.phase)
    })
}

/// The issue a job belongs to, or `None` for a project-level job.
async fn issue_id_for_job(db: &LocalDb, job_id: &str) -> Option<String> {
    db.read(|conn| {
        let job_id = job_id.to_string();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT issue_id FROM jobs WHERE id = ?1",
                    (job_id.as_str(),),
                )
                .await?;
            crate::storage::next_opt_text(&mut rows, 0).await
        })
    })
    .await
    .ok()
    .flatten()
}

async fn run_turn_end_checks_inner(
    orch: &Orchestrator,
    job_id: &str,
    cancel: &TurnEndCancel,
) -> Result<(), String> {
    // 1. Resolve the node's durable branch coordinate and base anchors.
    let owning_db = crate::execution::routing::owning_db_for_job(&orch.db, job_id)
        .await
        .map_err(|error| error.to_string())?;
    let Some(coords) = resolve_job_coords(&owning_db, job_id).await? else {
        return Ok(());
    };
    // Nothing that follows is owed if this wave may not launch: its verdicts
    // would validate a tree nobody will review again, or a workspace being taken
    // apart underneath it. Asked again immediately before submission, because
    // everything between here and there takes minutes. The mid-flight case (a
    // resolution landing WHILE a check runs) is the `cancel` race around the
    // suite below (CAIRN-2648).
    if let WaveLaunchability::Withdrawn(withdrawal) = turn_end_launchability(orch, job_id).await {
        log::info!(
            "turn-end checks for job {}: {}; nothing to run",
            short_id(job_id),
            withdrawal.describe()
        );
        return Ok(());
    }
    let repo_root = PathBuf::from(&coords.repository_path);
    let store_dir = crate::jj::project_store_dir(&orch.config_dir, &repo_root);
    let logical_repository = if crate::jj::is_jj_dir(&store_dir) {
        store_dir.clone()
    } else {
        repo_root.clone()
    };
    let sealed_commit = cairn_vcs::resolve_coordinate(&logical_repository, &coords.branch)
        .await
        .map_err(|error| {
            format!(
                "turn-end branch '{}' is unresolvable: {error}",
                coords.branch
            )
        })?;
    // The node's own delta starts at the LIVE fork point of its branch from its
    // integration target. The recorded `jobs.base_commit` row is the coordinate
    // the branch was cut at and does not follow a base advance, so diffing from
    // it absorbs every commit the target merged in the meantime — which is how a
    // zero-delta planner selected a full review suite (CAIRN-3108).
    let base =
        match crate::diff::live_job_branch_range(&orch.db.local, job_id, &orch.config_dir).await {
            Ok(Some(range)) => range.base,
            other => {
                log::warn!(
                    "turn-end checks for job {}: no live base coordinate ({}); nothing to run",
                    short_id(job_id),
                    match other {
                        Err(error) => error,
                        _ => "the job has no branch of its own".to_string(),
                    }
                );
                return Ok(());
            }
        };

    // 2. Load the checks contract DECLARED BY the sealed commit this turn is
    // about. Definition and content come from the same tree, so a branch's own
    // `.cairn/config.yaml` edit governs its own cadence and no sibling's
    // (CAIRN-3333).
    let (checks, extra_inputs, defined_by_commit) =
        match load_checks_contract_at_commit(&repo_root, &sealed_commit).await {
            Some(loaded) if !loaded.contract.checks.is_empty() => (
                loaded.contract.checks,
                loaded.contract.extra_inputs,
                loaded.defined_by_commit,
            ),
            _ => {
                log::debug!(
                    "turn-end checks for job {}: commit {} declares no checks; nothing to run",
                    short_id(job_id),
                    sealed_commit
                );
                return Ok(());
            }
        };
    // Hard assert, not a log: a definition arriving from any tree but the sealed
    // commit is the defect this binding exists to prevent, and it must fail
    // visibly rather than record a sibling's check against this node.
    assert_eq!(
        defined_by_commit, sealed_commit,
        "turn-end checks must be defined by the commit they evaluate"
    );

    // 3. Resolve the DB that owns this job (used below to queue the results push).
    let owning = crate::execution::routing::owning_db_for_job(&orch.db, job_id)
        .await
        .map_err(|e| e.to_string())?;

    // 4. Compute changed files between immutable logical coordinates.
    let jj = JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
    let changed_jj = jj.clone();
    let changed_repo = logical_repository.clone();
    let changed_head = sealed_commit.clone();
    let Some(changed) = tokio::task::spawn_blocking(move || {
        logical_changed_files(&changed_jj, &changed_repo, &base, &changed_head)
    })
    .await
    .map_err(|error| format!("turn-end changed-file task failed: {error}"))?
    else {
        log::debug!(
            "turn-end checks for job {}: changed-file set unresolvable; nothing to run",
            short_id(job_id)
        );
        return Ok(());
    };
    if changed.is_empty() {
        log::debug!(
            "turn-end checks for job {}: empty changed-file set; nothing to run",
            short_id(job_id)
        );
        return Ok(());
    }

    // 5. Select the applicable turn-end checks (cadence + input gate). Resolving
    // each check's inputs reads the sealed tree, and target expansion may run
    // cargo metadata, so planning belongs on the blocking pool.
    let planning_checks = checks.clone();
    let planning_extra_inputs = extra_inputs.clone();
    let planning_changed = changed.clone();
    let planning_repo = repo_root.clone();
    let planning_jj = jj.clone();
    let planning_repository = logical_repository.clone();
    let planning_commit = sealed_commit.clone();
    let (plans, planning_entries) = tokio::task::spawn_blocking(move || {
        // The entry listing feeds both the derived closures and the per-check
        // cache key below, so it is read once here and carried forward.
        let entries = if any_check_declares_inputs(planning_checks.values()) {
            tree_entries(&planning_jj, &planning_repository, &planning_commit).ok()
        } else {
            None
        };
        let blobs = TreeBlobs {
            jj: &planning_jj,
            repository: &planning_repository,
        };
        let snapshot = TreeSnapshot::new(entries.as_deref(), &blobs);
        let inputs = ResolvedInputs::resolve(&planning_checks, &planning_extra_inputs, &snapshot);
        let plans = applicable_turn_end_checks(
            &planning_checks,
            &inputs,
            &planning_changed,
            &planning_repo,
        );
        (plans, entries)
    })
    .await
    .map_err(|error| format!("turn-end check planning task failed: {error}"))?;
    if plans.is_empty() {
        log::debug!(
            "turn-end checks for job {}: no applicable review check; nothing to run",
            short_id(job_id)
        );
        return Ok(());
    }

    let applicable_names = plans
        .iter()
        .map(|plan| plan.name.clone())
        .collect::<std::collections::HashSet<_>>();

    // 6. Resolve the immutable tree identity used as the cache key.
    let hash_jj = jj.clone();
    let hash_repo = logical_repository.clone();
    let hash_commit = sealed_commit.clone();
    let tree_hash =
        tokio::task::spawn_blocking(move || logical_tree_hash(&hash_jj, &hash_repo, &hash_commit))
            .await
            .map_err(|error| format!("turn-end tree-hash task failed: {error}"))?
            .map_err(|error| error.to_string())?;
    let canonical_repo = coords.repository_path.clone();

    // Is this node's sealed tree byte-identical to its base's? See
    // `base_tree_coordinate` for why the coordinate is resolved the way it is.
    // Unresolvable leaves this false, which preserves the pre-existing behaviour
    // of running the suite.
    let tree_matches_base = match base_tree_coordinate(&coords) {
        Some(coordinate) => {
            match cairn_vcs::resolve_coordinate(&logical_repository, coordinate).await {
                Ok(base_commit) => {
                    let base_jj = jj.clone();
                    let base_repo = logical_repository.clone();
                    tokio::task::spawn_blocking(move || {
                        logical_tree_hash(&base_jj, &base_repo, &base_commit)
                    })
                    .await
                    .ok()
                    .and_then(Result::ok)
                    .is_some_and(|base_tree| base_tree == tree_hash)
                }
                Err(error) => {
                    log::debug!(
                        "turn-end checks for job {}: base coordinate '{coordinate}' is \
                         unresolvable ({error}); falling through to the changed-file gate",
                        short_id(job_id)
                    );
                    false
                }
            }
        }
        None => false,
    };

    // Publish the immutable facts the 1 Hz status poll needs. They remain valid
    // until the single-flight slot is released because this suite is pinned to
    // the sealed tree.
    cancel.set_runtime_status(tree_hash.clone(), applicable_names);

    // 7. Loop-break gate: drop any plan already cached for its INPUT hash (the
    // content of just that check's impact-matched files). A covered plan is
    // re-stamped onto the current whole tree so the `/checks` listing still shows
    // it, then skipped; only genuinely-uncovered plans run. If none remain, the
    // tree has already been fully checked (e.g. a resume that committed nothing) —
    // return WITHOUT launching so the agent is never nagged on the same break.
    let db = orch.db.local.clone();
    let cache_db = db.clone();
    let cache_checks = checks.clone();
    let cache_extra_inputs = extra_inputs.clone();
    let cache_jj = jj.clone();
    let cache_repo = logical_repository.clone();
    let cache_tree_hash = tree_hash.clone();
    let cache_project_id = coords.project_id.clone();
    let cache_job_id = job_id.to_string();
    let cache_commit = sealed_commit.clone();
    let cache_filtered = tokio::task::spawn_blocking(move || {
        let entries = planning_entries;
        let blobs = TreeBlobs {
            jj: &cache_jj,
            repository: &cache_repo,
        };
        let snapshot = TreeSnapshot::new(entries.as_deref(), &blobs);
        let inputs = ResolvedInputs::resolve(&cache_checks, &cache_extra_inputs, &snapshot);
        let mut to_run: Vec<(CheckPlan, String)> = Vec::new();
        let mut suppressed: Vec<String> = Vec::new();
        for plan in plans {
            let check = cache_checks
                .get(&plan.name)
                .expect("planned check must retain its configured definition");
            let input_hash = check_result_key(
                check,
                inputs.for_check(&plan.name),
                entries.as_deref(),
                &cache_tree_hash,
                &check_platform_identity(),
                check_toolchain_identity(),
            );
            // Turn-end checks are fleet-backed. Placement has not happened yet,
            // so a coordinator-local observation cannot prove the environment of
            // the executor that will be selected. Preserve infrastructure
            // suppression, but fail safe by sending every otherwise runnable plan
            // to fleet admission until selection supplies trusted identity.
            match crate::execution::cache::get_suppressed_check_result(
                cache_db.clone(),
                &cache_project_id,
                &plan.name,
                &input_hash,
                &cache_job_id,
                &cache_commit,
            ) {
                Ok(Some(entry)) => {
                    crate::execution::checks::record_suppressed_check(
                        &cache_db,
                        &cache_tree_hash,
                        &cache_job_id,
                        &cache_commit,
                        &plan.name,
                        entry,
                    );
                    suppressed.push(plan.name.clone());
                }
                _ => to_run.push((plan, input_hash)),
            }
        }
        (to_run, suppressed)
    })
    .await
    .map_err(|error| format!("turn-end cache planning task failed: {error}"))?;
    let (cache_filtered, suppressed) = cache_filtered;
    if !suppressed.is_empty() {
        log::warn!(
            "turn-end checks for job {}: running none of [{}] \u{2014} infrastructure-suppressed \
             after {} consecutive infrastructure failures",
            short_id(job_id),
            suppressed.join(", "),
            crate::execution::cache::OBSERVED_INFRA_FAILURE_BOUND
        );
    }
    let to_run = match turn_end_launch(cache_filtered, tree_matches_base) {
        TurnEndLaunch::Launch(plans) => plans,
        TurnEndLaunch::Skip(TurnEndSkip::FullyCached) => {
            log::debug!(
                "turn-end checks for job {}: every applicable check is already cached for this tree; nothing to run",
                short_id(job_id)
            );
            return Ok(());
        }
        TurnEndLaunch::Skip(TurnEndSkip::TreeMatchesBase(names)) => {
            log::info!(
                "turn-end checks for job {}: sealed tree is identical to the base's; \
                 inheriting cached verdicts and running none of [{}]",
                short_id(job_id),
                names.join(", ")
            );
            return Ok(());
        }
    };
    log::info!(
        "turn-end checks for job {}: launching {} check(s) [{}] over {} changed file(s)",
        short_id(job_id),
        to_run.len(),
        to_run
            .iter()
            .map(|(p, _)| p.name.as_str())
            .collect::<Vec<_>>()
            .join(", "),
        changed.len()
    );

    // 8. Prepare the host-readable, job-scoped log DIRECTORY (cleared for a fresh
    // run) so the PR-node / `/checks` render can tail each check's OWN log live
    // while the suite runs. One file per check keeps a running check's preview
    // scoped to that check instead of the whole suite's interleaved output.
    let log_dir = turn_end_log_dir(orch, job_id);
    prepare_log_dir(&log_dir);
    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "check_result_cache", "action": "update"}),
    );

    // 9. Build the changed-files override consumed by diff-scoped check scripts.
    // Cell checkouts are materialized at the immutable request base, so the
    // already-computed agent delta remains the canonical attribution source.
    let changed_files_body = changed
        .iter()
        .map(|change| change.path.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    let extra_env = vec![(CHANGED_FILES_CONTENT_ENV.to_string(), changed_files_body)];

    // 10. Submit every miss through one sequential pure-verdict lease, then feed
    // the keyed outcomes through the shared persistence, parsing, and ordering path.
    let timeouts: Vec<u32> = to_run
        .iter()
        .map(|(plan, _)| {
            resolve_check_timeout_ms(checks.get(&plan.name), DEFAULT_REVIEW_CHECK_TIMEOUT_MS)
        })
        .collect();
    let checks_tool_id = format!("turn-checks:{job_id}");
    let items: Vec<_> = to_run
        .iter()
        .enumerate()
        .map(|(index, (plan, input_hash))| {
            let log_path = turn_end_log_path(orch, job_id, &plan.name);
            let _ = std::fs::write(log_path, b"");
            PlannedCheckBatchItem {
                index,
                name: plan.name.clone(),
                input_hash: input_hash.clone(),
                resource_identity: CheckResourceIdentityInput::Configured {
                    name: plan.name.clone(),
                    check: checks
                        .get(&plan.name)
                        .expect("planned check must retain its configured definition")
                        .clone(),
                },
                command: plan.command.clone(),
                stream_id: crate::mcp::handlers::run::check_stream_id(&checks_tool_id, index),
                env: extra_env.clone(),
                verdict_environment_names: plan.verdict_environment_names.clone(),
                timeout_ms: timeouts[index].into(),
                executor: checks
                    .get(&plan.name)
                    .and_then(|check| check.executor.clone()),
                verdict_platforms: checks
                    .get(&plan.name)
                    .map(crate::execution::check_identity::verdict_platforms)
                    .unwrap_or_default(),
                resource_class: plan.resource_class,
            }
        })
        .collect();
    let batch = PlannedCheckBatchRequest {
        project_id: coords.project_id.clone(),
        repository: canonical_repo,
        store_dir,
        sealed_commit: sealed_commit.clone(),
        requesting_job_id: job_id.to_string(),
        owner: cairn_common::executor_protocol::CellOwnerRef {
            project_id: coords.project_id.clone(),
            project_key: Some(coords.project_key.clone()),
            issue_number: Some(coords.number),
            job_id: Some(job_id.to_string()),
            execution_seq: Some(coords.exec_seq),
            node_kind: Some(coords.node_segment.clone()),
        },
        affinity_key: Some(job_id.to_string()),
        priority: CellPriority::ReviewCheck,
        env: extra_env,
        items,
        run_context: None,
        mutation_policy: crate::fleet::MutationPolicy::PureVerdict,
        status_board: None,
    };
    // The last decision before the machine is asked for anything. Everything
    // since the first check took minutes, and a merge landing inside that window
    // is exactly the specimen this closes: the wave withdraws here having taken
    // no admission, rather than composing a red infrastructure check out of a
    // teardown Cairn performed itself.
    if let WaveLaunchability::Withdrawn(withdrawal) = turn_end_launchability(orch, job_id).await {
        log::info!(
            "turn-end checks for job {}: {}; withdrawing {} planned check(s) before submission",
            short_id(job_id),
            withdrawal.describe(),
            to_run.len()
        );
        return Ok(());
    }
    let batched_results = tokio::select! {
        biased;
        _ = cancel.cancelled() => {
            log::info!(
                "turn-end checks for job {}: cancelled mid-suite (issue resolved); abandoning {} check(s)",
                short_id(job_id),
                to_run.len()
            );
            if orch.fleet.cancel_job_requests(job_id) > 0 {
                let _ = orch
                    .services
                    .emitter
                    .emit("substrate-health-change", serde_json::json!({}));
            }
            return Ok(());
        }
        results = submit_planned_check_batch(orch, batch) => results
            .map_err(|error| format!("turn-end check batch configuration failed: {error}"))?,
    };
    for (index, result) in &batched_results.results {
        if let Ok(result) = result {
            let _ = std::fs::write(
                turn_end_log_path(orch, job_id, &to_run[*index].0.name),
                &result.output,
            );
        }
    }
    let batched_results = std::sync::Arc::new(std::sync::Mutex::new(batched_results.results));
    let outcomes = run_planned_checks_at_commit(
        db.clone(),
        &coords.project_id,
        crate::execution::checks::CheckRunCommit {
            evaluated: &sealed_commit,
            defined_by: &defined_by_commit,
        },
        &tree_hash,
        job_id,
        &to_run,
        &checks_tool_id,
        CheckExecMode::Shared,
        Some(orch),
        move |index, _command, _stream_id| {
            let batched_results = batched_results.clone();
            async move {
                batched_results
                    .lock()
                    .unwrap()
                    .remove(&index)
                    .unwrap_or_else(|| {
                        Err(crate::execution::checks::CheckExecutionFailure::substrate(
                            crate::execution::checks::SubstrateFailureShape::Result,
                            format!("missing review batch outcome for plan index {index}"),
                        ))
                    })
            }
        },
        |_| {},
    )
    .await;

    let any_failed = outcomes.iter().any(|o| !o.passed);
    // The wake decision keys on GENUINE failures only. An infrastructure failure
    // is a fact about Cairn, not a verdict about this change, so rousing the
    // author for one asks them to fix something they have no standing to touch —
    // and because it re-executes on every evaluation, it asks repeatedly. That is
    // the loop this cadence kept generating (CAIRN-3245).
    let any_genuine_failed = outcomes.iter().any(CheckOutcome::is_genuine_failure);
    let verdicts: Vec<String> = outcomes
        .iter()
        .map(|o| {
            format!(
                "{}={} ({}ms)",
                o.name,
                if o.passed { "pass" } else { "fail" },
                o.duration_ms
            )
        })
        .collect();

    // Nudge any live PR-node / `/checks` view to re-render with the fresh verdicts.
    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "check_result_cache", "action": "update"}),
    );

    log::info!(
        "turn-end checks for job {}: completed \u{2014} [{}] \u{2192} {}",
        short_id(job_id),
        verdicts.join(", "),
        if any_genuine_failed {
            "wake"
        } else {
            "passive"
        }
    );

    // 10. Deliver one push per distinct check state. Red execution evidence is
    // deliberately not reusable, so the attention ledger, not the result cache,
    // owns notification dampening. The stable lane-state key records green/red
    // transitions without treating volatile execution detail as a new state.
    let checks_uri = checks_uri_for_job(&coords);
    let fingerprint = turn_check_fingerprint(&tree_hash, &outcomes);
    let (delivery, effective_wake) = persist_turn_check_delivery(
        &owning,
        job_id,
        &checks_uri,
        fingerprint.as_deref(),
        any_genuine_failed,
    )
    .await?;

    if delivery == TurnCheckDelivery::Suppress {
        log::info!(
            "turn-end checks for job {}: deduped unchanged check state",
            short_id(job_id)
        );
    } else {
        let TurnCheckDelivery::Deliver(decision) = delivery else {
            unreachable!("suppression handled above")
        };
        log::info!(
            "turn-end checks for job {}: delivery decision {}",
            short_id(job_id),
            decision
        );
        if any_genuine_failed && effective_wake == Some(attention_push::Wake::Wake) {
            if let Err(error) = crate::messages::delivery::nudge_job_for_urgency(
                orch,
                job_id,
                crate::messages::queued::DeliveryUrgency::Steer,
            ) {
                log::warn!(
                    "turn-check failure wake for job {} failed: {}",
                    short_id(job_id),
                    error
                );
            }
        }
    }

    if any_failed
        && outcomes.iter().any(|outcome| {
            outcome
                .failure_kind
                .is_some_and(CheckFailureKind::is_infrastructure)
        })
    {
        if let (Some(issue_id), Some(fingerprint)) = (
            issue_id_for_job(&owning, job_id).await,
            fingerprint.as_deref(),
        ) {
            let operator_key = format!("turn-checks-infrastructure:{checks_uri}");
            match crate::orchestrator::parent_wake::queue_passive_parent_push(
                &owning,
                &issue_id,
                &checks_uri,
                &operator_key,
                fingerprint,
            )
            .await
            {
                Ok(true) => log::info!(
                    "turn-end checks for job {}: queued passive infrastructure signal",
                    short_id(job_id)
                ),
                Ok(false) => log::debug!(
                    "turn-end checks for job {}: infrastructure signal deduped or has no parent route",
                    short_id(job_id)
                ),
                Err(error) => log::warn!(
                    "turn-end checks for job {}: failed to route infrastructure signal: {}",
                    short_id(job_id),
                    error
                ),
            }
        } else {
            log::warn!(
                "turn-end checks for job {}: infrastructure signal fingerprint or parent issue unavailable",
                short_id(job_id)
            );
        }
    }
    Ok(())
}

const TURN_CHECK_FINGERPRINT_VERSION: &str = "turn-check-state-v2";

fn normalized_salient(value: &str) -> Option<String> {
    let normalized = value.replace("\r\n", "\n").replace('\r', "\n");
    let trimmed = normalized.trim();
    if trimmed.is_empty() || trimmed.contains('\0') {
        return None;
    }
    Some(trimmed.to_string())
}

fn turn_check_fingerprint(tree_hash: &str, outcomes: &[CheckOutcome]) -> Option<String> {
    let tree_hash = normalized_salient(tree_hash)?;
    if outcomes.is_empty() {
        return None;
    }
    let mut canonical = Vec::with_capacity(outcomes.len());
    for outcome in outcomes {
        let name = normalized_salient(&outcome.name)?;
        let kind = if outcome.passed {
            if outcome.failure_kind.is_some() {
                return None;
            }
            "pass".to_string()
        } else {
            outcome
                .failure_kind
                .map(|kind| kind.as_str().to_string())
                .unwrap_or_else(|| "ordinary_failure".to_string())
        };
        canonical.push((name, outcome.passed, kind));
    }
    canonical.sort_by(|left, right| left.0.cmp(&right.0));
    let payload =
        serde_json::to_vec(&(TURN_CHECK_FINGERPRINT_VERSION, tree_hash, canonical)).ok()?;
    Some(format!("sha256:{:x}", Sha256::digest(payload)))
}

/// The wake level a completed turn-end run is delivered at: a GENUINE failure
/// ROUSES the idle builder (`Wake`), while a clean run — and a run whose only red
/// is Cairn's own infrastructure — rides along `Passive`, delivered and recorded
/// without costing a turn. Pure, so the decision is unit-tested.
fn delivery_wake(any_genuine_failed: bool) -> attention_push::Wake {
    if any_genuine_failed {
        attention_push::Wake::Wake
    } else {
        attention_push::Wake::Passive
    }
}

/// First 8 chars of a job id for log lines (mirrors the ids elsewhere in this
/// module), clamped so a short id never panics.
fn short_id(job_id: &str) -> &str {
    &job_id[..job_id.len().min(8)]
}

/// Render the `### Systematic checks` section for a node job: the "running" live
/// log tail while a suite is in flight, plus the cached per-check verdicts for the
/// node's current sealed tree. Returns `None` when there is nothing to show (no
/// resolvable worktree/tree, and neither a running suite nor any cached verdict) —
/// callers omit the section entirely. Shared by the PR-node view and the `/checks`
/// read projection.
pub(crate) async fn render_turn_end_checks_section(
    orch: &Orchestrator,
    job_id: &str,
) -> Option<String> {
    let statuses = crate::execution::checks_status::node_check_statuses(orch, job_id).await?;
    format_checks_section(&statuses)
}

/// Pure renderer for the `### Systematic checks` section. Returns `None` when the
/// project has no configured checks. Split out so every status renders without a
/// DB or worktree.
fn format_checks_section(
    statuses: &[crate::execution::checks_status::NodeCheckStatus],
) -> Option<String> {
    use crate::execution::checks_status::{
        format_status_annotation, formatted_failure_names, NodeCheckState,
    };
    if statuses.is_empty() {
        return None;
    }
    let mut out = String::from("\n### Systematic checks\n\n");
    for status in statuses {
        match status.state {
            NodeCheckState::Passed => {
                let annotation = format_status_annotation(status)
                    .map(|a| format!(" ({a})"))
                    .unwrap_or_default();
                out.push_str(&format!("- \u{2713} {}{annotation}\n", status.name));
            }
            NodeCheckState::Failed => {
                let annotation = format_status_annotation(status)
                    .map(|a| format!(" \u{2014} {a}"))
                    .or_else(|| formatted_failure_names(status).map(|n| format!(" \u{2014} {n}")))
                    .unwrap_or_default();
                out.push_str(&format!("- \u{2717} {}{annotation}\n", status.name));
                if let Some(detail) = status
                    .output_tail
                    .as_deref()
                    .filter(|s| !s.trim().is_empty())
                {
                    out.push_str("\n```\n");
                    out.push_str(detail.trim_end());
                    out.push_str("\n```\n");
                }
            }
            NodeCheckState::Running => {
                out.push_str(&format!("- {}: _running\u{2026}_\n", status.name));
                if let Some(tail) = status
                    .output_tail
                    .as_deref()
                    .filter(|t| !t.trim().is_empty())
                {
                    out.push_str("\n```\n");
                    out.push_str(tail.trim_end());
                    out.push_str("\n```\n");
                }
            }
            NodeCheckState::Pending => out.push_str(&format!("- {}: pending\n", status.name)),
            NodeCheckState::NotApplicable => {
                out.push_str(&format!("- {}: not applicable\n", status.name));
            }
        }
    }
    Some(out)
}

/// The node's coordinates resolved from a `job_id` in one query.
pub(crate) struct JobCoords {
    pub(crate) project_id: String,
    pub(crate) repository_path: String,
    pub(crate) branch: String,
    pub(crate) base_branch: Option<String>,
    pub(crate) project_key: String,
    pub(crate) number: i32,
    pub(crate) exec_seq: i32,
    pub(crate) node_segment: String,
    parent_segment: Option<String>,
    is_workflow: bool,
}

/// Whether a cache-filtered turn-end plan set may launch, and the plans if so.
///
/// `Launch` is the ONLY variant that carries plans, and the plan set is the only
/// input to the batch items the cadence submits. That is the point of this type:
/// a cell request cannot be constructed without first going through
/// [`turn_end_launch`], so no future edit can accidentally submit past a skip.
#[derive(Debug, PartialEq, Eq)]
enum TurnEndLaunch {
    /// Submit these plans. Non-empty by construction.
    Launch(Vec<(CheckPlan, String)>),
    /// Run nothing and take no admission.
    Skip(TurnEndSkip),
}

#[derive(Debug, PartialEq, Eq)]
enum TurnEndSkip {
    /// Every applicable check already has a verdict for this exact tree.
    FullyCached,
    /// The node's sealed tree is byte-identical to its base's. Carries the names
    /// that would otherwise have run, for the log.
    TreeMatchesBase(Vec<String>),
}

/// Decide whether a cache-filtered turn-end plan set may take slot admission.
///
/// A node whose sealed tree is byte-identical to its base's changed nothing by
/// construction, so no check run against it could produce a verdict about the
/// node rather than about the base. Such a node runs nothing at all — the point
/// is that it takes NO admission, so the decision has to be made here, above
/// submission, rather than discovered after a slot has already been leased.
///
/// The tree check is evaluated even when uncached plans remain, which is the
/// whole case that matters: a stale base coordinate makes the changed-file gate
/// upstream select real checks for a node that changed nothing (CAIRN-3108).
/// Deciding only on `uncached.is_empty()` is exactly the bug.
///
/// Verdicts already cached for this tree are re-stamped onto the job by the
/// cache-filtering pass that produces `uncached`, so that inheritance has
/// necessarily already happened before this is called: a skipped zero-delta node
/// still renders the base's results rather than an empty checklist. Whatever is
/// left uncovered stays uncovered — a check that has never run on this tree is
/// the base's business, not this node's.
fn turn_end_launch(uncached: Vec<(CheckPlan, String)>, tree_matches_base: bool) -> TurnEndLaunch {
    if uncached.is_empty() {
        return TurnEndLaunch::Skip(TurnEndSkip::FullyCached);
    }
    if tree_matches_base {
        return TurnEndLaunch::Skip(TurnEndSkip::TreeMatchesBase(
            uncached
                .into_iter()
                .map(|(plan, _)| plan.name)
                .collect::<Vec<_>>(),
        ));
    }
    TurnEndLaunch::Launch(uncached)
}

/// The coordinate whose TREE defines "unchanged" for this node.
///
/// This is the integration target's CURRENT tip, in deliberate contrast to the
/// changed-file diff in `run_turn_end_checks_inner`, which wants the node's own
/// delta and so starts at its fork point. A node whose sealed tree equals the
/// tree the base branch holds right now changed nothing that anyone will review,
/// whatever its history looks like.
///
/// A branch name resolves fresh from the store on every evaluation, so it cannot
/// go stale the way `jobs.base_commit` does, and comparing trees rather than
/// coordinates makes the answer a fact about content rather than about
/// bookkeeping (CAIRN-3108).
fn base_tree_coordinate(coords: &JobCoords) -> Option<&str> {
    coords.base_branch.as_deref()
}

pub(crate) fn checks_uri_for_job(coords: &JobCoords) -> String {
    match (coords.is_workflow, coords.parent_segment.as_deref()) {
        (true, _) => build_node_checks_uri(
            &coords.project_key,
            coords.number,
            coords.exec_seq,
            &coords.node_segment,
        ),
        (false, Some(parent)) => build_task_checks_uri(
            &coords.project_key,
            coords.number,
            coords.exec_seq,
            parent,
            &coords.node_segment,
        ),
        (false, None) => build_node_checks_uri(
            &coords.project_key,
            coords.number,
            coords.exec_seq,
            &coords.node_segment,
        ),
    }
}

/// Resolve the durable project, branch, base anchors, and URI identity for a job.
pub(crate) async fn resolve_job_coords(
    db: &LocalDb,
    job_id: &str,
) -> Result<Option<JobCoords>, String> {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT j.project_id, p.repo_path, j.branch, j.base_branch,
                            p.key, i.number, e.seq, j.uri_segment,
                            parent.uri_segment, j.agent_config_id
                     FROM jobs j
                     JOIN projects p ON p.id = j.project_id
                     JOIN issues i ON i.id = j.issue_id
                     JOIN executions e ON e.id = j.execution_id
                     LEFT JOIN jobs parent ON j.parent_job_id = parent.id
                     WHERE j.id = ?1 LIMIT 1",
                    (job_id.as_str(),),
                )
                .await?;
            match rows.next().await? {
                Some(row) => Ok(Some(JobCoords {
                    project_id: row.text(0)?,
                    repository_path: row.text(1)?,
                    branch: row.opt_text(2)?.filter(|s| !s.is_empty()).ok_or_else(|| {
                        crate::storage::DbError::Row(format!("job {job_id} has no branch"))
                    })?,
                    base_branch: row.opt_text(3)?.filter(|s| !s.is_empty()),
                    project_key: row.text(4)?,
                    number: row.i64(5)? as i32,
                    exec_seq: row.i64(6)? as i32,
                    node_segment: row.opt_text(7)?.unwrap_or_default(),
                    parent_segment: row.opt_text(8)?,
                    is_workflow: row.opt_text(9)?.as_deref() == Some("workflow"),
                })),
                None => Ok(None),
            }
        })
    })
    .await
    .map_err(|e| format!("failed to resolve job coords: {e}"))
}

/// The host-readable, job-scoped directory holding ONE live log file per check
/// for a turn-end run. Lives under the app state dir (not the worktree) so it
/// survives worktree teardown for the PR-node render.
fn turn_end_log_dir(orch: &Orchestrator, job_id: &str) -> PathBuf {
    orch.config_dir.join("turn-checks").join(job_id)
}

/// The live log file for a SINGLE check within a job's turn-end run. Each check
/// tees into its OWN file (created the instant it starts), so the PR-node /
/// `/checks` render can tail exactly that check's output — several may be running
/// and tailing at once under concurrent isolation — instead of a shared blob that
/// made every running check preview the same interleaved text.
fn turn_end_log_path(orch: &Orchestrator, job_id: &str, check_name: &str) -> PathBuf {
    turn_end_log_dir(orch, job_id).join(format!("{}.log", sanitize_log_name(check_name)))
}

/// Slugify a check name into a filesystem-safe log filename stem: any character
/// outside `[A-Za-z0-9._-]` becomes `_`. Real check names are already slugs, so
/// this only guards against pathological config.
fn sanitize_log_name(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// Clear the job's per-check log directory so a fresh suite starts clean — a stale
/// per-check log must not make a not-yet-started check look like it is running
/// with old output. Best-effort: a failure here only costs the live tail, never
/// the run.
fn prepare_log_dir(dir: &Path) {
    let _ = std::fs::remove_dir_all(dir);
    let _ = std::fs::create_dir_all(dir);
}

/// Whether a single check's per-check log file exists yet. The runner creates the
/// file the instant a check starts — before any output — so existence marks the
/// check as actively RUNNING even while it is still silent (e.g. `tsc --noEmit`
/// before its first line). A queued check has no file after `prepare_log_dir`
/// cleared the directory, so it reads as pending instead.
pub(crate) fn turn_end_check_started(orch: &Orchestrator, job_id: &str, check_name: &str) -> bool {
    turn_end_log_path(orch, job_id, check_name).exists()
}

/// Last `max_chars` chars of a single check's live log file, or `None` when it is
/// missing/empty (that check exists but has not produced output yet). Existence is
/// a SEPARATE signal ([`turn_end_check_started`]): a running-but-silent check has
/// a file with no tail, so callers must not infer "queued" from a `None` tail.
pub(crate) fn read_turn_end_log_tail(
    orch: &Orchestrator,
    job_id: &str,
    check_name: &str,
) -> Option<String> {
    read_log_tail(&turn_end_log_path(orch, job_id, check_name), LOG_TAIL_CHARS)
}

/// Last `max_chars` chars of a log file at `path`, or `None` when it is missing or
/// blank. Reads only enough bytes from the end to hold that many UTF-8 characters,
/// so polling a multi-megabyte cargo or vitest log stays constant-cost.
///
/// Split from [`read_turn_end_log_tail`] so the missing/empty-vs-content boundary
/// and large-file behavior are unit-testable without an [`Orchestrator`].
fn read_log_tail(path: &Path, max_chars: usize) -> Option<String> {
    if max_chars == 0 {
        return None;
    }
    let mut file = std::fs::File::open(path).ok()?;
    let len = file.metadata().ok()?.len();
    let max_bytes = max_chars.saturating_mul(4) as u64;
    let start = len.saturating_sub(max_bytes);
    file.seek(SeekFrom::Start(start)).ok()?;

    let mut bytes = Vec::with_capacity((len - start) as usize);
    file.read_to_end(&mut bytes).ok()?;
    // A concurrent writer can leave the sampled suffix on a partial UTF-8 code
    // point. Lossy decoding preserves the useful tail instead of dropping the
    // whole update; the next poll replaces any transient replacement character.
    let content = String::from_utf8_lossy(&bytes);
    let trimmed = content.trim_end();
    if trimmed.is_empty() {
        return None;
    }
    Some(tail(trimmed, max_chars))
}

/// Last `max_chars` characters of `s`, on a char boundary.
fn tail(s: &str, max_chars: usize) -> String {
    let count = s.chars().count();
    if count <= max_chars {
        return s.to_string();
    }
    s.chars().skip(count - max_chars).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::checks_status::{NodeCheckState, NodeCheckStatus};
    use cairn_common::executor_protocol::{
        CellCheckoutKind, CellOccupancy, CellResidency, OwnerDeathPolicy, PersistentCellLifecycle,
        PersistentCellState, RepositoryLocator, ResidencyFootprint, ResidencyHolder,
        ResidencyPhase,
    };

    fn cell_held_by(holder: ResidencyHolder, phase: ResidencyPhase) -> PersistentCellState {
        PersistentCellState {
            warm_command_classes: Vec::new(),
            executor_id: "executor".into(),
            executor_display_name: None,
            project_id: "p".into(),
            cell_id: "cell-1".into(),
            path: "/cell".into(),
            workspace_name: "workspace".into(),
            repository: "/repo".into(),
            checkout_kind: CellCheckoutKind::default(),
            git_common_dir: None,
            authority_path: String::new(),
            lifecycle: PersistentCellLifecycle::Idle,
            cell_epoch: 1,
            last_sealed_commit: None,
            last_used_unix_ms: 0,
            last_affinity_key: None,
            preparation_fingerprint: None,
            residency: Some(CellResidency {
                holder,
                repository: RepositoryLocator::ColocatedPath {
                    project_id: "p".into(),
                    repository_id: "p".into(),
                    absolute_path: "/repo".into(),
                },
                owner_ref: None,
                selector: None,
                incarnation_id: "inc".into(),
                current_base_commit: "base".into(),
                phase,
                last_heartbeat_unix_ms: 0,
                reclaim_deadline_unix_ms: 0,
                death_policy: OwnerDeathPolicy {
                    heartbeat_timeout_ms: 1_000,
                    reclaim_grace_ms: 1_000,
                },
                footprint: ResidencyFootprint::default(),
                state_revision: 1,
                events: Vec::new(),
            }),
            occupancy: CellOccupancy::default(),
        }
    }

    /// A wave checks the workspace its job works in, so a workspace being taken
    /// apart is a reason not to launch one — and only the job's OWN residency
    /// speaks to that. Another job's teardown, a dev instance's, and a healthy
    /// residency are all silence.
    #[test]
    fn only_this_jobs_own_reclamation_withholds_its_wave() {
        let job = |id: &str| ResidencyHolder::Job { job_id: id.into() };
        for phase in [ResidencyPhase::AwaitingReclaim, ResidencyPhase::Releasing] {
            assert_eq!(
                reclaiming_owner_phase(&[cell_held_by(job("j-1"), phase)], "j-1"),
                Some(phase)
            );
        }
        assert_eq!(
            reclaiming_owner_phase(&[cell_held_by(job("j-1"), ResidencyPhase::Active)], "j-1"),
            None,
            "a live residency is exactly what a wave needs"
        );
        assert_eq!(
            reclaiming_owner_phase(
                &[cell_held_by(job("j-2"), ResidencyPhase::AwaitingReclaim)],
                "j-1"
            ),
            None,
            "another job's teardown is not this job's business"
        );
        assert_eq!(
            reclaiming_owner_phase(
                &[cell_held_by(
                    ResidencyHolder::DevInstance {
                        instance_id: "j-1".into()
                    },
                    ResidencyPhase::AwaitingReclaim
                )],
                "j-1"
            ),
            None,
            "a holder of another kind never answers for a job, whatever its id reads like"
        );
        assert_eq!(reclaiming_owner_phase(&[], "j-1"), None);
    }

    /// Each withdrawal says which condition it was, because "nothing to run" in
    /// a log with no reason attached is what sent operators to the substrate
    /// records to reconstruct it.
    #[test]
    fn every_withdrawal_names_its_own_condition() {
        let described = [
            WaveWithdrawal::IssueResolved("merged".into()),
            WaveWithdrawal::JobCancelled,
            WaveWithdrawal::OwnerReclaiming(ResidencyPhase::AwaitingReclaim),
            WaveWithdrawal::JobUnknown,
        ]
        .map(|withdrawal| withdrawal.describe());
        assert!(described[0].contains("merged"));
        assert!(described[1].contains("cancelled"));
        assert!(described[2].contains("reclaim"));
        assert_eq!(
            described
                .iter()
                .collect::<std::collections::HashSet<_>>()
                .len(),
            described.len(),
            "one sentence per condition"
        );
    }

    fn outcome(
        name: &str,
        passed: bool,
        kind: Option<CheckFailureKind>,
        output: &str,
    ) -> CheckOutcome {
        CheckOutcome {
            name: name.to_string(),
            passed,
            exit_code: if passed { Some(0) } else { Some(1) },
            failure_kind: kind,
            parsed: None,
            output_tail: output.to_string(),
            cached: false,
            recorded: None,
            duration_ms: 1,
            suppressed_after: None,
            not_recorded: None,
        }
    }

    fn status(name: &str, state: NodeCheckState) -> NodeCheckStatus {
        NodeCheckStatus {
            job_id: "job".to_string(),
            request_id: None,
            name: name.to_string(),
            state,
            policy: "advisory".to_string(),
            when: "write".to_string(),
            cached: None,
            duration_ms: Some(1234),
            ran_at: Some(1),
            passed: None,
            failed: None,
            skipped: None,
            suite_failures: None,
            failure_names: Vec::new(),
            output_tail: None,
            failure_kind: None,
            suppressed_after: None,
        }
    }

    #[test]
    fn fingerprint_is_order_independent_and_state_sensitive() {
        let rust = outcome("rust", false, None, "assertion A");
        let lint = outcome("lint", true, None, "");
        let first = turn_check_fingerprint("tree", &[rust, lint]).unwrap();
        let second = turn_check_fingerprint(
            "tree",
            &[
                outcome("lint", true, None, ""),
                outcome("rust", false, None, "assertion A"),
            ],
        )
        .unwrap();
        assert_eq!(first, second);
        // Failure text is execution noise, not stable evidence about a lane state.
        assert_eq!(
            first,
            turn_check_fingerprint(
                "tree",
                &[
                    outcome("lint", true, None, ""),
                    outcome("rust", false, None, "assertion B"),
                ],
            )
            .unwrap()
        );
        assert_ne!(
            first,
            turn_check_fingerprint("tree-2", &[outcome("rust", false, None, "assertion A")])
                .unwrap()
        );
        assert_ne!(
            first,
            turn_check_fingerprint("tree", &[outcome("rust", false, None, "assertion A")]).unwrap()
        );
    }

    #[test]
    fn fingerprint_distinguishes_green_ordinary_and_infrastructure_red() {
        let green = turn_check_fingerprint("tree", &[outcome("rust", true, None, "")]).unwrap();
        let red = turn_check_fingerprint("tree", &[outcome("rust", false, None, "same")]).unwrap();
        let infrastructure = turn_check_fingerprint(
            "tree",
            &[outcome(
                "rust",
                false,
                Some(CheckFailureKind::Infrastructure),
                "same",
            )],
        )
        .unwrap();
        assert_ne!(green, red);
        assert_ne!(red, infrastructure);
    }

    #[test]
    fn fingerprint_fails_open_when_required_identity_is_ambiguous() {
        assert!(turn_check_fingerprint("", &[outcome("rust", true, None, "")]).is_none());
        assert!(turn_check_fingerprint("tree", &[]).is_none());
    }

    #[test]
    fn fingerprint_ignores_real_wave_execution_noise_on_the_same_tree() {
        // CAIRN-3515 produced these two rust-test shapes on the same tree: the
        // selected failures, progress tail, and sealed commit all changed while
        // the lane-level verdict set did not.
        let first = [
            outcome("lint", true, None, ""),
            outcome(
                "rust-tests",
                false,
                None,
                "PASS [1.147s] (1871/3246)\nproject_checkout_validation_rejects_disposable_and_missing_paths",
            ),
        ];
        let second = [
            outcome("lint", true, None, ""),
            outcome(
                "rust-tests",
                false,
                None,
                "a_full_machine_makes_a_batch_slower_not_broken\noutput_wake_fires_when_process_exits_before_phrase",
            ),
        ];
        assert_eq!(
            turn_check_fingerprint("2670caaa55", &first),
            turn_check_fingerprint("2670caaa55", &second)
        );
    }

    #[test]
    fn fingerprint_moves_when_a_lane_appears_or_disappears() {
        let rust = outcome("rust", false, None, "failure");
        let with_lint = [
            outcome("rust", false, None, "different failure"),
            outcome("lint", true, None, ""),
        ];
        assert_ne!(
            turn_check_fingerprint("tree", &[rust]),
            turn_check_fingerprint("tree", &with_lint)
        );
    }

    #[test]
    fn delivery_decision_dampens_equal_red_and_green_states() {
        assert_eq!(
            turn_check_delivery(None, Some("red"), true),
            TurnCheckDelivery::Deliver("new")
        );
        assert_eq!(
            turn_check_delivery(Some(Some("red")), Some("red"), true),
            TurnCheckDelivery::Suppress
        );
        assert_eq!(
            turn_check_delivery(Some(Some("green")), Some("green"), false),
            TurnCheckDelivery::Suppress
        );
        assert_eq!(
            turn_check_delivery(Some(Some("red")), Some("changed"), true),
            TurnCheckDelivery::Deliver("changed")
        );
        assert_eq!(
            turn_check_delivery(Some(Some("red")), None, true),
            TurnCheckDelivery::Deliver("ambiguous/fail-open")
        );
        assert_eq!(
            turn_check_delivery(None, Some("green"), false),
            TurnCheckDelivery::Deliver("green")
        );
    }

    #[tokio::test]
    async fn unchanged_persistent_red_creates_one_turn_checks_wake() {
        let db = crate::storage::migrated_test_db("turn-check-delivery.db").await;
        db.execute_script(
            "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at)
               VALUES('p','w','P','PROJ','/tmp/repo',1,1);
             INSERT INTO issues(id,project_id,number,title,status,progress,attention,created_at,updated_at)
               VALUES('i','p',1,'I','active','active','none',1,1);
             INSERT INTO jobs(id,project_id,issue_id,status,node_name,created_at,updated_at)
               VALUES('job','p','i','complete','builder',1,1);",
        )
        .await
        .unwrap();
        let uri = "cairn://p/PROJ/1/1/builder/checks";
        let first = turn_check_fingerprint(
            "same-tree",
            &[outcome("rust-tests", false, None, "first flaky failure")],
        );
        let second = turn_check_fingerprint(
            "same-tree",
            &[outcome(
                "rust-tests",
                false,
                None,
                "different flaky failures and tail",
            )],
        );

        assert_eq!(
            persist_turn_check_delivery(&db, "job", uri, first.as_deref(), true)
                .await
                .unwrap(),
            (
                TurnCheckDelivery::Deliver("new"),
                Some(attention_push::Wake::Wake)
            )
        );
        assert_eq!(
            persist_turn_check_delivery(&db, "job", uri, second.as_deref(), true)
                .await
                .unwrap(),
            (TurnCheckDelivery::Suppress, None)
        );
        let pushes = attention_push::list_pending(&db, "job").await.unwrap();
        assert_eq!(pushes.len(), 1);
        assert_eq!(pushes[0].key, format!("turn-checks:{uri}"));
        assert_eq!(pushes[0].wake, attention_push::Wake::Wake);
    }

    #[test]
    fn section_is_none_when_no_configured_checks() {
        assert!(format_checks_section(&[]).is_none());
    }

    #[test]
    fn section_renders_running_with_log_tail() {
        let mut running = status("rust", NodeCheckState::Running);
        running.output_tail = Some("compiling...\nrunning tests".to_string());
        let s = format_checks_section(&[running]).unwrap();
        assert!(s.contains("### Systematic checks"));
        assert!(s.contains("rust: _running\u{2026}_"));
        assert!(s.contains("running tests"));
    }

    #[test]
    fn section_renders_running_without_a_log_yet() {
        let s = format_checks_section(&[status("rust", NodeCheckState::Running)]).unwrap();
        assert!(s.contains("_running\u{2026}_"));
        assert!(!s.contains("```"));
    }

    #[test]
    fn section_renders_cached_verdicts_and_inlines_failure_output() {
        let mut passed = status("rust", NodeCheckState::Passed);
        passed.passed = Some(12);
        passed.failed = Some(0);
        let mut failed = status("frontend", NodeCheckState::Failed);
        failed.failed = Some(2);
        failed.passed = Some(38);
        failed.output_tail = Some("assertion failed: left == right".to_string());
        let s = format_checks_section(&[passed, failed]).unwrap();
        assert!(s.contains("\u{2713} rust (12 tests)"));
        assert!(s.contains("\u{2717} frontend \u{2014} 2 of 40 failed"));
        assert!(s.contains("assertion failed: left == right"));
    }

    #[test]
    fn section_names_a_suite_collection_failure_instead_of_a_zero_tally() {
        // The durable "Systematic checks" surface reads from the cache, so it is
        // where a dropped suite count would resurrect "0 of 881 failed" long
        // after the run that produced it.
        let mut failed = status("frontend-partial", NodeCheckState::Failed);
        failed.passed = Some(881);
        failed.failed = Some(0);
        failed.suite_failures = Some(1);
        failed.failure_names = vec!["src/components/FileTabView.test.tsx".to_string()];
        failed.output_tail = Some(
            "src/components/FileTabView.test.tsx: Cannot find module './readableMarkdown'"
                .to_string(),
        );
        let s = format_checks_section(&[failed]).unwrap();
        assert!(
            s.contains("\u{2717} frontend-partial \u{2014} 1 suite failed to load"),
            "got: {s}"
        );
        assert!(!s.contains("0 of 881 failed"), "got: {s}");
        // The file that failed to load is still named, in the detail block.
        assert!(
            s.contains("src/components/FileTabView.test.tsx"),
            "got: {s}"
        );
    }

    #[test]
    fn section_counts_failing_tests_and_uncollected_suites_separately() {
        let mut failed = status("frontend-partial", NodeCheckState::Failed);
        failed.passed = Some(38);
        failed.failed = Some(2);
        failed.suite_failures = Some(3);
        let s = format_checks_section(&[failed]).unwrap();
        assert!(
            s.contains(
                "\u{2717} frontend-partial \u{2014} 2 of 40 failed, 3 suites failed to load"
            ),
            "got: {s}"
        );
    }

    #[test]
    fn section_renders_not_run_states() {
        let s = format_checks_section(&[
            status("docs", NodeCheckState::NotApplicable),
            status("lint", NodeCheckState::Pending),
        ])
        .unwrap();
        assert!(s.contains("docs: not applicable"));
        assert!(s.contains("lint: pending"));
    }

    #[test]
    fn empty_log_file_exists_but_yields_no_tail() {
        // A running-but-silent check: the file exists (started) but its tail is
        // None until it emits. The status model must key RUNNING off existence,
        // not off a non-empty tail, or a quiet check looks queued.
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir =
            std::env::temp_dir().join(format!("cairn-checks-tail-{}-{nanos}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("frontend-build.log");

        std::fs::write(&path, b"").unwrap();
        assert!(path.exists());
        assert_eq!(read_log_tail(&path, LOG_TAIL_CHARS), None);

        std::fs::write(&path, b"compiling...\n").unwrap();
        assert_eq!(
            read_log_tail(&path, LOG_TAIL_CHARS).as_deref(),
            Some("compiling...")
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn large_log_tail_is_bounded_and_fast() {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "cairn-checks-tail-large-{}-{nanos}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("rust-full.log");
        let mut file = std::fs::File::create(&path).unwrap();
        let chunk = vec![b'x'; 1024 * 1024];
        for _ in 0..16 {
            std::io::Write::write_all(&mut file, &chunk).unwrap();
        }
        std::io::Write::write_all(&mut file, b"\nfinal cargo line\n").unwrap();
        drop(file);

        let started = std::time::Instant::now();
        let output = read_log_tail(&path, LOG_TAIL_CHARS).unwrap();
        let elapsed = started.elapsed();

        assert!(output.ends_with("final cargo line"));
        assert_eq!(output.chars().count(), LOG_TAIL_CHARS);
        assert!(
            elapsed < std::time::Duration::from_millis(50),
            "16 MiB log tail took {elapsed:?}; expected a bounded low-tens-of-ms read"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn log_tail_preserves_multibyte_utf8_boundary() {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "cairn-checks-tail-utf8-{}-{nanos}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("frontend.log");
        std::fs::write(&path, format!("{}DONE\n", "é".repeat(3_000))).unwrap();

        let output = read_log_tail(&path, 8).unwrap();
        assert_eq!(output, "ééééDONE");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn sanitize_log_name_slugs_unsafe_chars() {
        assert_eq!(sanitize_log_name("frontend-build"), "frontend-build");
        assert_eq!(sanitize_log_name("rust_full.v2"), "rust_full.v2");
        assert_eq!(sanitize_log_name("weird/name space"), "weird_name_space");
    }

    #[test]
    fn tail_keeps_last_chars_on_boundary() {
        assert_eq!(tail("abcdef", 3), "def");
        assert_eq!(tail("abc", 10), "abc");
    }

    #[test]
    fn checks_uri_uses_canonical_job_shape() {
        let mut coords = JobCoords {
            project_id: "p".into(),
            repository_path: "/repo".into(),
            branch: "agent/test".into(),
            base_branch: None,
            project_key: "CAIRN".into(),
            number: 42,
            exec_seq: 2,
            node_segment: "review-rust".into(),
            parent_segment: Some("builder".into()),
            is_workflow: false,
        };
        assert_eq!(
            checks_uri_for_job(&coords),
            "cairn://p/CAIRN/42/2/builder/task/review-rust/checks"
        );

        coords.node_segment = "workflow".into();
        coords.parent_segment = Some("coordinator".into());
        coords.is_workflow = true;
        assert_eq!(
            checks_uri_for_job(&coords),
            "cairn://p/CAIRN/42/2/workflow/checks"
        );

        coords.node_segment = "builder".into();
        coords.parent_segment = None;
        coords.is_workflow = false;
        assert_eq!(
            checks_uri_for_job(&coords),
            "cairn://p/CAIRN/42/2/builder/checks"
        );
    }

    /// The zero-delta gate resolves a branch NAME, never a recorded commit.
    ///
    /// `jobs.base_commit` is the row that goes stale when the base-advance
    /// fanout loses its compare-and-swap race, and gating a rebased zero-delta
    /// branch on a stale coordinate is what fired full review suites on planner
    /// nodes (CAIRN-3108). `JobCoords` no longer carries that row at all, so
    /// this pins the only remaining input: a name the store resolves fresh on
    /// every evaluation.
    #[test]
    fn the_zero_delta_gate_resolves_a_branch_name_not_a_recorded_commit() {
        let mut coords = JobCoords {
            project_id: "p".into(),
            repository_path: "/repo".into(),
            branch: "agent/test".into(),
            base_branch: Some("main".into()),
            project_key: "CAIRN".into(),
            number: 42,
            exec_seq: 1,
            node_segment: "planner".into(),
            parent_segment: None,
            is_workflow: false,
        };
        assert_eq!(base_tree_coordinate(&coords), Some("main"));

        // No integration target is no gate: the suite runs rather than skipping
        // on a coordinate nobody resolved.
        coords.base_branch = None;
        assert_eq!(base_tree_coordinate(&coords), None);
    }

    fn uncached_plan(name: &str) -> (CheckPlan, String) {
        (
            CheckPlan {
                name: name.to_string(),
                applies: true,
                command: format!("bun run {name}"),
                scope: crate::execution::selection::CheckScope::Full,
                resource_class: crate::config::project_settings::CheckResourceClass::Shared,
                verdict_environment_names: Vec::new(),
                config_error: None,
                verdict_platforms: Vec::new(),
            },
            format!("input-{name}"),
        )
    }

    /// The regression this exists for. A planner node's branch is rebased onto a
    /// new base, so its tree is byte-identical to that base's, but its recorded
    /// `base_commit` still points at the old one. The changed-file gate upstream
    /// therefore selects real, uncached review checks for a node that changed
    /// nothing (job 7d9755b2: seven files, three of them Rust). Nothing may be
    /// submitted for it.
    ///
    /// Note the shape of the assertion: the plan set is NOT empty. A gate that
    /// only refuses to launch when nothing is left uncached is exactly the bug,
    /// and would pass a test that fed it an empty plan set.
    #[test]
    fn a_tree_identical_to_its_base_launches_nothing_even_with_uncached_checks() {
        let uncached = vec![
            uncached_plan("rust-full"),
            uncached_plan("rust-lint"),
            uncached_plan("rust-windows-executor-stack"),
        ];

        let decision = turn_end_launch(uncached.clone(), true);

        match decision {
            TurnEndLaunch::Skip(TurnEndSkip::TreeMatchesBase(names)) => assert_eq!(
                names,
                vec!["rust-full", "rust-lint", "rust-windows-executor-stack"],
                "the skip names what it declined to run, for the operator's log"
            ),
            other => panic!("a zero-delta node must take no admission at all, got {other:?}"),
        }

        // Control: the same uncached plans on a node whose tree genuinely differs
        // still launch, unchanged. Without this, a gate that skipped everything
        // would also pass.
        assert_eq!(
            turn_end_launch(uncached.clone(), false),
            TurnEndLaunch::Launch(uncached),
            "a node that actually changed something still runs its checks"
        );
    }

    #[test]
    fn a_fully_cached_tree_launches_nothing() {
        assert_eq!(
            turn_end_launch(Vec::new(), false),
            TurnEndLaunch::Skip(TurnEndSkip::FullyCached)
        );
    }

    #[test]
    fn green_rides_along_passively_red_wakes() {
        assert_eq!(delivery_wake(false), attention_push::Wake::Passive);
        assert_eq!(delivery_wake(true), attention_push::Wake::Wake);
    }

    /// The wake predicate. Only a red the agent's own change owns counts; an
    /// infrastructure failure and a suppression are both facts about Cairn.
    #[test]
    fn only_a_verdict_about_the_change_is_a_genuine_failure() {
        assert!(outcome("rust", false, None, "assertion failed").is_genuine_failure());
        assert!(
            outcome("rust", false, Some(CheckFailureKind::TimedOut), "slow").is_genuine_failure()
        );
        assert!(outcome("rust", false, Some(CheckFailureKind::Killed), "oom").is_genuine_failure());
        assert!(!outcome("rust", true, None, "").is_genuine_failure());
        for kind in [
            CheckFailureKind::Infrastructure,
            CheckFailureKind::SpawnError,
            CheckFailureKind::RunnerError,
        ] {
            assert!(
                !outcome("rust", false, Some(kind), "sccache died").is_genuine_failure(),
                "{kind:?} is a failure inside Cairn, not a verdict"
            );
        }

        let mut suppressed = outcome("rust", false, Some(CheckFailureKind::Infrastructure), "");
        suppressed.suppressed_after = Some(3);
        assert!(!suppressed.is_genuine_failure());
    }

    /// The delivery decision an infrastructure-only suite reaches. The suite IS
    /// red — `any_failed` is true — and it still must not wake anyone.
    #[test]
    fn an_infrastructure_only_suite_is_red_yet_delivers_passively() {
        let outcomes = [
            outcome("lint", true, None, ""),
            outcome(
                "rust-full",
                false,
                Some(CheckFailureKind::Infrastructure),
                "sccache: server startup failed",
            ),
        ];
        assert!(outcomes.iter().any(|o| !o.passed));
        assert!(!outcomes.iter().any(CheckOutcome::is_genuine_failure));
        assert_eq!(
            delivery_wake(outcomes.iter().any(CheckOutcome::is_genuine_failure)),
            attention_push::Wake::Passive
        );
    }

    /// The wake-loop generator, pinned. Two runs of the same broken substrate
    /// produce different diagnostic text — a different pid, a different crate —
    /// and the delivery fingerprint must NOT move, or the dampener never fires
    /// and every re-execution rouses the author again (CAIRN-3245).
    #[test]
    fn infrastructure_flake_text_never_moves_the_fingerprint() {
        let first = turn_check_fingerprint(
            "tree",
            &[outcome(
                "rust-full",
                false,
                Some(CheckFailureKind::Infrastructure),
                "sccache: failed to connect to server at 127.0.0.1:4226 (pid 51201)",
            )],
        )
        .unwrap();
        let second = turn_check_fingerprint(
            "tree",
            &[outcome(
                "rust-full",
                false,
                Some(CheckFailureKind::Infrastructure),
                "sccache: failed to connect to server at 127.0.0.1:4226 (pid 88317)",
            )],
        )
        .unwrap();
        assert_eq!(
            first, second,
            "an infrastructure outcome contributes its kind, never its text"
        );

        // The lane stayed ordinarily red, so different failure text is not a
        // delivery-state change.
        assert_eq!(
            turn_check_fingerprint("tree", &[outcome("rust", false, None, "failure A")]).unwrap(),
            turn_check_fingerprint("tree", &[outcome("rust", false, None, "failure B")]).unwrap(),
        );
    }

    /// The state an infrastructure failure DOES have to distinguish: whether the
    /// genuine verdicts beside it changed. A suite that is green-plus-infra must
    /// not hash the same as one that went genuinely red.
    #[test]
    fn a_genuine_verdict_beside_an_infrastructure_failure_still_moves_the_fingerprint() {
        let infra_only = turn_check_fingerprint(
            "tree",
            &[
                outcome("lint", true, None, ""),
                outcome(
                    "rust-full",
                    false,
                    Some(CheckFailureKind::Infrastructure),
                    "sccache died",
                ),
            ],
        )
        .unwrap();
        let now_genuinely_red = turn_check_fingerprint(
            "tree",
            &[
                outcome("lint", false, None, "clippy: unused import"),
                outcome(
                    "rust-full",
                    false,
                    Some(CheckFailureKind::Infrastructure),
                    "sccache died differently",
                ),
            ],
        )
        .unwrap();
        assert_ne!(infra_only, now_genuinely_red);
    }

    #[test]
    fn short_id_never_panics_on_a_short_string() {
        assert_eq!(short_id("abcd"), "abcd");
        assert_eq!(short_id("0123456789"), "01234567");
    }
}
