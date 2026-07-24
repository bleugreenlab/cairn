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
//! turn-end; `when:write` never runs here (it is the mid-turn cadence).
//! Selection reuses the write cadence's machinery
//! ([`crate::execution::selection::plan_checks`], the impact gate, placeholder
//! substitution) via [`crate::execution::checks::applicable_turn_end_checks`], and
//! results share the `check_result_cache` keyed by each check's input hash.
//!
//! ## Unsandboxed by design
//!
//! At turn-end the agent is idle, so an interactive fence permission prompt would
//! hang with no one to answer. The suite therefore runs UNSANDBOXED
//! ([`crate::mcp::handlers::run::run_check_command_unsandboxed`], `sandbox_enabled=false`),
//! taking the same no-fence path the post-fence-grant re-execution uses. These are
//! trusted, system-driven project-config commands — the identical trust basis as
//! the write cadence.
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

use crate::execution::cache::{get_check_result, store_check_result, CheckResultCacheWrite};
use crate::execution::checks::{
    applicable_turn_end_checks, check_platform_identity, check_resource_identity, check_result_key,
    check_toolchain_identity, load_live_project_checks, resolve_check_timeout_ms,
    run_planned_checks, submit_planned_check_batch, CheckExecMode, CheckFailureKind, CheckOutcome,
    PlannedCheckBatchItem, PlannedCheckBatchRequest, DEFAULT_REVIEW_CHECK_TIMEOUT_MS,
};
use crate::execution::selection::CheckPlan;
use crate::fleet::CellPriority;
use crate::jj::{node_changed_files, sealed_tree_entries, sealed_tree_hash, JjEnv};
use crate::orchestrator::{attention_push, Orchestrator, TurnEndCancel};
use crate::storage::{LocalDb, RowExt};

/// Env var handed to an isolated review check naming a file of newline-delimited
/// changed paths, so `scripts/lib/check-base.ts` can attribute findings to the
/// agent's diff without any VCS metadata in the `.jj`-stripped clone. Must match
/// `CHANGED_FILES_ENV` in that script.
const CHANGED_FILES_ENV: &str = "CAIRN_CHECK_CHANGED_FILES";
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

/// Returns `true` when a non-cached check freshly failed (`fresh_red`).
async fn run_turn_end_checks_inner(
    orch: &Orchestrator,
    job_id: &str,
    cancel: &TurnEndCancel,
) -> Result<bool, String> {
    // 1. Resolve the node's coordinates (project, issue, worktree, base anchors).
    let Some(coords) = resolve_job_coords(&orch.db.local, job_id).await? else {
        return Ok(false);
    };
    // The issue already reached a terminal (merged/closed) state before this suite
    // launched — its verdicts would validate a tree nobody will review again, so
    // return before submitting any slot requests. The mid-flight case (the issue
    // resolving WHILE a check runs) is handled by the `cancel` race around the
    // suite below (CAIRN-2648).
    if matches!(coords.issue_status.as_str(), "merged" | "closed") {
        log::info!(
            "turn-end checks for job {}: issue already {}; nothing to run",
            short_id(job_id),
            coords.issue_status
        );
        return Ok(false);
    }
    let Some(worktree_path) = coords.worktree_path.clone().filter(|p| !p.is_empty()) else {
        log::debug!(
            "turn-end checks for job {}: no worktree; nothing to run",
            short_id(job_id)
        );
        return Ok(false);
    };
    let repo_root = Path::new(&worktree_path);

    // 2. Load the LIVE project checks contract (same source as the write cadence).
    let checks = match load_live_project_checks(orch, &coords.project_id, repo_root).await {
        Some(checks) if !checks.is_empty() => checks,
        _ => {
            log::debug!(
                "turn-end checks for job {}: no checks contract; nothing to run",
                short_id(job_id)
            );
            return Ok(false);
        }
    };

    // 3. Resolve the DB that owns this job (used below to queue the results push).
    let owning = crate::execution::routing::owning_db_for_job(&orch.db, job_id)
        .await
        .map_err(|e| e.to_string())?;

    // 4. Compute the node's changed files (fork..@). This waits on jj, so the
    // detached async review path must not run it on a Tokio worker.
    let jj = JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
    let changed_jj = jj.clone();
    let changed_repo = repo_root.to_path_buf();
    let base_branch = coords.base_branch.clone();
    let base_commit = coords.base_commit.clone();
    let Some(changed) = tokio::task::spawn_blocking(move || {
        node_changed_files(
            &changed_jj,
            &changed_repo,
            base_branch.as_deref(),
            base_commit.as_deref(),
        )
    })
    .await
    .map_err(|error| format!("turn-end changed-file task failed: {error}"))?
    else {
        log::debug!(
            "turn-end checks for job {}: changed-file set unresolvable; nothing to run",
            short_id(job_id)
        );
        return Ok(false);
    };
    if changed.is_empty() {
        log::debug!(
            "turn-end checks for job {}: empty changed-file set; nothing to run",
            short_id(job_id)
        );
        return Ok(false);
    }

    // 5. Select the applicable turn-end checks (cadence + impact gate). Target
    // expansion may run cargo metadata, so planning belongs on the blocking pool.
    let planning_checks = checks.clone();
    let planning_changed = changed.clone();
    let planning_repo = repo_root.to_path_buf();
    let plans = tokio::task::spawn_blocking(move || {
        applicable_turn_end_checks(&planning_checks, &planning_changed, &planning_repo)
    })
    .await
    .map_err(|error| format!("turn-end check planning task failed: {error}"))?;
    if plans.is_empty() {
        log::debug!(
            "turn-end checks for job {}: no applicable review check; nothing to run",
            short_id(job_id)
        );
        return Ok(false);
    }

    let applicable_names = plans
        .iter()
        .map(|plan| plan.name.clone())
        .collect::<std::collections::HashSet<_>>();

    // 6. Resolve the sealed tree identity used as the cache key.
    let hash_jj = jj.clone();
    let hash_repo = repo_root.to_path_buf();
    let tree_hash = tokio::task::spawn_blocking(move || sealed_tree_hash(&hash_jj, &hash_repo))
        .await
        .map_err(|error| format!("turn-end tree-hash task failed: {error}"))?
        .map_err(|error| error.to_string())?;
    let commit_jj = jj.clone();
    let commit_repo = repo_root.to_path_buf();
    let sealed_commit = tokio::task::spawn_blocking(move || {
        commit_jj.run(
            &commit_repo,
            &["log", "-r", "@", "--no-graph", "-T", "commit_id"],
            "resolve sealed check commit",
        )
    })
    .await
    .map_err(|error| format!("turn-end commit-id task failed: {error}"))??;
    let (canonical_repo, store_dir) = crate::execution::checks::resolve_check_repository(
        orch,
        &coords.project_id,
        job_id,
        repo_root,
    )
    .await?;

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
    let cache_jj = jj.clone();
    let cache_repo = repo_root.to_path_buf();
    let cache_tree_hash = tree_hash.clone();
    let cache_project_id = coords.project_id.clone();
    let cache_job_id = job_id.to_string();
    let mut to_run = tokio::task::spawn_blocking(move || {
        let entries = if plans.iter().any(|plan| {
            cache_checks
                .get(&plan.name)
                .is_some_and(|check| check.impact.is_some())
        }) {
            sealed_tree_entries(&cache_jj, &cache_repo).ok()
        } else {
            None
        };
        let mut to_run: Vec<(CheckPlan, String)> = Vec::new();
        for plan in plans {
            let check = cache_checks
                .get(&plan.name)
                .expect("planned check must retain its configured definition");
            let input_hash = check_result_key(
                check,
                entries.as_deref(),
                &cache_tree_hash,
                &check_platform_identity(),
                check_toolchain_identity(),
            );
            match get_check_result(cache_db.clone(), &cache_project_id, &plan.name, &input_hash)
                .ok()
                .flatten()
            {
                Some(entry) => {
                    let _ = store_check_result(
                        cache_db.clone(),
                        CheckResultCacheWrite {
                            project_id: cache_project_id.clone(),
                            tree_hash: cache_tree_hash.clone(),
                            input_hash,
                            check_name: plan.name.clone(),
                            exit_code: entry.exit_code,
                            passed: entry.passed,
                            output_tail: entry.output_tail,
                            duration_ms: entry.duration_ms,
                            target_results_json: entry.target_results_json,
                            job_id: Some(cache_job_id.clone()),
                            cached: Some(true),
                            failure_kind: entry.failure_kind,
                            executor_id: entry.executor_id,
                            executor_device_id: entry.executor_device_id,
                            executor_connection_generation: entry.executor_connection_generation,
                            executor_cell_id: entry.executor_cell_id,
                            executor_lease_epoch: entry.executor_lease_epoch,
                            executor_started_at_unix_ms: entry.executor_started_at_unix_ms,
                            executor_finished_at_unix_ms: entry.executor_finished_at_unix_ms,
                            toolchain_fingerprint: entry.toolchain_fingerprint,
                        },
                    );
                }
                None => to_run.push((plan, input_hash)),
            }
        }
        to_run
    })
    .await
    .map_err(|error| format!("turn-end cache planning task failed: {error}"))?;
    if to_run.is_empty() {
        log::debug!(
            "turn-end checks for job {}: every applicable check is already cached for this tree; nothing to run",
            short_id(job_id)
        );
        return Ok(false);
    }
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
    let changed_files_path = log_dir.join("changed-files.txt");
    let changed_files_body = changed
        .iter()
        .map(|change| change.path.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    let extra_env = std::fs::write(&changed_files_path, changed_files_body)
        .map(|()| {
            vec![(
                CHANGED_FILES_ENV.to_string(),
                changed_files_path.to_string_lossy().into_owned(),
            )]
        })
        .map_err(|error| format!("failed to write review changed-files input: {error}"))?;

    for (plan, _) in &mut to_run {
        plan.requires_runner_local_admission = false;
    }

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
                resource_identity_key: check_resource_identity(
                    &plan.name,
                    checks
                        .get(&plan.name)
                        .expect("planned check must retain its configured definition"),
                )
                .key,
                command: plan.command.clone(),
                stream_id: crate::mcp::handlers::run::check_stream_id(&checks_tool_id, index),
                env: extra_env.clone(),
                timeout_ms: timeouts[index],
                constraints: checks
                    .get(&plan.name)
                    .and_then(|check| check.constraints.clone()),
            }
        })
        .collect();
    let delivery_commit = sealed_commit.trim().to_string();
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
    let batched_results = tokio::select! {
        biased;
        _ = cancel.cancelled() => {
            log::info!(
                "turn-end checks for job {}: cancelled mid-suite (issue resolved); abandoning {} check(s)",
                short_id(job_id),
                to_run.len()
            );
            if orch.fleet.cancel_job_requests(job_id) > 0 {
                let _ = orch.services.emitter.emit(
                    "db-change",
                    serde_json::json!({"table": "build_slots", "action": "cancel"}),
                );
            }
            return Ok(false);
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
    let outcomes = run_planned_checks(
        db.clone(),
        &coords.project_id,
        &tree_hash,
        job_id,
        &to_run,
        &checks_tool_id,
        CheckExecMode::Shared,
        &orch.check_admission,
        Some(orch),
        move |index, _command, _stream_id| {
            let batched_results = batched_results.clone();
            async move {
                batched_results
                    .lock()
                    .unwrap()
                    .remove(&index)
                    .unwrap_or_else(|| {
                        Err(crate::execution::checks::CheckExecutionFailure::Substrate(
                            format!("Cairn check infrastructure failure: missing review batch outcome for plan index {index}"),
                        ))
                    })
            }
        },
        |_| {},
    )
    .await;

    let any_failed = outcomes.iter().any(|o| !o.passed);
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
        if any_failed { "wake" } else { "passive" }
    );

    // 10. Deliver one push per distinct sealed check state. Red execution evidence
    // is deliberately not reusable, so the attention ledger, not the result cache,
    // owns notification dampening. Green remains passive and records a transition
    // that allows a later identical-looking red to wake again.
    let checks_uri = checks_uri_for_job(&coords);
    let key = format!("turn-checks:{checks_uri}");
    let fingerprint = turn_check_fingerprint(&tree_hash, &delivery_commit, &outcomes);
    let latest = attention_push::latest_push_fingerprint(&owning, job_id, &key)
        .await
        .map_err(|error| format!("failed to read turn-check fingerprint: {error}"))?;
    let duplicate_failure = any_failed
        && fingerprint.is_some()
        && latest.as_ref().and_then(|value| value.as_ref()) == fingerprint.as_ref();

    if duplicate_failure {
        log::info!(
            "turn-end checks for job {}: deduped unchanged failing delivery",
            short_id(job_id)
        );
    } else {
        let decision = if fingerprint.is_none() {
            "ambiguous/fail-open"
        } else if !any_failed {
            "green"
        } else if latest.is_none() {
            "new"
        } else {
            "changed"
        };
        log::info!(
            "turn-end checks for job {}: delivery decision {}",
            short_id(job_id),
            decision
        );
        let (_, effective_wake) = attention_push::push_with_fingerprint(
            &owning,
            job_id,
            &checks_uri,
            delivery_wake(any_failed),
            attention_push::Boundary::Event,
            &key,
            fingerprint.as_deref(),
        )
        .await
        .map_err(|error| format!("failed to queue turn-check results push: {error}"))?;
        if any_failed && effective_wake == attention_push::Wake::Wake {
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
    Ok(any_failed)
}

const TURN_CHECK_FINGERPRINT_VERSION: &str = "turn-check-state-v1";
const SALIENT_FAILURE_CHARS: usize = 1_500;

fn normalized_salient(value: &str) -> Option<String> {
    let normalized = value.replace("\r\n", "\n").replace('\r', "\n");
    let trimmed = normalized.trim();
    if trimmed.is_empty() || trimmed.contains('\0') {
        return None;
    }
    Some(tail(trimmed, SALIENT_FAILURE_CHARS))
}

fn turn_check_fingerprint(
    tree_hash: &str,
    sealed_commit: &str,
    outcomes: &[CheckOutcome],
) -> Option<String> {
    let tree_hash = normalized_salient(tree_hash)?;
    let sealed_commit = normalized_salient(sealed_commit)?;
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
        let salient = if outcome.passed {
            None
        } else if let Some(parsed) = &outcome.parsed {
            if parsed.failures.is_empty() {
                Some(normalized_salient(&outcome.output_tail)?)
            } else {
                let mut failures = Vec::with_capacity(parsed.failures.len());
                for failure in &parsed.failures {
                    failures.push((
                        normalized_salient(&failure.name)?,
                        match failure.message.as_deref() {
                            Some(message) => Some(normalized_salient(message)?),
                            None => None,
                        },
                    ));
                }
                failures.sort();
                Some(serde_json::to_string(&failures).ok()?)
            }
        } else {
            Some(normalized_salient(&outcome.output_tail)?)
        };
        canonical.push((name, outcome.passed, kind, salient));
    }
    canonical.sort_by(|left, right| left.0.cmp(&right.0));
    let payload = serde_json::to_vec(&(
        TURN_CHECK_FINGERPRINT_VERSION,
        tree_hash,
        sealed_commit,
        canonical,
    ))
    .ok()?;
    Some(format!("sha256:{:x}", Sha256::digest(payload)))
}

/// The wake level a completed turn-end run is delivered at: a failure ROUSES the
/// idle builder (`Wake`), a clean run rides along `Passive` so green never costs a
/// turn but is still delivered and recorded. Pure, so the green-passive /
/// red-wake decision is unit-tested.
fn delivery_wake(any_failed: bool) -> attention_push::Wake {
    if any_failed {
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
    pub(crate) worktree_path: Option<String>,
    pub(crate) base_branch: Option<String>,
    pub(crate) base_commit: Option<String>,
    pub(crate) project_key: String,
    pub(crate) number: i32,
    pub(crate) exec_seq: i32,
    pub(crate) node_segment: String,
    parent_segment: Option<String>,
    is_workflow: bool,
    /// The issue's stored lifecycle status (`active`, `merged`, `closed`, …). Read
    /// by the runner to skip a suite whose issue already resolved (CAIRN-2648).
    issue_status: String,
}

fn checks_uri_for_job(coords: &JobCoords) -> String {
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

/// Resolve everything the runner and renderer need from a `job_id`: the project
/// and issue ids, the worktree path and base VCS anchors, and the
/// project-key/number/exec-seq/node-segment that build the `/checks` URI.
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
                    "SELECT j.project_id, j.worktree_path, j.base_branch,
                            j.base_commit, p.key, i.number, e.seq, j.uri_segment,
                            parent.uri_segment, i.status, j.agent_config_id
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
                    worktree_path: row.opt_text(1)?,
                    base_branch: row.opt_text(2)?.filter(|s| !s.is_empty()),
                    base_commit: row.opt_text(3)?.filter(|s| !s.is_empty()),
                    project_key: row.text(4)?,
                    number: row.i64(5)? as i32,
                    exec_seq: row.i64(6)? as i32,
                    node_segment: row.opt_text(7)?.unwrap_or_default(),
                    parent_segment: row.opt_text(8)?,
                    issue_status: row.opt_text(9)?.unwrap_or_default(),
                    is_workflow: row.opt_text(10)?.as_deref() == Some("workflow"),
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
            duration_ms: 1,
        }
    }

    fn status(name: &str, state: NodeCheckState) -> NodeCheckStatus {
        NodeCheckStatus {
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
            failure_names: Vec::new(),
            output_tail: None,
            failure_kind: None,
        }
    }

    #[test]
    fn fingerprint_is_order_independent_and_state_sensitive() {
        let rust = outcome("rust", false, None, "assertion A");
        let lint = outcome("lint", true, None, "");
        let first = turn_check_fingerprint("tree", "commit", &[rust, lint]).unwrap();
        let second = turn_check_fingerprint(
            "tree",
            "commit",
            &[
                outcome("lint", true, None, ""),
                outcome("rust", false, None, "assertion A"),
            ],
        )
        .unwrap();
        assert_eq!(first, second);
        assert_ne!(
            first,
            turn_check_fingerprint(
                "tree",
                "commit",
                &[outcome("rust", false, None, "assertion B")]
            )
            .unwrap()
        );
        assert_ne!(
            first,
            turn_check_fingerprint(
                "tree-2",
                "commit",
                &[outcome("rust", false, None, "assertion A")]
            )
            .unwrap()
        );
        assert_ne!(
            first,
            turn_check_fingerprint(
                "tree",
                "commit-2",
                &[outcome("rust", false, None, "assertion A")]
            )
            .unwrap()
        );
    }

    #[test]
    fn fingerprint_distinguishes_green_ordinary_and_infrastructure_red() {
        let green =
            turn_check_fingerprint("tree", "commit", &[outcome("rust", true, None, "")]).unwrap();
        let red = turn_check_fingerprint("tree", "commit", &[outcome("rust", false, None, "same")])
            .unwrap();
        let infrastructure = turn_check_fingerprint(
            "tree",
            "commit",
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
    fn fingerprint_fails_open_when_required_identity_or_failure_detail_is_ambiguous() {
        assert!(turn_check_fingerprint("", "commit", &[outcome("rust", true, None, "")]).is_none());
        assert!(turn_check_fingerprint("tree", "", &[outcome("rust", true, None, "")]).is_none());
        assert!(turn_check_fingerprint("tree", "commit", &[]).is_none());
        assert!(turn_check_fingerprint(
            "tree",
            "commit",
            &[outcome("rust", false, None, "  \r\n  ")]
        )
        .is_none());
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
            worktree_path: None,
            base_branch: None,
            base_commit: None,
            project_key: "CAIRN".into(),
            number: 42,
            exec_seq: 2,
            node_segment: "review-rust".into(),
            parent_segment: Some("builder".into()),
            is_workflow: false,
            issue_status: "active".into(),
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

    #[test]
    fn green_rides_along_passively_red_wakes() {
        assert_eq!(delivery_wake(false), attention_push::Wake::Passive);
        assert_eq!(delivery_wake(true), attention_push::Wake::Wake);
    }

    #[test]
    fn short_id_never_panics_on_a_short_string() {
        assert_eq!(short_id("abcd"), "abcd");
        assert_eq!(short_id("0123456789"), "01234567");
    }

    #[test]
    fn global_check_capacity_is_a_sane_bound() {
        let n = crate::execution::check_admission::CheckAdmissionController::capacity_for_host();
        assert!(
            (2..=4).contains(&n),
            "review concurrency cap {n} must stay in [2, 4]"
        );
    }
}
