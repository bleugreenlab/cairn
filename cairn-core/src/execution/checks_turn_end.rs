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
//! ## Concurrency and isolation
//!
//! The cache-miss checks run CONCURRENTLY, each in its own copy-on-write clone of
//! the sealed worktree ([`crate::execution::check_isolation`]) — the same engine
//! ([`crate::execution::checks::run_planned_checks`]) and isolation machinery the
//! write cadence uses. Because the review suites are heavy and each is internally
//! multi-threaded (two full Rust compiles among them, each cloning its own
//! `target`), the parallelism is BOUNDED by [`review_max_concurrency`] rather than
//! unbounded like the thin write cadence. When a cheap clone is unavailable the
//! whole batch falls back to SEQUENTIAL in-place execution in the one shared
//! checkout ([`check_isolation::decide_exec_mode`]). Isolation is fold-free here:
//! a stray tracked write lands in a disposable clone and is discarded, so it can
//! never dirty `@` and wedge the next write's seal.
//!
//! ## Two guards keep it from looping
//!
//! - Single-flight (`Orchestrator::try_begin_turn_end_checks`): a rapid re-idle
//!   never stacks a second suite for the same job.
//! - Loop-break (the cache): a plan whose `(project_id, name, input_hash)` is already
//!   cached is dropped before launch. A resume from a failing check produces a
//!   follow-up turn; if it commits a fix the affected check's input hash changes
//!   and it runs once; if it commits nothing every input hash is unchanged, every
//!   check is already cached, and nothing relaunches — so the agent is resumed at
//!   most ONCE per failing tree.

use std::path::{Path, PathBuf};

use cairn_common::uri::build_node_checks_uri;

use crate::execution::cache::{get_check_result, store_check_result, CheckResultCacheWrite};
use crate::execution::check_isolation;
use crate::execution::checks::{
    applicable_turn_end_checks, input_hash_for, load_live_project_checks, resolve_check_timeout_ms,
    run_planned_checks, DEFAULT_REVIEW_CHECK_TIMEOUT_MS,
};
use crate::execution::selection::CheckPlan;
use crate::jj::{node_changed_files, sealed_tree_entries, sealed_tree_hash, JjEnv};
use crate::orchestrator::{attention_push, Orchestrator};
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
pub async fn run_turn_end_checks(orch: Orchestrator, job_id: String) {
    // `fresh_red` is true only when this launch ran a check that freshly failed
    // (a non-cached failing verdict). Early-exit paths (no worktree, no checks, no
    // changed files, all-cached) yield `false`, so a planner writing a plan still
    // yields a prompt parent wake at the completion edge.
    let fresh_red = match run_turn_end_checks_inner(&orch, &job_id).await {
        Ok(fresh_red) => fresh_red,
        Err(e) => {
            log::warn!(
                "turn-end checks for job {}: {}",
                &job_id[..job_id.len().min(8)],
                e
            );
            false
        }
    };
    // Release the single-flight slot BEFORE evaluating review readiness so the
    // checks-settled gate does not see this job's own in-flight marker.
    orch.end_turn_end_checks(&job_id);
    // The checks pipeline owns review evaluation end-to-end (CAIRN-2483): every
    // exit path lands here, so a green completion, an all-cached exit, an empty
    // changed set, or an inner error all re-evaluate whether the reviewed issue
    // has settled and, if so, wake its watchers.
    if let Some(issue_id) = issue_id_for_job(&orch.db.local, &job_id).await {
        crate::orchestrator::lifecycle::evaluate_review_readiness(&orch, &issue_id, fresh_red)
            .await;
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
async fn run_turn_end_checks_inner(orch: &Orchestrator, job_id: &str) -> Result<bool, String> {
    // 1. Resolve the node's coordinates (project, issue, worktree, base anchors).
    let Some(coords) = resolve_job_coords(&orch.db.local, job_id).await? else {
        return Ok(false);
    };
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

    // 4. Compute the node's changed files (fork..@).
    let jj = JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
    let Some(changed) = node_changed_files(
        &jj,
        repo_root,
        coords.base_branch.as_deref(),
        coords.base_commit.as_deref(),
    ) else {
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

    // 5. Select the applicable turn-end checks (cadence + impact gate).
    let plans = applicable_turn_end_checks(&checks, &changed, repo_root);
    if plans.is_empty() {
        log::debug!(
            "turn-end checks for job {}: no applicable review check; nothing to run",
            short_id(job_id)
        );
        return Ok(false);
    }

    // 6. Resolve the sealed tree identity used as the cache key.
    let tree_hash = sealed_tree_hash(&jj, repo_root).map_err(|e| e.to_string())?;

    // 7. Loop-break gate: drop any plan already cached for its INPUT hash (the
    // content of just that check's impact-matched files). A covered plan is
    // re-stamped onto the current whole tree so the `/checks` listing still shows
    // it, then skipped; only genuinely-uncovered plans run. If none remain, the
    // tree has already been fully checked (e.g. a resume that committed nothing) —
    // return WITHOUT launching so the agent is never nagged on the same break.
    let db = orch.db.local.clone();
    let entries = if plans
        .iter()
        .any(|p| checks.get(&p.name).is_some_and(|c| c.impact.is_some()))
    {
        sealed_tree_entries(&jj, repo_root).ok()
    } else {
        None
    };
    let mut to_run: Vec<(CheckPlan, String)> = Vec::new();
    for plan in plans {
        let input_hash = input_hash_for(
            checks.get(&plan.name).and_then(|c| c.impact.as_ref()),
            entries.as_deref(),
            &tree_hash,
        );
        match get_check_result(db.clone(), &coords.project_id, &plan.name, &input_hash)
            .ok()
            .flatten()
        {
            Some(entry) => {
                // Covered for this input; re-stamp onto the current tree so the
                // `/checks` listing surfaces it, then skip (no re-run).
                let _ = store_check_result(
                    db.clone(),
                    CheckResultCacheWrite {
                        project_id: coords.project_id.clone(),
                        tree_hash: tree_hash.clone(),
                        input_hash,
                        check_name: plan.name.clone(),
                        exit_code: entry.exit_code,
                        passed: entry.passed,
                        output_tail: entry.output_tail,
                        duration_ms: entry.duration_ms,
                        target_results_json: entry.target_results_json,
                        job_id: Some(job_id.to_string()),
                        cached: Some(true),
                        failure_kind: entry.failure_kind,
                    },
                );
            }
            None => to_run.push((plan, input_hash)),
        }
    }
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

    // 9. Decide the execution mode: give every cache-miss check its own COW clone
    // of the sealed worktree so the review suite runs concurrently in isolation,
    // BOUNDED (the suites are heavy). A clone failure routes the whole batch to
    // sequential in-place execution in the shared worktree. The guard removes the
    // review clone root on every exit path — including the fallback. Review is
    // FOLD-FREE by contract: the clones are discarded, never copied back, so a
    // stray tracked write lands in a disposable clone rather than dirtying `@`.
    let clone_root = check_isolation::turn_end_clone_root_for_job(&orch.config_dir, job_id);
    let _clone_guard = check_isolation::CloneGuard::new(clone_root.clone());
    let (mode, clones) = {
        let misses: Vec<(usize, &str)> = to_run
            .iter()
            .enumerate()
            .map(|(index, (plan, _))| (index, plan.name.as_str()))
            .collect();
        check_isolation::decide_exec_mode(&*orch.services.fs, repo_root, &clone_root, &misses)
    };

    // Isolated checks run in `.jj`-stripped clones, so a diff-scoped check
    // (rust-lint, dead-code) can't resolve its own changed-file set from the
    // clone's VCS and would fall back to full-strict, gating on pre-existing base
    // findings. Hand them the already-computed set via a file + env override so
    // attribution stays diff-scoped without the clone ever shelling jj. The Shared
    // fallback runs in the real worktree, where jj resolution works, so it needs
    // no override (empty env, unchanged behavior).
    let extra_env: Vec<(String, String)> = if mode == check_isolation::CheckExecMode::Isolated {
        let path = log_dir.join("changed-files.txt");
        let body = changed
            .iter()
            .map(|c| c.path.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        match std::fs::write(&path, body) {
            Ok(()) => vec![(
                CHANGED_FILES_ENV.to_string(),
                path.to_string_lossy().into_owned(),
            )],
            Err(e) => {
                log::warn!(
                    "turn-end checks for job {}: failed to write changed-files override ({e}); \
                     isolated diff-scoped checks will run full-strict",
                    short_id(job_id)
                );
                Vec::new()
            }
        }
    } else {
        Vec::new()
    };

    // 10. Run the misses through the shared engine, concurrently and bounded.
    // `to_run` is already miss-only, so the engine's cache-hit branch never fires:
    // it runs every plan, stores each verdict (`cached: false`, `job_id`), and
    // parses per-test detail exactly as the old inline loop did. `notify` is a
    // no-op — review has no `check-status` frontend consumer; its live surface is
    // the per-check log tail plus the bracketing `db-change`s.
    let cap = review_max_concurrency().min(to_run.len());
    let clones_ref = &clones;
    let to_run_ref = &to_run;
    // Per-check effective timeout, aligned to plan index. A check's schema
    // `timeout` overrides the review cadence default (sized for a cold, uncached
    // full Rust build; a healthy-but-slow suite is no longer killed at 10 min).
    let timeouts: Vec<u32> = to_run
        .iter()
        .map(|(plan, _)| {
            resolve_check_timeout_ms(checks.get(&plan.name), DEFAULT_REVIEW_CHECK_TIMEOUT_MS)
        })
        .collect();
    let timeouts_ref = &timeouts;
    let extra_env_ref = &extra_env;
    let worktree = worktree_path.clone();
    let outcomes = run_planned_checks(
        db.clone(),
        &coords.project_id,
        &tree_hash,
        job_id,
        &to_run,
        &format!("turn-checks:{job_id}"),
        mode,
        move |index, command, stream_id| {
            let worktree = worktree.clone();
            async move {
                // Isolated: run in this check's clone; Shared fallback: the real
                // worktree. Review always runs UNSANDBOXED (an idle agent can't
                // answer a fence prompt), so the sandbox flag is ignored.
                let (cwd, _sandbox) =
                    check_isolation::resolve_check_exec(clones_ref, index, &worktree);
                let name = to_run_ref[index].0.name.clone();
                let log_path = turn_end_log_path(orch, job_id, &name);
                // Mark this check started the instant it begins (file-exists =
                // running, even before its first line), then nudge any live
                // `/checks` view to (re)start its tail poll.
                let _ = std::fs::write(&log_path, b"");
                let _ = orch.services.emitter.emit(
                    "db-change",
                    serde_json::json!({"table": "check_result_cache", "action": "update"}),
                );
                crate::mcp::handlers::run::run_check_command_unsandboxed(
                    orch,
                    &cwd,
                    &stream_id,
                    &command,
                    timeouts_ref[index],
                    &log_path,
                    extra_env_ref,
                )
                .await
            }
        },
        |_| {},
        Some(cap.max(1)),
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

    // 10. Deliver the results into the session. A failure ROUSES the idle builder
    // (Wake + nudge) so it resumes with the failing detail inlined; a clean run
    // rides along PASSIVELY — never a wasted turn on green, but the green verdict
    // is still delivered on the session's next resume and recorded on the wake
    // card. The push key is the same for both, so same-key supersession keeps at
    // most one checks push pending: a later red supersedes an undelivered green,
    // and vice versa.
    let checks_uri = build_node_checks_uri(
        &coords.project_key,
        coords.number,
        coords.exec_seq,
        &coords.node_segment,
    );
    let key = format!("turn-checks:{checks_uri}");
    if let Err(e) = attention_push::push(
        &owning,
        job_id,
        &checks_uri,
        delivery_wake(any_failed),
        attention_push::Boundary::Event,
        &key,
    )
    .await
    {
        return Err(format!("failed to queue turn-check results push: {e}"));
    }
    // Only a failure wakes an idle builder now; a passive green push waits for the
    // session's next natural resume.
    if any_failed {
        if let Err(e) = crate::messages::delivery::nudge_job_for_urgency(
            orch,
            job_id,
            crate::messages::queued::DeliveryUrgency::Steer,
        ) {
            log::warn!(
                "turn-check failure wake for job {} failed: {}",
                short_id(job_id),
                e
            );
        }
    }
    Ok(any_failed)
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

/// A modest cap on how many review checks run concurrently under COW isolation.
/// The review suite carries heavy, internally-multithreaded compiles (two full
/// Rust builds among them), each cloning its own `src-tauri/target`, so the real
/// contention is a handful of concurrent heavy compiles rather than the raw check
/// count. Derive the cap from the core count (divided by 4, clamped to `[2, 4]`):
/// an 8-core host runs 2 at a time, a 16-core host runs 4 — enough parallelism to
/// win wall-clock without oversubscribing CPU/IO. A tunable constant; adjust the
/// divisor/clamp if the mix of heavy suites changes.
fn review_max_concurrency() -> usize {
    std::thread::available_parallelism()
        .map(|n| (n.get() / 4).clamp(2, 4))
        .unwrap_or(2)
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
pub async fn render_turn_end_checks_section(orch: &Orchestrator, job_id: &str) -> Option<String> {
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
                            j.base_commit, p.key, i.number, e.seq, j.uri_segment
                     FROM jobs j
                     JOIN projects p ON p.id = j.project_id
                     JOIN issues i ON i.id = j.issue_id
                     JOIN executions e ON e.id = j.execution_id
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
/// blank. Split from [`read_turn_end_log_tail`] so the missing/empty-vs-content
/// boundary is unit-testable without an [`Orchestrator`].
fn read_log_tail(path: &Path, max_chars: usize) -> Option<String> {
    let content = std::fs::read_to_string(path).ok()?;
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
    fn review_max_concurrency_is_a_sane_bound() {
        let n = review_max_concurrency();
        assert!(
            (2..=4).contains(&n),
            "review concurrency cap {n} must stay in [2, 4]"
        );
    }
}
