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
//! Settings UI edits. It is NOT read from the agent worktree's own committed
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
//! ## Concurrency and isolation
//!
//! The affected cache-MISS checks run CONCURRENTLY, each against its own
//! copy-on-write clone of the sealed worktree ([`crate::execution::check_isolation`]).
//! Isolation is universal — a formatter's writes physically cannot reach another
//! check's view because they never share a filesystem — so a check can mutate
//! freely and its changes are copied back into the real worktree, in plan order,
//! only after every check finishes; the existing fold then folds them into the
//! sealed commit. Every check therefore validates exactly the SEALED tree, one
//! well-defined input, which is strictly more deterministic than the previous
//! sequential in-place loop (where a check saw whatever tree the prior check left,
//! in arbitrary plan order). When a cheap COW clone is unavailable (a non-APFS
//! volume, a clone failure, disk full) the whole batch falls back to the original
//! SEQUENTIAL in-place execution in the one shared checkout — the mode is decided
//! once, up front. Isolated checks run unconfined (the disposable clone is the
//! isolation); the shared fallback keeps the sandbox + check-command exemption.
//!
//! ## Cache key
//!
//! Each check's verdict is keyed by its INPUT hash: the content identity of only
//! the files in the sealed tree matching that check's `impact` globs (see
//! [`input_hash_for`] / [`crate::execution::selection::check_input_hash`]). A
//! commit that changed none of a check's inputs — a doc-only commit landing after
//! a source commit — hits the cache even though the whole-tree hash moved, so the
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

use crate::config::project_settings::{load_checks, CheckCommand, CheckWhen};
use crate::execution::cache::{
    get_check_result, list_latest_check_results_for_project, store_check_result,
    CheckResultCacheEntry, CheckResultCacheWrite,
};
use crate::execution::check_isolation::{self, CheckExecMode, CheckMutation};
use crate::execution::check_parsers::{
    extract_running_tests, format_failure_excerpt, format_failure_names, parse_check_output,
    ParsedCheckResult, MAX_FAILURE_NAMES,
};
use crate::execution::selection::{plan_checks, CheckPlan};
use crate::jj::{
    node_changed_files, sealed_tree_entries, sealed_tree_hash, tree_entries, GraphFileChange, JjEnv,
};
use crate::mcp::handlers::run::{CheckExecResult, CheckStatusEntry, CheckStatusPayload};
use crate::mcp::handlers::RunContext;
use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};

/// Default per-check time cap for the mid-turn `when:write` cadence. Its checks
/// are light (change-scoped test runs, a formatter, small consistency guards),
/// so 10 minutes is ample. A check may raise its own via the schema `timeout`.
pub(crate) const DEFAULT_WRITE_CHECK_TIMEOUT_MS: u32 = 600_000;
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
}

impl CheckFailureKind {
    pub fn as_str(self) -> &'static str {
        match self {
            CheckFailureKind::TimedOut => "timed_out",
            CheckFailureKind::SpawnError => "spawn_error",
            CheckFailureKind::Killed => "killed",
        }
    }

    /// Parse a persisted `failure_kind` string back into the enum; `None` for a
    /// pass, an ordinary failure, or a legacy `NULL`. (Not `FromStr`: the
    /// absent/unknown case is an ordinary `None`, not a parse error.)
    pub fn from_stored(s: &str) -> Option<Self> {
        match s {
            "timed_out" => Some(CheckFailureKind::TimedOut),
            "spawn_error" => Some(CheckFailureKind::SpawnError),
            "killed" => Some(CheckFailureKind::Killed),
            _ => None,
        }
    }

    /// The human verdict fragment, given the run duration (used for the timeout
    /// budget it was killed at).
    pub fn describe(self, duration_ms: i64) -> String {
        match self {
            CheckFailureKind::TimedOut => {
                format!("timed out after {}", format_timeout_budget(duration_ms))
            }
            CheckFailureKind::SpawnError => "failed to spawn".to_string(),
            CheckFailureKind::Killed => "killed (signal)".to_string(),
        }
    }
}

/// Format a timeout budget compactly: whole minutes at or above a minute, else
/// seconds. `600_000` → `10m`, `1_800_000` → `30m`, `45_000` → `45s`.
pub(crate) fn format_timeout_budget(duration_ms: i64) -> String {
    if duration_ms >= 60_000 {
        format!("{}m", (duration_ms as f64 / 60_000.0).round() as i64)
    } else {
        format!("{}s", (duration_ms as f64 / 1000.0).round() as i64)
    }
}

/// Chars of combined check output retained in the cache row's `output_tail`.
const OUTPUT_TAIL_CHARS: usize = 4_000;

/// Run the affected `when:write` checks after a source-touching commit has been
/// sealed, streaming their output live and returning a compact inline pass/fail
/// summary to append to the originating tool result.
///
/// Returns `None` whenever nothing applied: no run context (so no streaming
/// target), no `checks` contract, no resolvable changed-file set, an empty
/// change set, or no `when:write` check whose impact the change set matches (a
/// doc-only / non-source commit). A cache hit returns the stored verdict without
/// re-running.
pub async fn run_write_checks_after_seal(
    orch: &Orchestrator,
    run_context: Option<&RunContext>,
    cwd: &str,
    tool_use_id: &str,
) -> Option<String> {
    // No run context ⇒ no run id to stream against and no job to anchor the diff.
    let run_context = run_context?;
    let repo_root = Path::new(cwd);

    // 1. Load the LIVE checks contract from the project's main checkout — the same
    // `.cairn/config.yaml` the Settings UI edits. The worktree's own committed
    // copy was snapshotted when the branch was cut, so a project-level edit made
    // mid-session would never reach an in-flight agent if we read it. Sourcing the
    // contract from the live project config makes a Settings edit take effect on
    // the very next commit, no restart. (Changed files, glob matching, the tree
    // hash, and the check commands themselves still run against the sealed
    // worktree commit at `repo_root` — only the contract is project-sourced.)
    let checks = load_live_project_checks(orch, &run_context.project_id, repo_root).await?;
    if checks.is_empty() {
        return None;
    }

    // 2. Compute the node's changed files (fork..@) from the live sealed graph.
    let jj = JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
    let (base_branch, base_commit) =
        load_node_vcs_anchors(&orch.db.local, &run_context.job_id).await;
    let changed = node_changed_files(
        &jj,
        repo_root,
        base_branch.as_deref(),
        base_commit.as_deref(),
    )?;
    if changed.is_empty() {
        return None;
    }

    // 3 + 4. Plan against the cumulative branch diff first. This remains the
    // conservative impact gate: a cumulative match may over-apply a check, but the
    // input-hash cache below turns unchanged inputs into hits so no command runs.
    let plans = applicable_write_checks(&checks, &changed, repo_root);
    if plans.is_empty() {
        return None;
    }

    // 5. Resolve the sealed tree identity used as the cache key.
    let tree_hash = sealed_tree_hash(&jj, repo_root).ok()?;

    // Safety baseline for the post-check fold below. A `when:write` check is an
    // OBSERVER of the just-sealed commit, but its command may rewrite tracked files
    // (a formatter, `lint --fix`, regenerated snapshots) or leave tracked churn.
    // Capture whether `@` is clean BEFORE any check runs: right after a seal `@` is
    // empty (whole-`@` run-seal) or carries only pre-existing unrelated dirt
    // (path-scoped write-seal). We fold `@` into the seal afterward ONLY when it was
    // clean here, so the fold captures check-made changes and NOTHING ELSE — never
    // pre-existing dirt a path-scoped seal deliberately left loose. A probe error
    // defaults to "not clean" so a doubtful case never folds.
    let clean_before = !crate::jj::is_working_copy_dirty(&jj, repo_root).unwrap_or(true);

    // Per-check input hash: key each verdict by the content identity of ONLY the
    // files matching that check's impact globs, so a commit touching none of a
    // check's inputs (a doc-only commit after a src-tauri commit) reuses the
    // stored verdict instead of re-running. Read the sealed tree once, and only
    // when some applicable check is impact-scoped.
    let entries = if plans
        .iter()
        .any(|p| checks.get(&p.name).is_some_and(|c| c.impact.is_some()))
    {
        sealed_tree_entries(&jj, repo_root).ok()
    } else {
        None
    };
    let latest_by_check: HashMap<String, CheckResultCacheEntry> =
        list_latest_check_results_for_project(orch.db.local.clone(), &run_context.project_id)
            .unwrap_or_default()
            .into_iter()
            .map(|row| (row.check_name.clone(), row))
            .collect();
    let keyed: Vec<(CheckPlan, String)> = plans
        .into_iter()
        .map(|plan| {
            let check = checks.get(&plan.name);
            let impact = check.and_then(|c| c.impact.as_ref());
            let input_hash = input_hash_for(impact, entries.as_deref(), &tree_hash);

            // If this input is already cached, keep the cumulative plan: the runner
            // will re-stamp the row and skip execution. Only a cache MISS needs a
            // concrete selector. For misses, a PASSING latest baseline lets us
            // narrow `{changedFiles}` / `{targets}` to the tree-vs-tree delta since
            // that verdict; a failing or unreadable baseline falls back to the full
            // cumulative branch diff. That fallback is required for soundness:
            // structured failures name tests, not necessarily files, so we cannot
            // feed a previous failure into file-based selectors like `vitest related`.
            let should_reselect = get_check_result(
                orch.db.local.clone(),
                &run_context.project_id,
                &plan.name,
                &input_hash,
            )
            .ok()
            .flatten()
            .is_none();
            let selected_plan = if should_reselect {
                match check {
                    Some(check) => {
                        let selected_changed = selected_changed_files_for_miss(
                            latest_by_check.get(&plan.name),
                            entries.as_deref(),
                            impact,
                            &changed,
                            &jj,
                            repo_root,
                        );
                        replan_one_check(&plan.name, check, &selected_changed, repo_root)
                            .unwrap_or(plan)
                    }
                    None => plan,
                }
            } else {
                plan
            };
            (selected_plan, input_hash)
        })
        .collect();

    // Decide the execution mode: give every cache-MISS check its own COW clone of
    // the sealed worktree so the affected checks can run concurrently in
    // isolation. Hits never execute, so they never need a clone. A single clone
    // failure (non-APFS volume, disk full, cross-volume) routes the WHOLE batch to
    // the sequential in-place fallback — the mode is decided once, up front. The
    // guard removes the job's clone root on every exit path.
    let clone_root =
        check_isolation::clone_root_for_job(&orch.config_dir, run_context.job_id.as_str());
    // Held for its Drop: removes the job's clone root on EVERY exit path below —
    // partial clones after a mid-batch clone failure, or the full set after the
    // fold. Created unconditionally; a no-op when no clone was ever made.
    let _clone_guard = check_isolation::CloneGuard::new(clone_root.clone());
    // Build the cache-MISS set (index + name); a hit never runs so it needs no
    // clone. `decide_exec_mode` COW-clones each miss and returns `Isolated` on full
    // success, or falls the whole batch back to `Shared` (empty clones) on any
    // clone failure — the mode is decided once, up front, for both cadences.
    let (mode, clones) = {
        let misses: Vec<(usize, &str)> = keyed
            .iter()
            .enumerate()
            .filter(|(_, (plan, input_hash))| {
                get_check_result(
                    orch.db.local.clone(),
                    &run_context.project_id,
                    &plan.name,
                    input_hash,
                )
                .ok()
                .flatten()
                .is_none()
            })
            .map(|(index, (plan, _))| (index, plan.name.as_str()))
            .collect();
        check_isolation::decide_exec_mode(&*orch.services.fs, repo_root, &clone_root, &misses)
    };
    // Snapshot each clone's baseline stat identity BEFORE any check runs, so the
    // post-run fold can isolate exactly the check-made mutations. Empty (and the
    // loop a no-op) in the `Shared` fallback, where nothing was cloned.
    let mut baselines: BTreeMap<usize, BTreeMap<PathBuf, (u64, u64)>> = BTreeMap::new();
    for (index, dir) in &clones {
        baselines.insert(*index, check_isolation::baseline_index(dir));
    }

    // The live status-line emitter. `run_planned_checks` calls this with a full
    // checklist snapshot on every state transition; we forward each snapshot to
    // the frontend as a `check-status` event keyed by the committing call id.
    // Follows the `db-change` emit idiom below.
    let emitter = orch.services.emitter.clone();
    let notify_run_id = run_context.run_id.clone();
    let notify_tool_use_id = tool_use_id.to_string();
    let clones_ref = &clones;
    // Per-check effective timeout, aligned to plan index (the `execute` closure
    // is indexed the same way). A check's schema `timeout` overrides the write
    // cadence default.
    let timeouts: Vec<u32> = keyed
        .iter()
        .map(|(plan, _)| {
            resolve_check_timeout_ms(checks.get(&plan.name), DEFAULT_WRITE_CHECK_TIMEOUT_MS)
        })
        .collect();
    let timeouts_ref = &timeouts;
    let results = run_planned_checks(
        orch.db.local.clone(),
        &run_context.project_id,
        &tree_hash,
        run_context.job_id.as_str(),
        &keyed,
        tool_use_id,
        mode,
        move |index, command, stream_id| async move {
            // Isolated: run in this check's own clone, unconfined. Shared: the real
            // sealed worktree, sandboxed (the check-command exemption still lifts
            // it for a declared check).
            let (run_cwd, sandbox_enabled) =
                check_isolation::resolve_check_exec(clones_ref, index, cwd);
            crate::mcp::handlers::run::run_check_command(
                orch,
                &run_cwd,
                &stream_id,
                Some(run_context),
                &command,
                timeouts_ref[index],
                sandbox_enabled,
            )
            .await
        },
        move |checks| {
            let _ = emitter.emit(
                "check-status",
                serde_json::to_value(CheckStatusPayload {
                    run_id: notify_run_id.clone(),
                    tool_use_id: notify_tool_use_id.clone(),
                    checks,
                })
                .unwrap_or(serde_json::Value::Null),
            );
        },
        // Unbounded: the write cadence is deliberately thin (a handful of light
        // checks), so every cache-miss runs at once.
        None,
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

    // Copy each isolated check's mutations back into the real worktree, in plan
    // order, so the existing fold below folds them into the sealed commit exactly
    // as it folds an in-place formatter's edits. A no-op in Shared mode (the
    // checks already ran in place). `clones` is a BTreeMap, so `.iter()` is
    // plan-index order — the deterministic conflict-resolution order.
    if mode == CheckExecMode::Isolated && !clones.is_empty() {
        let per_check: Vec<(String, Vec<CheckMutation>)> = clones
            .iter()
            .filter_map(|(index, dir)| {
                let baseline = baselines.get(index)?;
                let muts = check_isolation::detect_mutations(dir, baseline, repo_root);
                Some((keyed[*index].0.name.clone(), muts))
            })
            .collect();
        let touched = check_isolation::apply_mutations(repo_root, &per_check);
        if !touched.is_empty() {
            log::info!(
                "when:write checks: copied {} isolated-check mutation(s) back into the worktree \
                 before folding: {:?}",
                touched.len(),
                touched
            );
        }
    }

    // Fold any tracked changes the checks made into the just-sealed commit, leaving
    // `@` clean == the amended tip. One mechanism, two jobs: a formatter's edits
    // land in the commit, AND the seal-clean invariant is restored so a concurrent
    // base-advance / reconcile in the lock-free check window can never snapshot or
    // rebase a check-dirtied `@` into the stale / divergent / behind-tip tangle that
    // wedges the next write's seal (the regression this fixes, CAIRN-2260). A pure
    // verify check makes no tracked change, so the fold is a no-op — the desync is
    // fixed either way. Unconditional w.r.t. pass/fail; gated only on `clean_before`
    // (the safety property above).
    let folded = if clean_before {
        fold_worktree_after_checks(orch, run_context, &jj, repo_root).await
    } else {
        None
    };

    if results.is_empty() {
        return None;
    }
    // 6. Compose the inline summary, surfacing any folded check edits.
    let mut summary = format!("Checks: {}", format_check_summary(&results));
    if let Some(files) = folded.filter(|f| !f.is_empty()) {
        summary.push('\n');
        summary.push_str(&format_folded_summary(&files));
    }
    Some(summary)
}

/// Fold a `when:write` check's tracked working-copy changes into the just-sealed
/// commit, leaving `@` clean == the amended tip. Returns the folded files for the
/// inline summary, or `None` when the checks left `@` clean (a pure verify check)
/// or the fold could not proceed. Holds the per-store jj lock for the amend so it
/// never races a concurrent base-advance / reconcile / fold on the shared store
/// (the same lock the seal path takes).
///
/// The caller guarantees `@` was clean before the checks ran, so the fold can only
/// ever capture check-made changes, never agent work. A fold that can't proceed
/// (e.g. a concurrent advance left `@` stale) is logged and skipped — the next
/// write's stale recovery is the backstop.
async fn fold_worktree_after_checks(
    orch: &Orchestrator,
    run_context: &RunContext,
    jj: &JjEnv,
    repo_root: &Path,
) -> Option<Vec<String>> {
    // Nothing to fold if the checks left `@` clean.
    if !crate::jj::is_working_copy_dirty(jj, repo_root).unwrap_or(false) {
        return None;
    }
    let store_lock = store_lock_for_checks(orch, run_context, repo_root).await;
    let _guard = match store_lock.as_ref() {
        Some(lock) => Some(lock.lock().await),
        None => None,
    };
    match crate::jj::fold_worktree_into_seal(jj, repo_root) {
        Ok(outcome) => outcome.map(|o| o.folded_files),
        Err(e) => {
            log::warn!(
                "when:write checks: failed to fold check changes into the sealed commit \
                 (the next seal's stale recovery is the backstop): {e}"
            );
            None
        }
    }
}

/// Render the folded-files note appended under the pass/fail line, e.g.
/// `Folded 3 file(s) into the commit: a.ts, b.ts, c.ts`. Caps the listed names so
/// a large reformat doesn't flood the summary. Pure, so it is unit-tested.
fn format_folded_summary(files: &[String]) -> String {
    const MAX_NAMES: usize = 5;
    let shown: Vec<&str> = files.iter().take(MAX_NAMES).map(String::as_str).collect();
    let more = files.len().saturating_sub(shown.len());
    let names = if more > 0 {
        format!("{}, +{more} more", shown.join(", "))
    } else {
        shown.join(", ")
    };
    format!("Folded {} file(s) into the commit: {names}", files.len())
}

/// Resolve the shared-store jj lock for the check runner's worktree, mirroring
/// [`crate::mcp::vcs::resolve_store_lock`] but from a [`RunContext`] (the request
/// is not available here). `None` when the cwd is not a jj worktree or the
/// project's repo path can't be resolved — the discard then proceeds lockless
/// (best-effort).
async fn store_lock_for_checks(
    orch: &Orchestrator,
    run_context: &RunContext,
    repo_root: &Path,
) -> Option<Arc<tokio::sync::Mutex<()>>> {
    if !crate::jj::is_jj_dir(repo_root) {
        return None;
    }
    let repo_path =
        crate::mcp::handlers::run_context::project_path(&orch.db.local, &run_context.project_id)
            .await
            .ok()??;
    let store = crate::jj::project_store_dir(&orch.config_dir, Path::new(&repo_path));
    Some(orch.jj_store_lock(&store))
}

/// Resolve the project's LIVE `checks` contract for the runner.
///
/// The contract source is the project's main-checkout `.cairn/config.yaml`,
/// located by `project_id` (this resolves team replicas through the route table,
/// not just `projects.repo_path`). That is the same file the Settings UI writes,
/// so a project-level edit is visible here on the next commit without restarting
/// the session. This runs in the host orchestrator process (the MCP callback
/// handler), not the fenced agent subprocess, so reading the main checkout is
/// not a fence crossing — the same host-side read `projects::remote` already does.
///
/// Precedence: the live project config wins outright. The worktree's own
/// committed `.cairn/config.yaml` is consulted ONLY as a fallback when the
/// project repo path cannot be resolved (e.g. a team project with no local
/// clone), so an unresolved project never silently drops every check.
pub(crate) async fn load_live_project_checks(
    orch: &Orchestrator,
    project_id: &str,
    worktree_root: &Path,
) -> Option<HashMap<String, CheckCommand>> {
    let project_repo = crate::projects::crud::resolve_local_repo_path_and_key(&orch.db, project_id)
        .await
        .ok()
        .and_then(|(path, _key)| path);
    checks_from_source(project_repo.as_deref().map(Path::new), worktree_root)
}

/// Pick the live `checks` contract given the optionally-resolved project repo
/// path and the worktree fallback. Pure (filesystem reads only, no orchestrator)
/// so the project-wins / worktree-fallback precedence is unit-testable. Both
/// reads use the non-migrating [`load_checks`] so neither path triggers a config
/// migration commit from inside an agent run.
fn checks_from_source(
    project_repo: Option<&Path>,
    worktree_root: &Path,
) -> Option<HashMap<String, CheckCommand>> {
    match project_repo {
        Some(path) => load_checks(path),
        None => load_checks(worktree_root),
    }
}

/// The subset of planned checks that both apply to the change set AND run at the
/// TURN-END cadence. `when:review` (including the `idle` legacy alias) runs at
/// every turn-end; `when:write` never runs here (it is the mid-turn cadence). An
/// impact-scoped check that no changed file matches has `applies == false`. Pure,
/// so the cadence gate is unit-tested.
pub fn applicable_turn_end_checks(
    checks: &HashMap<String, CheckCommand>,
    changed: &[GraphFileChange],
    repo_root: &Path,
) -> Vec<CheckPlan> {
    plan_checks(checks, changed, repo_root)
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

pub fn applicable_write_checks(
    checks: &HashMap<String, CheckCommand>,
    changed: &[GraphFileChange],
    repo_root: &Path,
) -> Vec<CheckPlan> {
    plan_checks(checks, changed, repo_root)
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
    changed: &[GraphFileChange],
    repo_root: &Path,
) -> Option<CheckPlan> {
    let mut one = HashMap::new();
    one.insert(name.to_string(), check.clone());
    plan_checks(&one, changed, repo_root)
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
    impact: Option<&Vec<String>>,
    cumulative: &[GraphFileChange],
    jj: &JjEnv,
    repo_root: &Path,
) -> Vec<GraphFileChange> {
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
    baseline_delta_changed_files(
        Some(latest),
        Some(&baseline_entries),
        Some(current_entries),
        impact,
        cumulative,
    )
}

/// Pure decision rule for choosing a placeholder-selection change set. A passing
/// baseline means the cached verdict covered the baseline tree's impact-matched
/// subset, so the next run only has to select tests/targets reachable from the
/// paths whose matching tree entries changed since then. The baseline row is
/// project-global: it may have been re-stamped by another branch, but comparing
/// tree objects under the same impact globs is still sound. If the other branch's
/// tree differs in extra matching paths, the delta over-includes; it cannot hide a
/// current change from a passing baseline.
fn baseline_delta_changed_files(
    latest: Option<&CheckResultCacheEntry>,
    baseline_entries: Option<&[(String, String)]>,
    current_entries: Option<&[(String, String)]>,
    impact: Option<&Vec<String>>,
    cumulative: &[GraphFileChange],
) -> Vec<GraphFileChange> {
    if !latest.is_some_and(|row| row.passed) {
        return cumulative.to_vec();
    }
    let (Some(baseline), Some(current)) = (baseline_entries, current_entries) else {
        return cumulative.to_vec();
    };
    match diff_tree_entries_for_impact(baseline, current, impact) {
        Some(delta) if !delta.is_empty() => delta,
        _ => cumulative.to_vec(),
    }
}

fn diff_tree_entries_for_impact(
    baseline: &[(String, String)],
    current: &[(String, String)],
    impact: Option<&Vec<String>>,
) -> Option<Vec<GraphFileChange>> {
    let matcher = match impact {
        Some(globs) => Some(crate::execution::selection::build_glob_set(globs).ok()?),
        None => None,
    };
    let is_match = |path: &str| {
        matcher
            .as_ref()
            .map(|set| set.is_match(path))
            .unwrap_or(true)
    };
    let baseline: BTreeMap<&str, &str> = baseline
        .iter()
        .filter(|(path, _)| is_match(path))
        .map(|(path, blob)| (path.as_str(), blob.as_str()))
        .collect();
    let current: BTreeMap<&str, &str> = current
        .iter()
        .filter(|(path, _)| is_match(path))
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
    Some(changes)
}

/// The outcome of one planned check: its exit-code-driven verdict, the parsed
/// per-test detail (enrichment, may be absent), and the retained combined-output
/// tail used as the excerpt fallback. Carried out of [`run_planned_checks`] so
/// the inline summary can render WHAT failed, not just the exit code.
pub struct CheckOutcome {
    pub name: String,
    pub passed: bool,
    pub exit_code: Option<i32>,
    /// Terminal classification for a FAILING check (timeout / spawn error /
    /// signal kill), so a summary renders the real failure, not a bare exit
    /// code. `None` for a pass or an ordinary non-zero exit.
    pub failure_kind: Option<CheckFailureKind>,
    /// Structured per-test result, when the runner's output could be parsed.
    pub parsed: Option<ParsedCheckResult>,
    /// Retained combined-output tail — the excerpt source when the parse carries
    /// no per-failure messages (nextest) or there is no parse at all.
    pub output_tail: String,
    /// Whether this verdict was REUSED from the cache rather than run for this
    /// commit. The summary annotates cache hits so a reused verdict is
    /// distinguishable from a fresh run at a glance.
    pub cached: bool,
    /// Wall-clock duration of the run that produced this verdict, in ms. On a
    /// cache hit this is the stored duration of the original run. Surfaced for
    /// non-test-runner checks (typecheck, api, …) where a test count is not
    /// meaningful.
    pub duration_ms: i64,
}

/// The cache key for one check's verdict: the content identity of ONLY the files
/// in the sealed tree matching that check's impact globs (its "input hash"). A
/// check with NO impact globs keeps whole-tree keying (`tree_hash`), since every
/// change is one of its inputs. When the sealed tree can't be read (`entries` is
/// `None` after a git hiccup), an impact-scoped check falls back to the whole-tree
/// hash — conservative: it re-runs on any change and never falsely reuses a
/// verdict. Glob matching reuses the planner's globset
/// ([`crate::execution::selection::check_input_hash`]), so there is one glob
/// semantics in the codebase.
pub(crate) fn input_hash_for(
    impact: Option<&Vec<String>>,
    entries: Option<&[(String, String)]>,
    tree_hash: &str,
) -> String {
    match impact {
        None => tree_hash.to_string(),
        Some(globs) => match entries {
            Some(entries) => crate::execution::selection::check_input_hash(entries, globs),
            None => tree_hash.to_string(),
        },
    }
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
///   worktree (resolved by the caller's `execute` closure), so they run
///   CONCURRENTLY — unbounded via `join_all` when `max_concurrency` is `None` (the
///   thin write cadence), or capped at `n` in-flight via `buffer_unordered` when it
///   is `Some(n)` (the heavy review cadence). A formatter's writes land in its
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
pub(crate) async fn run_planned_checks<F, Fut, N>(
    db: Arc<LocalDb>,
    project_id: &str,
    tree_hash: &str,
    job_id: &str,
    plans: &[(CheckPlan, String)],
    tool_use_id: &str,
    mode: CheckExecMode,
    execute: F,
    notify: N,
    // Bound on concurrent cache-miss checks in `Isolated` mode: `None` runs them
    // all at once (the thin `when:write` cadence), `Some(n)` caps in-flight misses
    // at `n` (the heavy `when:review` cadence, to avoid oversubscribing CPU/IO).
    max_concurrency: Option<usize>,
) -> Vec<CheckOutcome>
where
    F: Fn(usize, String, String) -> Fut,
    Fut: std::future::Future<Output = Result<CheckExecResult, String>>,
    N: Fn(Vec<CheckStatusEntry>),
{
    // Checklist snapshot, seeded all-`pending` from the plan list. Each check
    // transitions ITS OWN entry and re-emits the whole snapshot, so the live line
    // is self-healing (latest snapshot wins). A std Mutex keeps the transition
    // helper a plain `Fn`; it is only ever locked to mutate + clone and released
    // before the (synchronous) emit, so no guard is held across an await.
    let snapshot: std::sync::Mutex<Vec<CheckStatusEntry>> = std::sync::Mutex::new(
        plans
            .iter()
            .enumerate()
            .map(|(index, (plan, _))| CheckStatusEntry {
                index,
                name: plan.name.clone(),
                state: "pending".to_string(),
                annotation: None,
            })
            .collect(),
    );

    // Transition one entry and re-emit the full snapshot. Scopes the guard so it
    // drops before the emit (which never awaits).
    let transition = |index: usize, state: &str, annotation: Option<String>| {
        let cloned = {
            let mut guard = snapshot.lock().unwrap();
            if let Some(entry) = guard.get_mut(index) {
                entry.state = state.to_string();
                entry.annotation = annotation;
            }
            guard.clone()
        };
        notify(cloned);
    };

    // Emit the planned set (all pending) up front.
    notify(snapshot.lock().unwrap().clone());

    // Phase 1: resolve cache HITS sequentially, and collect the MISS indices to
    // execute. `outcomes` is index-addressed so misses can complete out of order
    // (concurrent `Isolated` mode) and still reassemble into plan order.
    let mut outcomes: Vec<Option<CheckOutcome>> = (0..plans.len()).map(|_| None).collect();
    let mut misses: Vec<usize> = Vec::new();
    for (index, (plan, input_hash)) in plans.iter().enumerate() {
        // Cache hit ⇒ reuse the stored verdict and rehydrate the structured
        // detail; run nothing. The lookup is keyed by the per-check INPUT hash, so
        // a commit that changed none of this check's impact-matched files hits
        // even though the whole-tree hash moved.
        let Ok(Some(entry)) = get_check_result(db.clone(), project_id, &plan.name, input_hash)
        else {
            misses.push(index);
            continue;
        };
        // Re-stamp the row onto the current whole tree so the `/checks` listing
        // (keyed by whole-tree hash) still surfaces this check at the current
        // tree — without re-running it.
        let _ = store_check_result(
            db.clone(),
            CheckResultCacheWrite {
                project_id: project_id.to_string(),
                tree_hash: tree_hash.to_string(),
                input_hash: input_hash.clone(),
                check_name: plan.name.clone(),
                exit_code: entry.exit_code,
                passed: entry.passed,
                output_tail: entry.output_tail.clone(),
                duration_ms: entry.duration_ms,
                target_results_json: entry.target_results_json.clone(),
                job_id: Some(job_id.to_string()),
                cached: Some(true),
                failure_kind: entry.failure_kind.clone(),
            },
        );
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
        };
        // A cache hit jumps straight from pending to its final state.
        transition(
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
    // `execute`), letting `Isolated` mode hold many of these futures at once.
    let run_miss = |index: usize| {
        let db = &db;
        let execute = &execute;
        let transition = &transition;
        async move {
            let (plan, input_hash) = &plans[index];
            transition(index, "running", None);
            let stream_id = crate::mcp::handlers::run::check_stream_id(tool_use_id, index);
            let started = Instant::now();
            let (exit_code, passed, output, failure_kind) =
                match execute(index, plan.command.clone(), stream_id).await {
                    Ok(CheckExecResult {
                        exit_code,
                        output,
                        timed_out,
                    }) => {
                        let passed = exit_code == Some(0);
                        // Classify a failure by HOW it died: killed at its budget
                        // (timeout), or a signal kill with no exit code (a crash
                        // or OOM kill). A pass or an ordinary non-zero exit is
                        // unclassified.
                        let kind = if timed_out {
                            Some(CheckFailureKind::TimedOut)
                        } else if !passed && exit_code.is_none() {
                            Some(CheckFailureKind::Killed)
                        } else {
                            None
                        };
                        (exit_code, passed, output, kind)
                    }
                    // A spawn error / sandbox denial is a clear failure, never a
                    // silent pass — and a legible one: the process never ran.
                    Err(err) => (None, false, err, Some(CheckFailureKind::SpawnError)),
                };
            let duration_ms = started.elapsed().as_millis() as i64;

            // Enrich (fail-closed): a parse only adds detail, never changes `passed`.
            let parsed = parse_check_output(&plan.command, &output);
            let target_results_json = parsed.as_ref().and_then(|p| serde_json::to_string(p).ok());
            let output_tail = tail(&output, OUTPUT_TAIL_CHARS);

            let _ = store_check_result(
                db.clone(),
                CheckResultCacheWrite {
                    project_id: project_id.to_string(),
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
                    failure_kind: failure_kind.map(|k| k.as_str().to_string()),
                },
            );

            let outcome = CheckOutcome {
                name: plan.name.clone(),
                passed,
                exit_code,
                failure_kind,
                parsed,
                output_tail,
                cached: false,
                duration_ms,
            };
            transition(
                index,
                if passed { "passed" } else { "failed" },
                summary_annotation(&outcome),
            );
            (index, outcome)
        }
    };

    // Phase 2: execute the misses — concurrently when isolated, sequentially when
    // sharing the one checkout (the fallback correctness boundary).
    match mode {
        CheckExecMode::Isolated => {
            // Concurrent, optionally bounded. `None` polls every miss at once (the
            // thin write cadence); `Some(n)` keeps at most `n` heavy review checks
            // in flight via `buffer_unordered`. The `run_miss` futures are lazy, so
            // constructing them all and polling <= n at a time is correct, and the
            // index-addressed `outcomes` vec still reassembles into plan order.
            let done: Vec<(usize, CheckOutcome)> = match max_concurrency {
                Some(n) => {
                    use futures_util::StreamExt;
                    // Collect the lazy per-miss futures eagerly (as `join_all`
                    // does) before handing them to `buffer_unordered`; feeding the
                    // borrowing `map` closure straight into the stream combinator
                    // trips a higher-ranked-lifetime inference limit.
                    let futs: Vec<_> = misses.iter().map(|&index| run_miss(index)).collect();
                    futures_util::stream::iter(futs)
                        .buffer_unordered(n.max(1))
                        .collect()
                        .await
                }
                None => {
                    futures_util::future::join_all(misses.iter().map(|&index| run_miss(index)))
                        .await
                }
            };
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

/// Render the inline pass/fail summary appended to the originating tool result.
/// The first line is the compact per-check status
/// (`\u{2713} frontend \u{b7} \u{2717} typecheck (exit 1)`); each failing check
/// then gets a detail block naming the failing tests and a bounded output
/// excerpt, so the agent learns WHAT broke without re-running the suite. Pure, so
/// it is unit-tested directly.
pub fn format_check_summary(results: &[CheckOutcome]) -> String {
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
/// the trust-carrying part of the summary: it turns three indistinguishable
/// greens (a real N-test pass, a zero-selection vacuous pass, and a reused cache
/// hit) into three visibly different lines.
///
/// - Passing TEST-RUNNER check: `12 tests`, or `no tests matched the change`
///   when the selector executed zero tests (a `related` run that matched nothing).
/// - Passing non-test check (tsc/api/dead-code): `4.1s` on a fresh run (duration
///   is the only meaningful signal; a test count would be a lie).
/// - Failing TEST-RUNNER check: `2 of 40 failed, exit 101`.
/// - Failing non-test check: `exit 101`, or `failed to run` on a spawn error.
/// - A cache hit appends `cached` so a reused verdict never masquerades as fresh.
///
/// Pure, so it is unit-tested directly.
fn summary_annotation(o: &CheckOutcome) -> Option<String> {
    let test_parse = o.parsed.as_ref().filter(|p| p.is_test_runner());
    let mut parts: Vec<String> = Vec::new();
    if o.passed {
        match test_parse {
            Some(p) if p.tests_run() == 0 => parts.push("no tests matched the change".to_string()),
            Some(p) => parts.push(format!("{} tests", p.tests_run())),
            // Non-test check: duration is the only honest signal, and only on a
            // fresh run (a cache hit's stored duration would be misleading).
            None if !o.cached && o.duration_ms > 0 => {
                parts.push(format_check_duration(o.duration_ms))
            }
            None => {}
        }
    } else if let Some(kind) = o.failure_kind {
        // A classified death (timeout / spawn error / signal kill) renders AS
        // itself, never a bare `exit -1` the agent would mistake for a real test
        // failure and go debugging tests that never failed.
        parts.push(kind.describe(o.duration_ms));
    } else {
        match test_parse {
            Some(p) => {
                let exit = o
                    .exit_code
                    .map(|c| format!(", exit {c}"))
                    .unwrap_or_default();
                parts.push(format!("{} of {} failed{exit}", p.failed, p.tests_run()));
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

/// The node job's `(base_branch, base_commit)` VCS anchors — the inputs
/// [`node_changed_files`] needs to diff `fork..@`. Mirrors the projection in
/// `resources/files.rs`; empty/missing values fall through to `None`.
pub(crate) async fn load_node_vcs_anchors(
    db: &LocalDb,
    job_id: &str,
) -> (Option<String>, Option<String>) {
    let job_id = job_id.to_string();
    let row = db
        .read(|conn| {
            let job_id = job_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT base_branch, base_commit FROM jobs WHERE id = ?1 LIMIT 1",
                        (job_id.as_str(),),
                    )
                    .await?;
                match rows.next().await? {
                    Some(row) => Ok(Some((row.opt_text(0)?, row.opt_text(1)?))),
                    None => Ok(None),
                }
            })
        })
        .await;
    match row {
        Ok(Some((branch, commit))) => (
            branch.filter(|s| !s.is_empty()),
            commit.filter(|s| !s.is_empty()),
        ),
        _ => (None, None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::project_settings::CheckPolicy;
    use crate::execution::selection::CheckScope;
    use std::sync::atomic::{AtomicUsize, Ordering};

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
            policy: CheckPolicy::Advisory,
            when,
            timeout: None,
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
        let plans =
            applicable_write_checks(&repo_checks(), &[change("src/App.tsx")], Path::new("/repo"));
        let names: Vec<&str> = plans.iter().map(|p| p.name.as_str()).collect();
        assert_eq!(names, vec!["frontend", "typecheck"]);
    }

    #[test]
    fn gate_is_empty_for_a_doc_only_change() {
        let plans =
            applicable_write_checks(&repo_checks(), &[change("docs/x.md")], Path::new("/repo"));
        assert!(plans.is_empty(), "a doc-only commit triggers no checks");
    }

    #[test]
    fn gate_excludes_review_checks_for_a_rust_change() {
        let plans = applicable_write_checks(
            &repo_checks(),
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

    // --- contract source: live project config wins over the worktree -------

    /// Write a minimal `.cairn/config.yaml` declaring one `when:write` check.
    fn write_checks_config(dir: &Path, check_name: &str) {
        let path = crate::config::project_settings::get_project_config_path(dir);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(
            &path,
            format!(
                "checks:\n  {check_name}:\n    command: run-it\n    impact:\n      - src/**\n    when: write\n"
            ),
        )
        .unwrap();
    }

    #[test]
    fn checks_from_source_prefers_project_over_worktree() {
        let project = tempfile::TempDir::new().unwrap();
        let worktree = tempfile::TempDir::new().unwrap();
        // Project (live) config and the worktree's committed config disagree:
        // the project edited its checks after this branch was cut.
        write_checks_config(project.path(), "frontend");
        write_checks_config(worktree.path(), "typecheck");

        let checks = checks_from_source(Some(project.path()), worktree.path()).unwrap();
        assert!(
            checks.contains_key("frontend"),
            "the live project contract must win"
        );
        assert!(
            !checks.contains_key("typecheck"),
            "the stale worktree contract must not leak in"
        );
    }

    #[test]
    fn checks_from_source_falls_back_to_worktree_when_project_unresolved() {
        let worktree = tempfile::TempDir::new().unwrap();
        write_checks_config(worktree.path(), "typecheck");
        // No resolvable project repo path (e.g. a team project with no local
        // clone): fall back to the worktree's own committed contract rather than
        // silently dropping every check.
        let checks = checks_from_source(None, worktree.path()).unwrap();
        assert!(checks.contains_key("typecheck"));
    }

    #[test]
    fn checks_from_source_none_when_neither_declares_checks() {
        let project = tempfile::TempDir::new().unwrap();
        let worktree = tempfile::TempDir::new().unwrap();
        assert!(checks_from_source(Some(project.path()), worktree.path()).is_none());
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

        let delta = diff_tree_entries_for_impact(&baseline, &current, Some(&impact)).unwrap();
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

    #[test]
    fn baseline_decision_uses_delta_only_from_passing_baseline() {
        let baseline = vec![("src/a.ts".to_string(), "a1".to_string())];
        let current = vec![
            ("src/a.ts".to_string(), "a1".to_string()),
            ("src/b.ts".to_string(), "b1".to_string()),
        ];
        let impact = vec!["src/**".to_string()];
        let cumulative = vec![change("src/a.ts"), change("src/b.ts")];
        let passing = cache_entry("frontend", "tree-a", true);
        let failing = cache_entry("frontend", "tree-a", false);

        let narrowed = baseline_delta_changed_files(
            Some(&passing),
            Some(&baseline),
            Some(&current),
            Some(&impact),
            &cumulative,
        );
        assert_eq!(
            narrowed.iter().map(|c| c.path.as_str()).collect::<Vec<_>>(),
            vec!["src/b.ts"]
        );

        let from_fail = baseline_delta_changed_files(
            Some(&failing),
            Some(&baseline),
            Some(&current),
            Some(&impact),
            &cumulative,
        );
        assert_eq!(from_fail, cumulative);

        let from_missing = baseline_delta_changed_files(
            None,
            Some(&baseline),
            Some(&current),
            Some(&impact),
            &cumulative,
        );
        assert_eq!(from_missing, cumulative);
    }

    #[test]
    fn baseline_decision_falls_back_to_cumulative_on_empty_or_uncertain_delta() {
        let entries = vec![("src/a.ts".to_string(), "a1".to_string())];
        let impact = vec!["src/**".to_string()];
        let cumulative = vec![change("src/a.ts")];
        let passing = cache_entry("frontend", "tree-a", true);

        let empty_delta = baseline_delta_changed_files(
            Some(&passing),
            Some(&entries),
            Some(&entries),
            Some(&impact),
            &cumulative,
        );
        assert_eq!(empty_delta, cumulative);

        let unreadable_current = baseline_delta_changed_files(
            Some(&passing),
            Some(&entries),
            None,
            Some(&impact),
            &cumulative,
        );
        assert_eq!(unreadable_current, cumulative);
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
        let impact = vec!["src/**".to_string()];
        let cumulative = vec![change("src/f1.ts"), change("src/f2.ts")];
        let passing = cache_entry("frontend", "tree-a", true);
        let selected = baseline_delta_changed_files(
            Some(&passing),
            Some(&baseline),
            Some(&current),
            Some(&impact),
            &cumulative,
        );
        let check = check(
            "bunx vitest related --reporter=json {changedFiles}",
            Some(&["src/**"]),
            CheckWhen::Write,
        );

        let plan = replan_one_check("frontend", &check, &selected, Path::new("/repo")).unwrap();
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
        let plans =
            applicable_turn_end_checks(&checks, &[change("src/App.tsx")], Path::new("/repo"));
        let names: Vec<&str> = plans.iter().map(|p| p.name.as_str()).collect();
        assert_eq!(names, vec!["legacy"]);
    }

    #[test]
    fn turn_end_gate_excludes_a_non_matching_impact() {
        // A doc-only change matches no impact glob, so nothing applies.
        let plans = applicable_turn_end_checks(
            &cadence_checks(),
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
            duration_ms: 0,
        }
    }

    /// A test-runner parse with explicit pass/fail counts, so the summary's
    /// count-bearing annotations are exercised without a real runner.
    fn runner_parse(parser: &str, passed: usize, failed: usize) -> ParsedCheckResult {
        ParsedCheckResult {
            parser: parser.to_string(),
            passed,
            failed,
            skipped: 0,
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
            duration_ms: 0,
        }];
        let s = format_check_summary(&results);
        // Header status line first.
        assert!(s.starts_with("\u{2717} typecheck (exit 1)"));
        // Then a detail block naming the failing test and quoting the error.
        assert!(s.contains("\u{2717} typecheck \u{2014} 1 failed: a.ts(1,7)"));
        assert!(s.contains("TS2322: Type 'string' is not assignable"));
    }

    #[test]
    fn folded_summary_lists_files_and_caps_the_names() {
        // A short list is rendered in full.
        assert_eq!(
            format_folded_summary(&["a.ts".to_string(), "b.ts".to_string()]),
            "Folded 2 file(s) into the commit: a.ts, b.ts"
        );
        // A long list caps the listed names but keeps the true total.
        let many: Vec<String> = (0..8).map(|i| format!("f{i}.ts")).collect();
        let rendered = format_folded_summary(&many);
        assert!(rendered.starts_with("Folded 8 file(s) into the commit: "));
        assert!(rendered.contains("f0.ts") && rendered.contains("f4.ts"));
        assert!(rendered.contains("+3 more"));
        assert!(!rendered.contains("f5.ts"));
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
            duration_ms: 0,
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

    // --- timeout budgets + failure classification -------------------------

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
        CheckPlan {
            name: name.to_string(),
            applies: true,
            command: command.to_string(),
            scope: CheckScope::Full,
        }
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
        })
    }

    #[tokio::test]
    async fn cache_hit_skips_execution() {
        let db = cache_db().await;
        store_check_result(
            db.clone(),
            CheckResultCacheWrite {
                project_id: "project-a".to_string(),
                tree_hash: "tree-a".to_string(),
                input_hash: "ih-frontend".to_string(),
                check_name: "frontend".to_string(),
                exit_code: 0,
                passed: true,
                output_tail: "cached".to_string(),
                duration_ms: 1,
                target_results_json: None,
                job_id: Some("job-a".to_string()),
                cached: Some(false),
                failure_kind: None,
            },
        )
        .unwrap();

        let plans = vec![(
            plan("frontend", "bunx vitest run"),
            "ih-frontend".to_string(),
        )];
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = calls.clone();
        let results = run_planned_checks(
            db.clone(),
            "project-a",
            "tree-a",
            "job-a",
            &plans,
            "tool",
            CheckExecMode::Shared,
            move |_index, _command, _stream_id| {
                let counted = counted.clone();
                async move {
                    counted.fetch_add(1, Ordering::SeqCst);
                    exec_ok(Some(0), "ran")
                }
            },
            |_| {},
            None,
        )
        .await;

        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "a cache hit must not re-run the check"
        );
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name, "frontend");
        assert!(results[0].passed);
        assert_eq!(results[0].exit_code, Some(0));
    }

    #[tokio::test]
    async fn cache_miss_runs_then_stores() {
        let db = cache_db().await;
        let plans = vec![(plan("frontend", "bunx vitest run"), "ih-b".to_string())];
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
            move |_index, _command, _stream_id| {
                let counted = counted.clone();
                async move {
                    counted.fetch_add(1, Ordering::SeqCst);
                    exec_ok(Some(1), "vitest failed")
                }
            },
            |_| {},
            None,
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

        let stored = get_check_result(db, "project-a", "frontend", "ih-b")
            .unwrap()
            .expect("a miss stores the result");
        assert_eq!(stored.exit_code, 1);
        assert!(!stored.passed);
        assert_eq!(stored.output_tail, "vitest failed");
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
            move |_index, _command, _stream_id| {
                let out = nextest_output.clone();
                async move { exec_ok(Some(100), out) }
            },
            |_| {},
            None,
        )
        .await;

        // The outcome carries the parsed per-test detail.
        let parsed = results[0].parsed.as_ref().expect("nextest output parses");
        assert_eq!(parsed.parser, "nextest");
        assert_eq!(parsed.failed, 2);
        assert_eq!(parsed.failures.len(), 2);

        // And it is persisted in target_results_json for future baseline work.
        let stored = get_check_result(db, "project-a", "rust", "ih-structured")
            .unwrap()
            .expect("a miss stores the result");
        let json = stored
            .target_results_json
            .expect("structured results persisted");
        assert!(json.contains("\"parser\":\"nextest\""));
        assert!(json.contains("mycrate mod::test_a"));
    }

    /// The repro this whole change fixes. A src-tauri commit runs the rust check
    /// and caches its verdict keyed by the src-tauri input hash. A following
    /// doc-only commit moves the WHOLE-tree hash but leaves that input hash
    /// unchanged, so the verdict is a cache HIT — rust does not re-run — and the
    /// row is re-stamped onto the doc commit's tree so the `/checks` listing still
    /// surfaces it.
    #[tokio::test]
    async fn doc_only_commit_reuses_impact_scoped_verdict() {
        let db = cache_db().await;
        let calls = Arc::new(AtomicUsize::new(0));

        // Commit 1 touches src-tauri: rust runs and caches its verdict for input
        // hash IH1 at whole-tree tree-1.
        let plans = vec![(plan("rust", "bun run test:rust"), "IH1".to_string())];
        let counted = calls.clone();
        let r1 = run_planned_checks(
            db.clone(),
            "project-a",
            "tree-1",
            "job-a",
            &plans,
            "tool",
            CheckExecMode::Shared,
            move |_index, _command, _stream_id| {
                let counted = counted.clone();
                async move {
                    counted.fetch_add(1, Ordering::SeqCst);
                    exec_ok(Some(0), "ran")
                }
            },
            |_| {},
            None,
        )
        .await;
        assert_eq!(r1.len(), 1);
        assert_eq!(r1[0].name, "rust");
        assert!(r1[0].passed);
        assert_eq!(r1[0].exit_code, Some(0));
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        // Commit 2 is doc-only: the whole tree changes to tree-2, but the rust
        // input hash is UNCHANGED (still IH1), so the verdict is a cache hit and
        // the check does not re-run.
        let plans2 = vec![(plan("rust", "bun run test:rust"), "IH1".to_string())];
        let counted = calls.clone();
        let r2 = run_planned_checks(
            db.clone(),
            "project-a",
            "tree-2",
            "job-a",
            &plans2,
            "tool",
            CheckExecMode::Shared,
            move |_index, _command, _stream_id| {
                let counted = counted.clone();
                async move {
                    counted.fetch_add(1, Ordering::SeqCst);
                    exec_ok(Some(0), "ran")
                }
            },
            |_| {},
            None,
        )
        .await;
        assert_eq!(r2.len(), 1);
        assert_eq!(r2[0].name, "rust");
        assert!(r2[0].passed);
        assert_eq!(r2[0].exit_code, Some(0));
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "a doc-only commit must not re-run the rust check"
        );

        // The reused verdict was re-stamped onto the doc commit's tree, so the
        // tree-keyed `/checks` listing surfaces rust at the current tree.
        let stamped = get_check_result(db.clone(), "project-a", "rust", "IH1")
            .unwrap()
            .expect("the verdict is still cached");
        assert_eq!(stamped.tree_hash, "tree-2");
        let rows = crate::execution::cache::list_check_results(db, "project-a", "tree-2").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].check_name, "rust");
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
            move |index, _command, _stream_id| async move {
                match index {
                    // Killed at its budget, with a nextest SLOW line naming the
                    // test in flight at the kill.
                    0 => Ok(CheckExecResult {
                        exit_code: None,
                        output: "     SLOW [>  60.000s] mycrate mod::hangs\nstill going"
                            .to_string(),
                        timed_out: true,
                    }),
                    // The process could not be spawned.
                    1 => Err("Failed to spawn command: No such file or directory".to_string()),
                    // Died by signal mid-run (no exit code, not a timeout).
                    _ => Ok(CheckExecResult {
                        exit_code: None,
                        output: "segfault".to_string(),
                        timed_out: false,
                    }),
                }
            },
            |_| {},
            None,
        )
        .await;

        assert_eq!(results[0].failure_kind, Some(CheckFailureKind::TimedOut));
        assert_eq!(results[1].failure_kind, Some(CheckFailureKind::SpawnError));
        assert_eq!(results[2].failure_kind, Some(CheckFailureKind::Killed));
        assert!(results.iter().all(|o| !o.passed));

        // The classification is persisted, so every downstream surface can render
        // the real death rather than re-deriving it from exit -1.
        let kind_of = |name: &str, ih: &str| {
            get_check_result(db.clone(), "project-a", name, ih)
                .unwrap()
                .unwrap()
                .failure_kind
        };
        assert_eq!(kind_of("slow", "ih-slow").as_deref(), Some("timed_out"));
        assert_eq!(kind_of("nogo", "ih-nogo").as_deref(), Some("spawn_error"));
        assert_eq!(kind_of("crash", "ih-crash").as_deref(), Some("killed"));

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
        store_check_result(
            db.clone(),
            CheckResultCacheWrite {
                project_id: "project-a".to_string(),
                tree_hash: "tree-a".to_string(),
                input_hash: "ih-frontend".to_string(),
                check_name: "frontend".to_string(),
                exit_code: 0,
                passed: true,
                output_tail: String::new(),
                duration_ms: 1,
                target_results_json: None,
                job_id: Some("job-a".to_string()),
                cached: Some(false),
                failure_kind: None,
            },
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
            // typecheck misses and fails with a bare exit code.
            move |_index, _command, _stream_id| async move { exec_ok(Some(1), "boom") },
            move |checks| captured.lock().unwrap().push(checks),
            None,
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
            move |_index, _command, _stream_id| async move { exec_ok(Some(0), String::new()) },
            |_| {},
            None,
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

    /// The `Shared` FALLBACK must never overlap two check commands in the one
    /// checkout: a mutating check (a formatter / `--fix` lint) has to settle
    /// before the next check observes the shared sealed worktree. Even when each
    /// executor yields at an await, the concurrent-invocation high-water mark
    /// stays 1. This guards the fallback path against the formatter/reader race.
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
            None,
        )
        .await;

        assert_eq!(
            high_water.load(Ordering::SeqCst),
            1,
            "shared-mode checks must not overlap; exactly one may run at a time"
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
                move |_index, _command, _stream_id| {
                    let b = b.clone();
                    async move {
                        b.wait().await;
                        exec_ok(Some(0), String::new())
                    }
                },
                |_| {},
                None,
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
            // index 0 sleeps longest, so completion order reverses plan order.
            move |index, _command, _stream_id| async move {
                let delay = (3 - index as u64) * 20;
                tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                exec_ok(Some(0), String::new())
            },
            |_| {},
            None,
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
            // z fails, the others pass — mixed final states under concurrency.
            move |index, _command, _stream_id| async move {
                let code = if index == 2 { 1 } else { 0 };
                exec_ok(Some(code), String::new())
            },
            move |checks| captured.lock().unwrap().push(checks),
            None,
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

    /// Bounded `Isolated` mode caps in-flight misses: with `Some(2)` over 3 misses
    /// the concurrent high-water mark never exceeds 2, yet still reaches 2 (genuine
    /// overlap, not accidental sequencing).
    #[tokio::test]
    async fn bounded_isolated_never_exceeds_cap() {
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
            Some(2),
        )
        .await;

        assert_eq!(results.len(), 3);
        assert_eq!(
            high_water.load(Ordering::SeqCst),
            2,
            "bounded isolated concurrency must reach but never exceed the cap of 2"
        );
    }

    /// A bound of `Some(1)` makes `Isolated` mode effectively sequential: at most
    /// one miss runs at a time.
    #[tokio::test]
    async fn bounded_isolated_one_is_sequential() {
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
        run_planned_checks(
            db.clone(),
            "project-a",
            "tree-seq1",
            "job-a",
            &plans,
            "tool",
            CheckExecMode::Isolated,
            move |_index, _command, _stream_id| {
                let a = a.clone();
                let hw = hw.clone();
                async move {
                    let now = a.fetch_add(1, Ordering::SeqCst) + 1;
                    hw.fetch_max(now, Ordering::SeqCst);
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    a.fetch_sub(1, Ordering::SeqCst);
                    exec_ok(Some(0), String::new())
                }
            },
            |_| {},
            Some(1),
        )
        .await;

        assert_eq!(
            high_water.load(Ordering::SeqCst),
            1,
            "a cap of 1 must serialize isolated misses"
        );
    }
}
