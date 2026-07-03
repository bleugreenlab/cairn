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
//! Only the `when:write` cadence runs here; `when:idle`/`when:review` run at
//! turn-end ([`crate::execution::checks_turn_end`]). A check passes iff its
//! command exits `0` — output parsing ([`crate::execution::check_parsers`]) is
//! pure enrichment (failing test names + excerpt) and never changes a verdict;
//! a spawn error or sandbox denial is a clear failure, never a silent pass.
//! Placeholder selectors narrow to the delta since the check's last PASSING
//! baseline and fall back to the cumulative branch diff on any uncertainty (see
//! `baseline_delta_changed_files`). Checks are invoked through the `run` verb's
//! process machinery directly (not `run_one`), so a sandbox-blocked syscall
//! surfaces as a failed exit rather than an interactive fence prompt.
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
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use crate::config::project_settings::{load_checks, CheckCommand, CheckWhen};
use crate::execution::cache::{
    get_check_result, list_latest_check_results_for_project, store_check_result,
    CheckResultCacheEntry, CheckResultCacheWrite,
};
use crate::execution::check_parsers::{
    format_failure_excerpt, format_failure_names, parse_check_output, ParsedCheckResult,
};
use crate::execution::selection::{plan_checks, CheckPlan};
use crate::jj::{
    node_changed_files, sealed_tree_entries, sealed_tree_hash, tree_entries, GraphFileChange, JjEnv,
};
use crate::mcp::handlers::RunContext;
use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};

/// Per-check time cap. vitest/tsc can run a while; this mirrors the `run` verb's
/// generous per-item ceiling.
const CHECK_TIMEOUT_MS: u32 = 600_000;

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

    let results = run_planned_checks(
        orch.db.local.clone(),
        &run_context.project_id,
        &tree_hash,
        run_context.job_id.as_str(),
        &keyed,
        tool_use_id,
        move |command, stream_id| async move {
            crate::mcp::handlers::run::run_check_command(
                orch,
                cwd,
                &stream_id,
                Some(run_context),
                &command,
                CHECK_TIMEOUT_MS,
            )
            .await
        },
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
/// `write` cadence. This is the gate: `when:idle`/`when:review` are excluded, and an
/// impact-scoped check that no changed file matches has `applies == false`. With
/// every `when:write` check impact-scoped, a doc-only / non-source commit yields
/// an empty set, so nothing runs.
/// The subset of planned checks that both apply to the change set AND run at a
/// TURN-END cadence, given whether the node currently has an open PR. `when:idle`
/// runs at every work-turn-end; `when:review` runs only when a PR is open;
/// `when:write` never runs here (it is the mid-turn cadence). Pure, so the
/// cadence + pr_open gate is unit-tested.
pub fn applicable_turn_end_checks(
    checks: &HashMap<String, CheckCommand>,
    changed: &[GraphFileChange],
    repo_root: &Path,
    pr_open: bool,
) -> Vec<CheckPlan> {
    plan_checks(checks, changed, repo_root)
        .into_iter()
        .filter(|plan| plan.applies)
        .filter(|plan| {
            checks
                .get(&plan.name)
                .is_some_and(|check| match check.when {
                    CheckWhen::Idle => true,
                    CheckWhen::Review => pr_open,
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
/// A miss parses the runner's output into structured per-test results
/// ([`parse_check_output`]) and persists them in the cache row's
/// `target_results_json`; a hit rehydrates that column. Parsing is pure
/// enrichment — `passed` / `exit_code` stay exit-code-driven either way, so a
/// parser miss can never turn a failing exit into a pass.
async fn run_planned_checks<F, Fut>(
    db: Arc<LocalDb>,
    project_id: &str,
    tree_hash: &str,
    job_id: &str,
    plans: &[(CheckPlan, String)],
    tool_use_id: &str,
    execute: F,
) -> Vec<CheckOutcome>
where
    F: Fn(String, String) -> Fut,
    Fut: std::future::Future<Output = Result<(Option<i32>, String), String>>,
{
    let mut results = Vec::with_capacity(plans.len());
    for (index, (plan, input_hash)) in plans.iter().enumerate() {
        // Cache hit ⇒ reuse the stored verdict and rehydrate the structured
        // detail; run nothing. The lookup is keyed by the per-check INPUT hash, so
        // a commit that changed none of this check's impact-matched files hits even
        // though the whole-tree hash moved.
        if let Ok(Some(entry)) = get_check_result(db.clone(), project_id, &plan.name, input_hash) {
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
                },
            );
            // Rehydrate the structured per-test detail persisted at run time.
            let parsed = entry
                .target_results_json
                .as_deref()
                .and_then(|s| serde_json::from_str::<ParsedCheckResult>(s).ok());
            results.push(CheckOutcome {
                name: plan.name.clone(),
                passed: entry.passed,
                exit_code: Some(entry.exit_code),
                parsed,
                output_tail: entry.output_tail,
                cached: true,
                duration_ms: entry.duration_ms,
            });
            continue;
        }

        // Miss ⇒ run to completion (streaming) and record the result keyed by the
        // input hash, stamped with the current whole tree.
        let stream_id = crate::mcp::handlers::run::check_stream_id(tool_use_id, index);
        let started = Instant::now();
        let (exit_code, passed, output) = match execute(plan.command.clone(), stream_id).await {
            Ok((code, combined)) => (code, code == Some(0), combined),
            // A spawn error / sandbox denial is a clear failure, never a silent pass.
            Err(err) => (None, false, err),
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
            },
        );

        results.push(CheckOutcome {
            name: plan.name.clone(),
            passed,
            exit_code,
            parsed,
            output_tail,
            cached: false,
            duration_ms,
        });
    }
    results
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
fn format_check_detail(o: &CheckOutcome) -> Option<String> {
    let names = o.parsed.as_ref().and_then(format_failure_names);
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
            "i".to_string(),
            check("run-i", Some(&["src/**"]), CheckWhen::Idle),
        );
        checks.insert(
            "r".to_string(),
            check("run-r", Some(&["src/**"]), CheckWhen::Review),
        );
        checks
    }

    #[test]
    fn turn_end_gate_runs_idle_but_not_review_without_a_pr() {
        let plans = applicable_turn_end_checks(
            &cadence_checks(),
            &[change("src/App.tsx")],
            Path::new("/repo"),
            false,
        );
        let names: Vec<&str> = plans.iter().map(|p| p.name.as_str()).collect();
        assert_eq!(
            names,
            vec!["i"],
            "idle runs; review is gated off, write never"
        );
    }

    #[test]
    fn turn_end_gate_adds_review_when_a_pr_is_open() {
        let plans = applicable_turn_end_checks(
            &cadence_checks(),
            &[change("src/App.tsx")],
            Path::new("/repo"),
            true,
        );
        let names: Vec<&str> = plans.iter().map(|p| p.name.as_str()).collect();
        assert_eq!(names, vec!["i", "r"], "idle + review run; write never");
    }

    #[test]
    fn turn_end_gate_excludes_a_non_matching_impact() {
        // A doc-only change matches no impact glob, so nothing applies even with
        // a PR open.
        let plans = applicable_turn_end_checks(
            &cadence_checks(),
            &[change("docs/x.md")],
            Path::new("/repo"),
            true,
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
            move |_command, _stream_id| {
                let counted = counted.clone();
                async move {
                    counted.fetch_add(1, Ordering::SeqCst);
                    Ok((Some(0), "ran".to_string()))
                }
            },
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
            move |_command, _stream_id| {
                let counted = counted.clone();
                async move {
                    counted.fetch_add(1, Ordering::SeqCst);
                    Ok((Some(1), "vitest failed".to_string()))
                }
            },
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
            move |_command, _stream_id| {
                let out = nextest_output.clone();
                async move { Ok((Some(100), out)) }
            },
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
            move |_command, _stream_id| {
                let counted = counted.clone();
                async move {
                    counted.fetch_add(1, Ordering::SeqCst);
                    Ok((Some(0), "ran".to_string()))
                }
            },
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
            move |_command, _stream_id| {
                let counted = counted.clone();
                async move {
                    counted.fetch_add(1, Ordering::SeqCst);
                    Ok((Some(0), "ran".to_string()))
                }
            },
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
}
