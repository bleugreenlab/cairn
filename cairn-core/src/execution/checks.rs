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
//! ## Scope (Wave 3)
//!
//! Only the `when:write` cadence runs here; `when:idle`/`when:review` are out of
//! scope. There is no regression-delta/baseline/inheritance and no per-test
//! parsing — a check passes iff its command exits `0`, and a spawn error or
//! sandbox denial is a clear failure, never a silent pass. Checks are invoked
//! through the `run` verb's process machinery directly (not `run_one`), so a
//! sandbox-blocked syscall surfaces as a failed exit rather than an interactive
//! fence prompt; routing the fence-prompting `when:review` cargo check through the
//! suspend/resume re-drive is deferred to Wave 4.
//!
//! ## Cache key
//!
//! The cache is keyed by the sealed tree identity ([`crate::jj::sealed_tree_hash`]),
//! which is the git tree object of the sealed `@-` commit — a genuine content
//! hash. Two separately-created commits with identical tree content therefore
//! share a key, so a clean rebase/squash that preserves file content reuses the
//! prior verdict instead of re-running. (If the git-backend resolution ever
//! fails, `sealed_tree_hash` falls back to the commit id: still correct, just
//! per-commit rather than per-tree.)

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use crate::config::project_settings::{load_checks, CheckCommand, CheckWhen};
use crate::execution::cache::{get_check_result, store_check_result, CheckResultCacheWrite};
use crate::execution::selection::{plan_checks, CheckPlan};
use crate::jj::{node_changed_files, sealed_tree_hash, GraphFileChange, JjEnv};
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

    // 3 + 4. Plan, then filter to the applicable `when:write` checks (the gate).
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

    let results = run_planned_checks(
        orch.db.local.clone(),
        &run_context.project_id,
        &tree_hash,
        &plans,
        tool_use_id,
        move |command, stream_id| async move {
            crate::mcp::handlers::bash::run_check_command(
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

/// Execute the planned checks against the sealed tree, consulting the cache
/// first. Generic over the spawn closure so the cache hit/miss behavior is
/// unit-testable without spawning a real process. Returns `(name, passed,
/// exit_code)` per check in plan order.
async fn run_planned_checks<F, Fut>(
    db: Arc<LocalDb>,
    project_id: &str,
    tree_hash: &str,
    plans: &[CheckPlan],
    tool_use_id: &str,
    execute: F,
) -> Vec<(String, bool, Option<i32>)>
where
    F: Fn(String, String) -> Fut,
    Fut: std::future::Future<Output = Result<(Option<i32>, String), String>>,
{
    let mut results = Vec::with_capacity(plans.len());
    for (index, plan) in plans.iter().enumerate() {
        // Cache hit ⇒ use the stored verdict, run nothing.
        if let Ok(Some(entry)) = get_check_result(db.clone(), project_id, tree_hash, &plan.name) {
            results.push((plan.name.clone(), entry.passed, Some(entry.exit_code)));
            continue;
        }

        // Miss ⇒ run to completion (streaming) and record the result.
        let stream_id = crate::mcp::handlers::bash::run_item_stream_id(tool_use_id, index);
        let started = Instant::now();
        let (exit_code, passed, output) = match execute(plan.command.clone(), stream_id).await {
            Ok((code, combined)) => (code, code == Some(0), combined),
            // A spawn error / sandbox denial is a clear failure, never a silent pass.
            Err(err) => (None, false, err),
        };
        let duration_ms = started.elapsed().as_millis() as i64;

        let _ = store_check_result(
            db.clone(),
            CheckResultCacheWrite {
                project_id: project_id.to_string(),
                tree_hash: tree_hash.to_string(),
                check_name: plan.name.clone(),
                exit_code: exit_code.unwrap_or(-1),
                passed,
                output_tail: tail(&output, OUTPUT_TAIL_CHARS),
                duration_ms,
                target_results_json: None,
            },
        );

        results.push((plan.name.clone(), passed, exit_code));
    }
    results
}

/// Render the per-check pass/fail line, e.g. `\u{2713} frontend \u{b7} \u{2717}
/// typecheck (exit 1)`. Pure, so it is unit-tested directly.
pub fn format_check_summary(results: &[(String, bool, Option<i32>)]) -> String {
    results
        .iter()
        .map(|(name, passed, exit_code)| {
            if *passed {
                format!("\u{2713} {name}")
            } else {
                match exit_code {
                    Some(code) => format!("\u{2717} {name} (exit {code})"),
                    None => format!("\u{2717} {name} (failed to run)"),
                }
            }
        })
        .collect::<Vec<_>>()
        .join(" \u{b7} ")
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

    #[test]
    fn summary_renders_pass_and_fail() {
        let s = format_check_summary(&[
            ("frontend".to_string(), true, Some(0)),
            ("typecheck".to_string(), false, Some(1)),
        ]);
        assert_eq!(s, "\u{2713} frontend \u{b7} \u{2717} typecheck (exit 1)");
    }

    #[test]
    fn summary_renders_spawn_failure_without_exit_code() {
        let s = format_check_summary(&[("frontend".to_string(), false, None)]);
        assert_eq!(s, "\u{2717} frontend (failed to run)");
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
                check_name: "frontend".to_string(),
                exit_code: 0,
                passed: true,
                output_tail: "cached".to_string(),
                duration_ms: 1,
                target_results_json: None,
            },
        )
        .unwrap();

        let plans = vec![plan("frontend", "bunx vitest run")];
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = calls.clone();
        let results = run_planned_checks(
            db.clone(),
            "project-a",
            "tree-a",
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
        assert_eq!(results, vec![("frontend".to_string(), true, Some(0))]);
    }

    #[tokio::test]
    async fn cache_miss_runs_then_stores() {
        let db = cache_db().await;
        let plans = vec![plan("frontend", "bunx vitest run")];
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = calls.clone();
        let results = run_planned_checks(
            db.clone(),
            "project-a",
            "tree-b",
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
        assert_eq!(results, vec![("frontend".to_string(), false, Some(1))]);

        let stored = get_check_result(db, "project-a", "tree-b", "frontend")
            .unwrap()
            .expect("a miss stores the result");
        assert_eq!(stored.exit_code, 1);
        assert!(!stored.passed);
        assert_eq!(stored.output_tail, "vitest failed");
    }
}
