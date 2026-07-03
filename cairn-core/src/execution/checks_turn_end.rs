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
//! `when:idle` checks run at every work-turn-end; `when:review` checks run only
//! when the node has an OPEN PR (see [`crate::orchestrator::attention_push::has_open_pr_for_issue`]).
//! `when:write` never runs here. Selection reuses the write cadence's machinery
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
use std::time::Instant;

use cairn_common::uri::build_node_checks_uri;

use crate::execution::cache::{
    get_check_result, list_check_results, store_check_result, CheckResultCacheWrite,
};
use crate::execution::check_parsers::{
    format_failure_excerpt, format_failure_names, parse_check_output, ParsedCheckResult,
};
use crate::execution::checks::{
    applicable_turn_end_checks, input_hash_for, load_live_project_checks,
};
use crate::execution::selection::CheckPlan;
use crate::jj::{node_changed_files, sealed_tree_entries, sealed_tree_hash, JjEnv};
use crate::orchestrator::{attention_push, Orchestrator};
use crate::storage::{LocalDb, RowExt};

/// Per-check time cap — mirrors the write cadence's generous ceiling.
const CHECK_TIMEOUT_MS: u32 = 600_000;
/// Chars of combined output retained per check in the cache row's `output_tail`.
const OUTPUT_TAIL_CHARS: usize = 4_000;
/// Chars of the live log file surfaced in the "running" render.
const LOG_TAIL_CHARS: usize = 2_000;

/// Background entry point: run the affected turn-end checks for a job, then
/// release the single-flight slot. The caller ([`spawn_turn_end_checks`] in
/// lifecycle) has already claimed the slot via `try_begin_turn_end_checks`; this
/// function is responsible for releasing it on every path.
pub async fn run_turn_end_checks(orch: Orchestrator, job_id: String) {
    if let Err(e) = run_turn_end_checks_inner(&orch, &job_id).await {
        log::warn!(
            "turn-end checks for job {}: {}",
            &job_id[..job_id.len().min(8)],
            e
        );
    }
    orch.end_turn_end_checks(&job_id);
}

async fn run_turn_end_checks_inner(orch: &Orchestrator, job_id: &str) -> Result<(), String> {
    // 1. Resolve the node's coordinates (project, issue, worktree, base anchors).
    let Some(coords) = resolve_job_coords(&orch.db.local, job_id).await? else {
        return Ok(());
    };
    let Some(worktree_path) = coords.worktree_path.clone().filter(|p| !p.is_empty()) else {
        log::debug!(
            "turn-end checks for job {}: no worktree; nothing to run",
            short_id(job_id)
        );
        return Ok(());
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
            return Ok(());
        }
    };

    // 3. Is a PR open? Gates the `when:review` cadence.
    let owning = crate::execution::routing::owning_db_for_job(&orch.db, job_id)
        .await
        .map_err(|e| e.to_string())?;
    let pr_open = attention_push::has_open_pr_for_issue(&owning, &coords.issue_id)
        .await
        .unwrap_or(false);

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
        return Ok(());
    };
    if changed.is_empty() {
        log::debug!(
            "turn-end checks for job {}: empty changed-file set; nothing to run",
            short_id(job_id)
        );
        return Ok(());
    }

    // 5. Select the applicable turn-end checks (cadence + pr_open + impact gate).
    let plans = applicable_turn_end_checks(&checks, &changed, repo_root, pr_open);
    if plans.is_empty() {
        log::debug!(
            "turn-end checks for job {}: no applicable idle/review check (pr_open={}); nothing to run",
            short_id(job_id),
            pr_open
        );
        return Ok(());
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
        return Ok(());
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

    // 8. Prepare the host-readable, job-scoped log file (truncated for a fresh run)
    // so the PR-node / `/checks` render can tail it live while checks run.
    let log_path = turn_end_log_path(orch, job_id);
    prepare_log(&log_path);

    // 9. Run each remaining check UNSANDBOXED, capturing to the cache and log.
    let mut any_failed = false;
    let mut verdicts: Vec<String> = Vec::with_capacity(to_run.len());
    for (index, (plan, input_hash)) in to_run.iter().enumerate() {
        let stream_id = format!("turn-checks:{job_id}:{index}");
        let started = Instant::now();
        let (exit_code, passed, output) =
            match crate::mcp::handlers::run::run_check_command_unsandboxed(
                orch,
                &worktree_path,
                &stream_id,
                &plan.command,
                CHECK_TIMEOUT_MS,
                &log_path,
            )
            .await
            {
                Ok((code, combined)) => (code, code == Some(0), combined),
                // A spawn error is a clear failure, never a silent pass.
                Err(err) => (None, false, err),
            };
        let duration_ms = started.elapsed().as_millis() as i64;

        // Enrich with structured per-test results (fail-closed: `passed` above is
        // exit-code-driven and unaffected by whether the parse succeeds).
        let target_results_json =
            parse_check_output(&plan.command, &output).and_then(|p| serde_json::to_string(&p).ok());
        let _ = store_check_result(
            db.clone(),
            CheckResultCacheWrite {
                project_id: coords.project_id.clone(),
                tree_hash: tree_hash.clone(),
                input_hash: input_hash.clone(),
                check_name: plan.name.clone(),
                exit_code: exit_code.unwrap_or(-1),
                passed,
                output_tail: tail(&output, OUTPUT_TAIL_CHARS),
                duration_ms,
                target_results_json,
            },
        );
        if !passed {
            any_failed = true;
        }
        verdicts.push(format!(
            "{}={} ({duration_ms}ms)",
            plan.name,
            if passed { "pass" } else { "fail" }
        ));
    }

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
    Ok(())
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
pub async fn render_turn_end_checks_section(orch: &Orchestrator, job_id: &str) -> Option<String> {
    let coords = resolve_job_coords(&orch.db.local, job_id)
        .await
        .ok()
        .flatten()?;
    let worktree_path = coords.worktree_path.clone().filter(|p| !p.is_empty())?;
    let jj = JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
    // The cache is keyed by the sealed tree identity; resolve it from the live
    // worktree (best-effort — a torn-down worktree yields no section for now).
    let tree_hash = sealed_tree_hash(&jj, Path::new(&worktree_path)).ok()?;
    let rows = list_check_results(orch.db.local.clone(), &coords.project_id, &tree_hash).ok()?;
    let in_flight = orch.turn_end_checks_in_flight(job_id);
    let tuples: Vec<CheckRow> = rows
        .into_iter()
        .map(|r| CheckRow {
            name: r.check_name,
            passed: r.passed,
            duration_ms: r.duration_ms,
            output_tail: r.output_tail,
            target_results_json: r.target_results_json,
        })
        .collect();
    let log_tail = if in_flight {
        read_log_tail(orch, job_id, LOG_TAIL_CHARS)
    } else {
        None
    };
    format_checks_section(in_flight, log_tail.as_deref(), &tuples)
}

/// One rendered check row (name + verdict + duration + failure output tail +
/// the persisted structured per-test detail, when present).
struct CheckRow {
    name: String,
    passed: bool,
    duration_ms: i64,
    output_tail: String,
    target_results_json: Option<String>,
}

/// Pure renderer for the `### Systematic checks` section. Returns `None` when the
/// section would be empty (not running and no cached rows). Split out so the
/// running vs. cached-verdict states are unit-tested without a DB or worktree.
fn format_checks_section(
    in_flight: bool,
    log_tail: Option<&str>,
    rows: &[CheckRow],
) -> Option<String> {
    if !in_flight && rows.is_empty() {
        return None;
    }
    let mut out = String::from("\n### Systematic checks\n\n");
    if in_flight {
        out.push_str("_running\u{2026}_\n");
        if let Some(tail) = log_tail.filter(|t| !t.trim().is_empty()) {
            out.push_str("\n```\n");
            out.push_str(tail.trim_end());
            out.push_str("\n```\n");
        }
    }
    for row in rows {
        if row.passed {
            out.push_str(&format!(
                "- \u{2713} {} ({}ms)\n",
                row.name, row.duration_ms
            ));
            continue;
        }
        // Rehydrate the structured per-test detail (enrichment; the stored
        // `passed` flag stays the authority for the verdict).
        let parsed = row
            .target_results_json
            .as_deref()
            .and_then(|s| serde_json::from_str::<ParsedCheckResult>(s).ok());
        match parsed.as_ref().and_then(format_failure_names) {
            Some(names) => out.push_str(&format!(
                "- \u{2717} {} \u{2014} {names} ({}ms)\n",
                row.name, row.duration_ms
            )),
            None => out.push_str(&format!(
                "- \u{2717} {} ({}ms)\n",
                row.name, row.duration_ms
            )),
        }
        let detail = format_failure_excerpt(parsed.as_ref(), row.output_tail.trim_end());
        if !detail.trim().is_empty() {
            out.push_str("\n```\n");
            out.push_str(detail.trim_end());
            out.push_str("\n```\n");
        }
    }
    Some(out)
}

/// The node's coordinates resolved from a `job_id` in one query.
struct JobCoords {
    project_id: String,
    issue_id: String,
    worktree_path: Option<String>,
    base_branch: Option<String>,
    base_commit: Option<String>,
    project_key: String,
    number: i32,
    exec_seq: i32,
    node_segment: String,
}

/// Resolve everything the runner and renderer need from a `job_id`: the project
/// and issue ids, the worktree path and base VCS anchors, and the
/// project-key/number/exec-seq/node-segment that build the `/checks` URI.
async fn resolve_job_coords(db: &LocalDb, job_id: &str) -> Result<Option<JobCoords>, String> {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT j.project_id, j.issue_id, j.worktree_path, j.base_branch,
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
                    issue_id: row.text(1)?,
                    worktree_path: row.opt_text(2)?,
                    base_branch: row.opt_text(3)?.filter(|s| !s.is_empty()),
                    base_commit: row.opt_text(4)?.filter(|s| !s.is_empty()),
                    project_key: row.text(5)?,
                    number: row.i64(6)? as i32,
                    exec_seq: row.i64(7)? as i32,
                    node_segment: row.opt_text(8)?.unwrap_or_default(),
                })),
                None => Ok(None),
            }
        })
    })
    .await
    .map_err(|e| format!("failed to resolve job coords: {e}"))
}

/// The host-readable, job-scoped log file for a turn-end run. Lives under the app
/// state dir (not the worktree) so it survives worktree teardown for the PR-node
/// render.
fn turn_end_log_path(orch: &Orchestrator, job_id: &str) -> PathBuf {
    orch.config_dir
        .join("turn-checks")
        .join(format!("{job_id}.log"))
}

/// Create the log's parent dir and truncate the file so a fresh run starts clean.
/// Best-effort: a failure here only costs the live tail, never the run.
fn prepare_log(path: &Path) {
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let _ = std::fs::write(path, b"");
}

/// Last `max_chars` chars of the job's log file, or `None` when it is missing/empty.
fn read_log_tail(orch: &Orchestrator, job_id: &str, max_chars: usize) -> Option<String> {
    let content = std::fs::read_to_string(turn_end_log_path(orch, job_id)).ok()?;
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

    fn row(name: &str, passed: bool, ms: i64, tail: &str) -> CheckRow {
        CheckRow {
            name: name.to_string(),
            passed,
            duration_ms: ms,
            output_tail: tail.to_string(),
            target_results_json: None,
        }
    }

    #[test]
    fn section_is_none_when_idle_and_no_rows() {
        assert!(format_checks_section(false, None, &[]).is_none());
    }

    #[test]
    fn section_renders_running_with_log_tail() {
        let s = format_checks_section(true, Some("compiling...\nrunning tests"), &[]).unwrap();
        assert!(s.contains("### Systematic checks"));
        assert!(s.contains("_running\u{2026}_"));
        assert!(s.contains("running tests"));
    }

    #[test]
    fn section_renders_running_without_a_log_yet() {
        // In flight but the log is still empty: show the running state, no fence.
        let s = format_checks_section(true, None, &[]).unwrap();
        assert!(s.contains("_running\u{2026}_"));
        assert!(!s.contains("```"));
    }

    #[test]
    fn section_renders_cached_verdicts_and_inlines_failure_output() {
        let rows = [
            row("rust", true, 12345, ""),
            row("frontend", false, 2100, "assertion failed: left == right"),
        ];
        let s = format_checks_section(false, None, &rows).unwrap();
        assert!(s.contains("\u{2713} rust (12345ms)"));
        assert!(s.contains("\u{2717} frontend (2100ms)"));
        // A failing check inlines its output tail; a passing one does not.
        assert!(s.contains("assertion failed: left == right"));
    }

    #[test]
    fn section_renders_structured_failure_names() {
        let json = serde_json::to_string(
            &crate::execution::check_parsers::parse_check_output(
                "bunx tsc --noEmit",
                "a.ts(1,7): error TS2322: bad type",
            )
            .unwrap(),
        )
        .unwrap();
        let rows = [CheckRow {
            name: "typecheck".to_string(),
            passed: false,
            duration_ms: 900,
            output_tail: "raw tail".to_string(),
            target_results_json: Some(json),
        }];
        let s = format_checks_section(false, None, &rows).unwrap();
        // The failing row leads with the structured name list...
        assert!(s.contains("\u{2717} typecheck \u{2014} 1 failed: a.ts(1,7) (900ms)"));
        // ...and inlines the composed failure message rather than the raw tail.
        assert!(s.contains("TS2322: bad type"));
        assert!(!s.contains("raw tail"));
    }

    #[test]
    fn tail_keeps_last_chars_on_boundary() {
        assert_eq!(tail("abcdef", 3), "def");
        assert_eq!(tail("abc", 10), "abc");
    }

    #[test]
    fn green_rides_along_passively_red_wakes() {
        // A clean run is delivered but never wakes an idle node; a failure rouses it.
        assert_eq!(delivery_wake(false), attention_push::Wake::Passive);
        assert_eq!(delivery_wake(true), attention_push::Wake::Wake);
    }

    #[test]
    fn short_id_never_panics_on_a_short_string() {
        assert_eq!(short_id("abcd"), "abcd");
        assert_eq!(short_id("0123456789"), "01234567");
    }
}
