use std::collections::HashMap;
use std::path::Path;

use crate::config::project_settings::{CheckPolicy, CheckWhen};
use crate::execution::cache::{
    list_check_results, list_check_results_for_job, CheckResultCacheEntry,
};
use crate::execution::check_parsers::{
    extract_running_tests, format_failure_excerpt, format_failure_names, ParsedCheckResult,
    MAX_FAILURE_NAMES,
};
use crate::execution::checks::{load_live_project_checks, CheckFailureKind};
use crate::execution::checks_turn_end::{
    read_turn_end_log_tail, resolve_job_coords, turn_end_check_started,
};
use crate::execution::selection::plan_checks;
use crate::jj::{node_changed_files, sealed_tree_hash, JjEnv};
use crate::orchestrator::Orchestrator;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeCheckStatus {
    pub(crate) name: String,
    pub(crate) state: NodeCheckState,
    pub(crate) policy: String,
    pub(crate) when: String,
    pub(crate) cached: Option<bool>,
    pub(crate) duration_ms: Option<i64>,
    pub(crate) ran_at: Option<i64>,
    pub(crate) passed: Option<usize>,
    pub(crate) failed: Option<usize>,
    pub(crate) skipped: Option<usize>,
    pub(crate) failure_names: Vec<String>,
    pub(crate) output_tail: Option<String>,
    /// Terminal classification of a FAILING check — `"timed_out"`,
    /// `"spawn_error"`, or `"killed"` — so a surface renders the real death, not
    /// an opaque red. `None` for a pass, an ordinary non-zero exit, and legacy
    /// rows.
    pub(crate) failure_kind: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub enum NodeCheckState {
    Passed,
    Failed,
    Running,
    Pending,
    NotApplicable,
}

pub async fn node_check_statuses(
    orch: &Orchestrator,
    job_id: &str,
) -> Option<Vec<NodeCheckStatus>> {
    // Route to the database that owns this job (team replica or private DB); the
    // job coordinates and cached check results for a team node live in its
    // replica. A closed replica yields no statuses rather than a wrong read
    // against the private DB.
    let db = crate::execution::routing::owning_db_for_job(&orch.db, job_id)
        .await
        .ok()?;
    let coords = resolve_job_coords(&db, job_id).await.ok().flatten()?;
    let worktree_path = coords.worktree_path.clone().filter(|p| !p.is_empty());
    let worktree_root = worktree_path
        .as_deref()
        .map(Path::new)
        .unwrap_or_else(|| Path::new("."));
    let checks = load_live_project_checks(orch, &coords.project_id, worktree_root).await?;
    if checks.is_empty() {
        return Some(Vec::new());
    }

    // Status resolution waits on jj, cargo metadata, and the synchronous cache
    // bridge. Routing and config loading above stay async; the complete status
    // snapshot below belongs on the blocking pool so rendering `/checks` cannot
    // park a runtime worker.
    let review_in_flight = orch.turn_end_checks_in_flight(job_id);
    let write_in_flight = orch.write_checks_in_flight(job_id);
    let in_flight = review_in_flight || write_in_flight;
    let runtime_status = orch.turn_end_check_runtime_status(job_id);
    let status_db = db.clone();
    let status_job_id = job_id.to_string();
    let status_project_id = coords.project_id.clone();
    let status_base_branch = coords.base_branch.clone();
    let status_base_commit = coords.base_commit.clone();
    let status_worktree = worktree_path.clone();
    let status_root = worktree_root.to_path_buf();
    let status_checks = checks.clone();
    let status_jj = JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
    let (rows_by_name, applicable_names) = tokio::task::spawn_blocking(move || {
        // A running review suite publishes its sealed tree and applicable check
        // names once planning finishes. The 1 Hz live-tail poll therefore needs
        // only that immutable snapshot plus current-tree cache rows; re-running jj
        // tree resolution and the cumulative diff on every tick is both redundant
        // and, under repository load, orders of magnitude more expensive than the
        // rest of this handler.
        // Once the suite settles, take the full VCS-backed snapshot exactly once
        // so not-applicable checks and cross-equivalent-tree cache hits are exact.
        let (rows, applicable_names) = if in_flight {
            match runtime_status {
                Some(status) => (
                    list_check_results(status_db, &status_project_id, &status.tree_hash)
                        .unwrap_or_default(),
                    Some(status.applicable_names),
                ),
                // Planning has not published its sealed-tree snapshot yet. Keep
                // every review check pending rather than showing stale rows from
                // an earlier run of this long-lived job.
                None => (Vec::new(), None),
            }
        } else {
            let live_rows = status_worktree.as_deref().and_then(|path| {
                sealed_tree_hash(&status_jj, Path::new(path))
                    .ok()
                    .and_then(|tree_hash| {
                        list_check_results(status_db.clone(), &status_project_id, &tree_hash).ok()
                    })
                    .filter(|rows| !rows.is_empty())
            });
            let rows = live_rows
                .or_else(|| list_check_results_for_job(status_db, &status_job_id).ok())
                .unwrap_or_default();
            let changed = status_worktree.as_deref().and_then(|path| {
                node_changed_files(
                    &status_jj,
                    Path::new(path),
                    status_base_branch.as_deref(),
                    status_base_commit.as_deref(),
                )
            });
            let applicable_names = changed.as_ref().map(|changed| {
                plan_checks(&status_checks, changed, &status_root)
                    .into_iter()
                    .filter(|plan| plan.applies)
                    .map(|plan| plan.name)
                    .collect::<std::collections::HashSet<_>>()
            });
            (rows, applicable_names)
        };
        let rows_by_name: HashMap<String, CheckResultCacheEntry> = rows
            .into_iter()
            .map(|row| (row.check_name.clone(), row))
            .collect();
        (rows_by_name, applicable_names)
    })
    .await
    .ok()?;

    let mut names = checks.keys().cloned().collect::<Vec<_>>();
    names.sort();
    Some(
        names
            .into_iter()
            .map(|name| {
                let check = checks.get(&name).expect("name came from checks map");
                if let Some(row) = rows_by_name.get(&name) {
                    return status_from_row(&name, check.policy, check.when, row);
                }

                // A review check does not apply when the impact gate excluded it
                // from this tree's plan; it will never run, so it is neither
                // running nor pending.
                let not_applicable = applicable_names
                    .as_ref()
                    .is_some_and(|names| !names.contains(&name));

                // Turn-end review checks run CONCURRENTLY in isolated COW clones
                // (or sequentially in the shared worktree on the clone-unavailable
                // fallback), each into its OWN log file created the instant it
                // starts. Existence of that file — not a non-empty tail — is the
                // RUNNING signal, so a silent-but-active check is not mistaken for a
                // queued one; under isolation several checks read as running at
                // once, while a not-yet-started (or fallback-queued) check has no
                // file yet and stays pending. The tail is read separately and is
                // None while a running check has yet to emit.
                let started = (review_in_flight && check.when == CheckWhen::Review
                    || write_in_flight && check.when == CheckWhen::Write)
                    && !not_applicable
                    && (write_in_flight || turn_end_check_started(orch, job_id, &name));

                let state = if not_applicable {
                    NodeCheckState::NotApplicable
                } else if started {
                    NodeCheckState::Running
                } else {
                    NodeCheckState::Pending
                };

                // Only the actively-running check carries a live tail (and it may
                // still be None before its first line); pending and not-applicable
                // rows have none.
                let output_tail = if started {
                    read_turn_end_log_tail(orch, job_id, &name)
                } else {
                    None
                };

                NodeCheckStatus {
                    name,
                    state,
                    policy: check.policy.as_str().to_string(),
                    when: check.when.as_str().to_string(),
                    cached: None,
                    duration_ms: None,
                    ran_at: None,
                    passed: None,
                    failed: None,
                    skipped: None,
                    failure_names: Vec::new(),
                    output_tail,
                    failure_kind: None,
                }
            })
            .collect(),
    )
}

fn status_from_row(
    name: &str,
    policy: CheckPolicy,
    when: CheckWhen,
    row: &CheckResultCacheEntry,
) -> NodeCheckStatus {
    let parsed = row
        .target_results_json
        .as_deref()
        .and_then(|s| serde_json::from_str::<ParsedCheckResult>(s).ok());
    let failure_kind = row
        .failure_kind
        .as_deref()
        .and_then(CheckFailureKind::from_stored);
    let mut failure_names: Vec<String> = parsed
        .as_ref()
        .map(|p| p.failures.iter().map(|f| f.name.clone()).collect())
        .unwrap_or_default();
    // A timeout has no failing tests to name; surface the tests still running
    // when it was killed (nextest SLOW lines) so the wake detail answers "what
    // was it doing when it died?".
    if failure_kind == Some(CheckFailureKind::TimedOut) && failure_names.is_empty() {
        failure_names = extract_running_tests(&row.output_tail);
    }
    let output_tail = if row.passed {
        None
    } else {
        Some(format_failure_excerpt(
            parsed.as_ref(),
            row.output_tail.trim_end(),
        ))
        .filter(|s| !s.trim().is_empty())
    };
    NodeCheckStatus {
        name: name.to_string(),
        state: if row.passed {
            NodeCheckState::Passed
        } else {
            NodeCheckState::Failed
        },
        policy: policy.as_str().to_string(),
        when: when.as_str().to_string(),
        cached: row.cached,
        duration_ms: Some(row.duration_ms),
        ran_at: Some(row.ran_at),
        passed: parsed.as_ref().map(|p| p.passed),
        failed: parsed.as_ref().map(|p| p.failed),
        skipped: parsed.as_ref().map(|p| p.skipped),
        failure_names,
        output_tail,
        failure_kind: row.failure_kind.clone(),
    }
}

pub(crate) fn format_status_annotation(status: &NodeCheckStatus) -> Option<String> {
    let mut parts = Vec::new();
    match status.state {
        NodeCheckState::Passed => {
            if let (Some(passed), Some(failed)) = (status.passed, status.failed) {
                let total = passed + failed;
                if total == 0 {
                    parts.push("no tests matched the change".to_string());
                } else {
                    parts.push(format!("{total} tests"));
                }
            } else if let Some(ms) = status.duration_ms {
                parts.push(format_check_duration(ms));
            }
        }
        NodeCheckState::Failed => {
            if let Some(kind) = status
                .failure_kind
                .as_deref()
                .and_then(CheckFailureKind::from_stored)
            {
                // A classified death renders AS itself ("timed out after 30m",
                // "failed to spawn"), never a bare "N of M failed" the agent
                // would chase into tests that never failed.
                let mut s = if kind == CheckFailureKind::RunnerError {
                    match status.passed.unwrap_or(0) {
                        0 => "test runner failed before reporting tests".to_string(),
                        passed => format!(
                            "test runner failed after {passed} tests passed with no assertion failures"
                        ),
                    }
                } else {
                    kind.describe(status.duration_ms.unwrap_or(0))
                };
                if kind == CheckFailureKind::TimedOut && !status.failure_names.is_empty() {
                    s.push_str(&format!(
                        "; still running: {}",
                        join_running(&status.failure_names)
                    ));
                }
                parts.push(s);
            } else if let (Some(failed), Some(passed)) = (status.failed, status.passed) {
                parts.push(format!("{failed} of {} failed", failed + passed));
            }
        }
        _ => {}
    }
    if status.cached == Some(true) {
        parts.push("cached".to_string());
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join(", "))
    }
}

/// Comma-join running-test names for the timeout annotation, capped like the
/// failure-name list so a wide fan-out doesn't flood the line.
fn join_running(names: &[String]) -> String {
    let shown: Vec<&str> = names
        .iter()
        .take(MAX_FAILURE_NAMES)
        .map(String::as_str)
        .collect();
    let more = names.len().saturating_sub(shown.len());
    if more > 0 {
        format!("{}, +{more} more", shown.join(", "))
    } else {
        shown.join(", ")
    }
}

fn format_check_duration(ms: i64) -> String {
    if ms >= 1000 {
        format!("{:.1}s", ms as f64 / 1000.0)
    } else {
        format!("{ms}ms")
    }
}

pub(crate) fn formatted_failure_names(status: &NodeCheckStatus) -> Option<String> {
    let parsed = ParsedCheckResult {
        parser: "node-status".to_string(),
        passed: status.passed.unwrap_or(0),
        failed: status.failed.unwrap_or(status.failure_names.len()),
        skipped: status.skipped.unwrap_or(0),
        failures: status
            .failure_names
            .iter()
            .map(|name| crate::execution::check_parsers::CheckFailure {
                name: name.clone(),
                message: None,
            })
            .collect(),
    };
    format_failure_names(&parsed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_annotation_renders_counts_duration_and_cached() {
        let mut status = NodeCheckStatus {
            name: "rust".to_string(),
            state: NodeCheckState::Passed,
            policy: "advisory".to_string(),
            when: "write".to_string(),
            cached: Some(true),
            duration_ms: Some(4100),
            ran_at: Some(1),
            passed: Some(12),
            failed: Some(0),
            skipped: Some(1),
            failure_names: Vec::new(),
            output_tail: None,
            failure_kind: None,
        };
        assert_eq!(
            format_status_annotation(&status).as_deref(),
            Some("12 tests, cached")
        );
        status.passed = None;
        status.failed = None;
        status.cached = Some(false);
        assert_eq!(format_status_annotation(&status).as_deref(), Some("4.1s"));
    }

    #[test]
    fn failed_annotation_renders_timeout_with_still_running_tests() {
        let status = NodeCheckStatus {
            name: "rust-full".to_string(),
            state: NodeCheckState::Failed,
            policy: "advisory".to_string(),
            when: "review".to_string(),
            cached: None,
            duration_ms: Some(1_800_000),
            ran_at: Some(1),
            passed: None,
            failed: None,
            skipped: None,
            failure_names: vec!["mycrate mod::hangs".to_string()],
            output_tail: Some("...".to_string()),
            failure_kind: Some("timed_out".to_string()),
        };
        assert_eq!(
            format_status_annotation(&status).as_deref(),
            Some("timed out after 30m; still running: mycrate mod::hangs")
        );
    }

    #[test]
    fn failed_annotation_renders_spawn_error() {
        let status = NodeCheckStatus {
            name: "rust-lint".to_string(),
            state: NodeCheckState::Failed,
            policy: "advisory".to_string(),
            when: "review".to_string(),
            cached: None,
            duration_ms: Some(6),
            ran_at: Some(1),
            passed: None,
            failed: None,
            skipped: None,
            failure_names: Vec::new(),
            output_tail: Some("Failed to spawn command".to_string()),
            failure_kind: Some("spawn_error".to_string()),
        };
        assert_eq!(
            format_status_annotation(&status).as_deref(),
            Some("failed to spawn")
        );
    }
}
