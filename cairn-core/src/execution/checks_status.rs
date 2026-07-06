use std::collections::HashMap;
use std::path::Path;

use crate::config::project_settings::{CheckPolicy, CheckWhen};
use crate::execution::cache::{
    list_check_results, list_check_results_for_job, CheckResultCacheEntry,
};
use crate::execution::check_parsers::{
    format_failure_excerpt, format_failure_names, ParsedCheckResult,
};
use crate::execution::checks::load_live_project_checks;
use crate::execution::checks_turn_end::{
    read_turn_end_log_tail, resolve_job_coords, turn_end_check_started,
};
use crate::execution::selection::plan_checks;
use crate::jj::{node_changed_files, sealed_tree_hash, JjEnv};
use crate::orchestrator::Orchestrator;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeCheckStatus {
    pub name: String,
    pub state: NodeCheckState,
    pub policy: String,
    pub when: String,
    pub cached: Option<bool>,
    pub duration_ms: Option<i64>,
    pub ran_at: Option<i64>,
    pub passed: Option<usize>,
    pub failed: Option<usize>,
    pub skipped: Option<usize>,
    pub failure_names: Vec<String>,
    pub output_tail: Option<String>,
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
    let coords = resolve_job_coords(&orch.db.local, job_id)
        .await
        .ok()
        .flatten()?;
    let worktree_path = coords.worktree_path.clone().filter(|p| !p.is_empty());
    let worktree_root = worktree_path
        .as_deref()
        .map(Path::new)
        .unwrap_or_else(|| Path::new("."));
    let checks = load_live_project_checks(orch, &coords.project_id, worktree_root).await?;
    if checks.is_empty() {
        return Some(Vec::new());
    }

    let jj = JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
    let live_rows = worktree_path.as_deref().and_then(|path| {
        sealed_tree_hash(&jj, Path::new(path))
            .ok()
            .and_then(|tree_hash| {
                list_check_results(orch.db.local.clone(), &coords.project_id, &tree_hash).ok()
            })
            .filter(|rows| !rows.is_empty())
    });
    let rows = live_rows
        .or_else(|| list_check_results_for_job(orch.db.local.clone(), job_id).ok())
        .unwrap_or_default();
    let rows_by_name: HashMap<String, CheckResultCacheEntry> = rows
        .into_iter()
        .map(|row| (row.check_name.clone(), row))
        .collect();

    let in_flight = orch.turn_end_checks_in_flight(job_id);
    let changed = worktree_path.as_deref().and_then(|path| {
        node_changed_files(
            &jj,
            Path::new(path),
            coords.base_branch.as_deref(),
            coords.base_commit.as_deref(),
        )
    });
    let plans_by_name = changed.as_ref().map(|changed| {
        plan_checks(&checks, changed, worktree_root)
            .into_iter()
            .map(|plan| (plan.name.clone(), plan))
            .collect::<HashMap<_, _>>()
    });

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
                let not_applicable = plans_by_name
                    .as_ref()
                    .and_then(|plans| plans.get(&name))
                    .is_some_and(|plan| !plan.applies);

                // Turn-end review checks run SEQUENTIALLY, each into its OWN log
                // file created the instant it starts. Existence of that file — not a
                // non-empty tail — is the RUNNING signal, so a silent-but-active
                // check is not mistaken for a queued one; checks still behind the
                // active one have no file yet and stay pending. The tail is read
                // separately and is None while a running check has yet to emit.
                let started = in_flight
                    && check.when == CheckWhen::Review
                    && !not_applicable
                    && turn_end_check_started(orch, job_id, &name);

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
    let failure_names = parsed
        .as_ref()
        .map(|p| p.failures.iter().map(|f| f.name.clone()).collect())
        .unwrap_or_default();
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
    }
}

pub fn format_status_annotation(status: &NodeCheckStatus) -> Option<String> {
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
            if let (Some(failed), Some(passed)) = (status.failed, status.passed) {
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

pub fn format_check_duration(ms: i64) -> String {
    if ms >= 1000 {
        format!("{:.1}s", ms as f64 / 1000.0)
    } else {
        format!("{ms}ms")
    }
}

pub fn formatted_failure_names(status: &NodeCheckStatus) -> Option<String> {
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
}
