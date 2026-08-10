use crate::config::project_settings::{CheckPolicy, CheckWhen};
use crate::execution::cache::{list_check_results_for_job, CheckResultCacheEntry};
use crate::execution::check_parsers::{
    extract_running_tests, format_failure_excerpt, format_failure_names, tail_chars,
    ParsedCheckResult, MAX_FAILURE_NAMES,
};
use crate::execution::checks::{load_checks_contract_at_commit, CheckFailureKind};
use crate::execution::checks_turn_end::{
    read_turn_end_log_tail, resolve_job_coords, turn_end_check_started,
};
use crate::execution::inputs::{
    any_check_declares_inputs, ResolvedInputs, TreeBlobs, TreeSnapshot,
};
use crate::execution::selection::plan_checks;
use crate::jj::{logical_changed_files, logical_tree_hash, JjEnv};
use crate::orchestrator::Orchestrator;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeCheckStatus {
    /// Stable owner used to join this lane to the canonical build-fabric row.
    pub(crate) job_id: String,
    /// The live fabric request for this lane, while one has been submitted.
    pub(crate) request_id: Option<String>,
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
    /// Files that failed as a whole without any test in them failing — a vitest
    /// file that could not be COLLECTED. Such a file runs no test, so folding it
    /// into `failed` renders "0 of 881 failed": a red check pointing at nothing.
    /// `None` for legacy rows and runners with no separate collection phase.
    pub(crate) suite_failures: Option<usize>,
    pub(crate) failure_names: Vec<String>,
    pub(crate) output_tail: Option<String>,
    /// Terminal classification of a FAILING check — `"timed_out"`,
    /// `"spawn_error"`, or `"killed"` — so a surface renders the real death, not
    /// an opaque red. `None` for a pass, an ordinary non-zero exit, and legacy
    /// rows.
    pub(crate) failure_kind: Option<String>,
    /// When set, Cairn has STOPPED executing this check for these inputs after
    /// this many consecutive infrastructure failures. The row it is rendered from
    /// is the last real attempt, so without this a suppressed check would read as
    /// an ordinary infrastructure red that is still being retried.
    pub(crate) suppressed_after: Option<i64>,
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
    let project_root = std::path::PathBuf::from(&coords.repository_path);
    let store = crate::jj::project_store_dir(&orch.config_dir, &project_root);
    let logical_repository = if crate::jj::is_jj_dir(&store) {
        store
    } else {
        project_root.clone()
    };
    let logical_head = crate::execution::cache::resolve_job_logical_head(orch, job_id)
        .await
        .ok()?;
    // The same live fork point the review cadence gates on. Reading the
    // recorded `jobs.base_commit` here would show a node checks that its own
    // suite never planned, because that row does not follow a base advance.
    let live_base = crate::diff::live_job_branch_range(&db, job_id, &orch.config_dir)
        .await
        .map_err(|error| {
            log::debug!("node {job_id} check status: no live base coordinate ({error})");
        })
        .ok()
        .flatten()
        .map(|range| range.base);
    // The status a node renders must be the suite its own cadences plan, so the
    // contract comes from the node's logical head — the same commit the cadences
    // read it from. Reading the project checkout here would show a node checks
    // that its own suite never selected (CAIRN-3333).
    let contract = load_checks_contract_at_commit(&project_root, &logical_head).await?;
    let checks = contract.contract.checks;
    let extra_inputs = contract.contract.extra_inputs;
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
    let request_ids = runtime_status
        .as_ref()
        .map(|status| status.request_ids.clone())
        .unwrap_or_else(|| orch.recent_turn_end_check_request_ids(job_id));
    let status_db = db.clone();
    let status_job_id = job_id.to_string();
    let status_project_id = coords.project_id.clone();
    let status_base = live_base;
    let status_repository = logical_repository;
    let status_head = logical_head;
    let status_root = project_root;
    let status_checks = checks.clone();
    let status_extra_inputs = extra_inputs.clone();
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
                    crate::execution::cache::list_check_results_for_head(
                        status_db,
                        &status_project_id,
                        &status.tree_hash,
                        &status_job_id,
                        &status_head,
                    )
                    .unwrap_or_default(),
                    Some(status.applicable_names),
                ),
                // Planning has not published its sealed-tree snapshot yet. Keep
                // every review check pending rather than showing stale rows from
                // an earlier run of this long-lived job.
                None => (Vec::new(), None),
            }
        } else {
            let live_rows = logical_tree_hash(&status_jj, &status_repository, &status_head)
                .ok()
                .and_then(|tree_hash| {
                    crate::execution::cache::list_check_results_for_head(
                        status_db.clone(),
                        &status_project_id,
                        &tree_hash,
                        &status_job_id,
                        &status_head,
                    )
                    .ok()
                });
            let rows = live_rows
                .or_else(|| list_check_results_for_job(status_db, &status_job_id).ok())
                .unwrap_or_default();
            let changed = status_base.as_deref().and_then(|base| {
                logical_changed_files(&status_jj, &status_repository, base, &status_head)
            });
            // The projection must answer "does this check apply" exactly as the
            // runners do, so it resolves each check's inputs against the same
            // sealed tree they key by.
            let applicable_names = changed.as_ref().map(|changed| {
                let entries = if any_check_declares_inputs(status_checks.values()) {
                    crate::jj::tree_entries(&status_jj, &status_repository, &status_head).ok()
                } else {
                    None
                };
                let blobs = TreeBlobs {
                    jj: &status_jj,
                    repository: &status_repository,
                };
                let snapshot = TreeSnapshot::new(entries.as_deref(), &blobs);
                let inputs =
                    ResolvedInputs::resolve(&status_checks, &status_extra_inputs, &snapshot);
                plan_checks(&status_checks, &inputs, changed, &status_root)
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

                // A review check does not apply when the impact gate excluded it
                // from this tree's plan; it will never run, so it is neither
                // running nor pending.
                let not_applicable = applicable_names
                    .as_ref()
                    .is_some_and(|names| !names.contains(&name));

                // `LaneEvidence::Recorded` outranks everything below, and this
                // returns before the `started` probe so a lane that already holds
                // a verdict never pays for a filesystem check it cannot use.
                if let Some(row) = rows_by_name.get(&name) {
                    return status_from_row(
                        job_id,
                        request_ids.get(&name).cloned(),
                        &name,
                        check.policy,
                        check.when,
                        row,
                    );
                }

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

                let state = match lane_evidence(false, not_applicable, started) {
                    LaneEvidence::NotApplicable => NodeCheckState::NotApplicable,
                    LaneEvidence::Running => NodeCheckState::Running,
                    // `Recorded` is unreachable here: the row arm returned above.
                    LaneEvidence::Recorded | LaneEvidence::Pending => NodeCheckState::Pending,
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
                    job_id: job_id.to_string(),
                    request_id: request_ids.get(&name).cloned(),
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
                    suite_failures: None,
                    failure_names: Vec::new(),
                    output_tail,
                    failure_kind: None,
                    suppressed_after: None,
                }
            })
            .collect(),
    )
}

/// What a lane knows about one check, in the order the answers outrank each
/// other.
///
/// The one ordering decision worth stating is `Recorded` over `NotApplicable`.
/// It matters in exactly one situation: a check the impact gate would NOT have
/// selected for this node's diff, whose verdict some other evaluation of the
/// IDENTICAL tree already recorded. A whole-tree default-branch attestation is
/// the case that actually happens — it plans over the entire tree, so it records
/// verdicts for checks no particular diff would select.
///
/// The verdict wins. `NotApplicable` asserts that a check will never run and has
/// nothing to say about this head, and that is simply false once evidence keyed
/// to this exact tree exists. Cache rows are keyed by tree hash, so such a
/// verdict is true of this head by construction: same tree, same result. The
/// impact gate decides what to RUN; it does not decide what is true.
///
/// The consequence is deliberate and should not surprise a reader of `/checks`:
/// a lane can carry a real verdict — including a red — for a check this node's
/// own change would not have selected, and a settle-wait will report it. That is
/// a fact about the tree the node is sitting on, which is the question a lane
/// answers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LaneEvidence {
    /// A verdict recorded against this node's sealed tree.
    Recorded,
    /// The impact gate excluded this check from the tree's plan.
    NotApplicable,
    /// A wave has started this check and it has not returned.
    Running,
    /// Selected, with nothing back yet.
    Pending,
}

pub(crate) fn lane_evidence(has_row: bool, not_applicable: bool, started: bool) -> LaneEvidence {
    if has_row {
        LaneEvidence::Recorded
    } else if not_applicable {
        LaneEvidence::NotApplicable
    } else if started {
        LaneEvidence::Running
    } else {
        LaneEvidence::Pending
    }
}

fn status_from_row(
    job_id: &str,
    request_id: Option<String>,
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
        job_id: job_id.to_string(),
        request_id,
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
        suite_failures: parsed.as_ref().map(|p| p.suite_failures),
        failure_names,
        output_tail,
        failure_kind: row.failure_kind.clone(),
        suppressed_after: (row.infra_failure_streak
            >= crate::execution::cache::OBSERVED_INFRA_FAILURE_BOUND)
            .then_some(row.infra_failure_streak),
    }
}

pub(crate) fn format_status_annotation(status: &NodeCheckStatus) -> Option<String> {
    // A suppressed check produced no verdict at this tree — Cairn declined to run
    // it. Every arm below describes a verdict, so saying any of them here would
    // dress the absence of a result up as one.
    if status.suppressed_after.is_some() {
        return Some("suppressed".to_string());
    }
    let mut parts = Vec::new();
    match status.state {
        NodeCheckState::Passed => {
            if let (Some(passed), Some(failed)) = (status.passed, status.failed) {
                let total = passed + failed;
                // Mirrors `execution::checks::summary_annotation`: a skipped test
                // never disappears into a green, and a suite that skipped itself
                // entirely reads differently from one whose selector matched
                // nothing (CAIRN-3164).
                let skipped = status.skipped.unwrap_or(0);
                match (total, skipped) {
                    (0, 0) => parts.push("no tests matched the change".to_string()),
                    (0, skipped) => parts.push(format!("no tests ran, {skipped} skipped")),
                    (total, 0) => parts.push(format!("{total} tests")),
                    (total, skipped) => parts.push(format!("{total} tests, {skipped} skipped")),
                }
            } else if let Some(ms) = status.duration_ms {
                parts.push(format_check_duration(ms));
            }
        }
        NodeCheckState::Failed => {
            if let Some(annotation) = failed_annotation(
                status
                    .failure_kind
                    .as_deref()
                    .and_then(CheckFailureKind::from_stored),
                status.duration_ms,
                status.passed,
                status.failed,
                status.suite_failures,
                &status.failure_names,
            ) {
                parts.push(annotation);
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

/// Failing lanes that get their own detail line in a wake body before the rest
/// are rolled up by name. A wake is read at the top of a resume prompt: it names
/// the reds and stops, and `/checks` holds the complete listing.
const WAKE_DETAILED_FAILURES: usize = 4;
/// Trailing chars of a failing lane's output kept when the lane named no failing
/// test. Without it such a lane — a compiler, a linter, a build — would say only
/// that it failed, which is the thing this body exists to stop doing.
const WAKE_EXCERPT_CHARS: usize = 600;
/// Check names listed in a wake's green roll-up before it collapses to a count.
const WAKE_ROLLUP_NAMES: usize = 12;

/// The body a delivered `turn-checks:` attention push carries: the verdicts
/// themselves, so a woken agent reads WHAT failed instead of a pointer to where
/// the answer lives. `None` when this job has recorded no verdict at all, which
/// leaves the caller its reference-only line.
///
/// Deliberately NOT [`node_check_statuses`]. That resolves the live tree hash,
/// the base fork point and the whole impact plan (jj, plus `cargo metadata` for
/// target expansion) — seconds of work on a busy repository, taken while resume
/// assembly holds the launch lock. A wake speaks about verdicts already recorded,
/// so it reads exactly those: one indexed query for the job's latest row per
/// check. The consequence is that a lane's last recorded verdict may predate this
/// node's current tree; the URI in the wake header is the live surface.
pub(crate) async fn check_wake_body(orch: &Orchestrator, job_id: &str) -> Option<String> {
    let db = crate::execution::routing::owning_db_for_job(&orch.db, job_id)
        .await
        .ok()?;
    match crate::execution::cache::latest_check_results_for_job(&db, job_id).await {
        Ok(rows) => format_check_wake_body(&rows),
        Err(error) => {
            log::debug!("check wake body for job {job_id}: {error}");
            None
        }
    }
}

/// Pure renderer for [`check_wake_body`], so the shape a wake speaks in is
/// unit-tested without a database.
pub(crate) fn format_check_wake_body(rows: &[CheckResultCacheEntry]) -> Option<String> {
    if rows.is_empty() {
        return None;
    }
    let mut failing: Vec<&CheckResultCacheEntry> = rows.iter().filter(|row| !row.passed).collect();
    failing.sort_by(|left, right| left.check_name.cmp(&right.check_name));
    let mut passing: Vec<String> = rows
        .iter()
        .filter(|row| row.passed)
        .map(|row| row.check_name.clone())
        .collect();
    passing.sort();

    if failing.is_empty() {
        return Some(format!(
            "Project checks on this node: all {} passing ({}).",
            passing.len(),
            join_capped(&passing, WAKE_ROLLUP_NAMES)
        ));
    }

    let mut out = format!(
        "Project checks on this node: {} of {} failing.\n",
        failing.len(),
        rows.len()
    );
    for row in failing.iter().take(WAKE_DETAILED_FAILURES) {
        out.push_str(&format!(
            "\n- \u{2717} {} \u{2014} {}",
            row.check_name,
            wake_failure_detail(row)
        ));
    }
    if failing.len() > WAKE_DETAILED_FAILURES {
        let rest: Vec<String> = failing
            .iter()
            .skip(WAKE_DETAILED_FAILURES)
            .map(|row| row.check_name.clone())
            .collect();
        out.push_str(&format!(
            "\n- \u{2717} also failing: {}",
            join_capped(&rest, WAKE_ROLLUP_NAMES)
        ));
    }
    if !passing.is_empty() {
        out.push_str(&format!(
            "\n- \u{2713} passing: {}",
            join_capped(&passing, WAKE_ROLLUP_NAMES)
        ));
    }
    Some(out)
}

/// What one failing lane says for itself in a wake body: the verdict sentence,
/// then the concrete failures — named tests when the runner named them, and the
/// tail of the output when it did not.
fn wake_failure_detail(row: &CheckResultCacheEntry) -> String {
    if row.infra_failure_streak >= crate::execution::cache::OBSERVED_INFRA_FAILURE_BOUND {
        return format!(
            "not run \u{2014} suppressed after {} infrastructure failures",
            row.infra_failure_streak
        );
    }
    let parsed = row
        .target_results_json
        .as_deref()
        .and_then(|json| serde_json::from_str::<ParsedCheckResult>(json).ok());
    let kind = row
        .failure_kind
        .as_deref()
        .and_then(CheckFailureKind::from_stored);
    let mut names: Vec<String> = parsed
        .as_ref()
        .map(|p| p.failures.iter().map(|f| f.name.clone()).collect())
        .unwrap_or_default();
    if kind == Some(CheckFailureKind::TimedOut) && names.is_empty() {
        names = extract_running_tests(&row.output_tail);
    }
    let annotation = failed_annotation(
        kind,
        Some(row.duration_ms),
        parsed.as_ref().map(|p| p.passed),
        parsed.as_ref().map(|p| p.failed),
        parsed.as_ref().map(|p| p.suite_failures),
        &names,
    )
    .unwrap_or_else(|| format!("exit {}", row.exit_code));

    // Named failures ARE the answer to "what broke", and a timeout has already
    // spent its names on the still-running list. The output tail is the fallback
    // for a lane that named nothing at all.
    if kind != Some(CheckFailureKind::TimedOut) && !names.is_empty() {
        return format!("{annotation}: {}", join_capped(&names, MAX_FAILURE_NAMES));
    }
    let excerpt = format_failure_excerpt(parsed.as_ref(), row.output_tail.trim_end());
    let excerpt = tail_chars(excerpt.trim_end(), WAKE_EXCERPT_CHARS);
    if excerpt.trim().is_empty() {
        return annotation;
    }
    let indented = excerpt
        .lines()
        .map(|line| format!("    {line}"))
        .collect::<Vec<_>>()
        .join("\n");
    format!("{annotation}\n{indented}")
}

/// The sentence a FAILING lane renders as, written from recorded facts alone.
///
/// Shared by the `/checks` lane annotation and the attention wake body so a red
/// is described the same way wherever it is read: the wake that rouses the author
/// and the listing they open next must not disagree about what happened.
/// `None` when the row carries neither a classified death nor a test tally — the
/// caller then says what it can (an exit code) rather than inventing a verdict.
///
/// `names` are the failing test names, or — for a timeout, which has none — the
/// tests still running when the check was killed.
pub(crate) fn failed_annotation(
    failure_kind: Option<CheckFailureKind>,
    duration_ms: Option<i64>,
    passed: Option<usize>,
    failed: Option<usize>,
    suite_failures: Option<usize>,
    names: &[String],
) -> Option<String> {
    if let Some(kind) = failure_kind {
        // A classified death renders AS itself ("timed out after 30m",
        // "failed to spawn"), never a bare "N of M failed" the agent
        // would chase into tests that never failed.
        let mut s = if kind == CheckFailureKind::RunnerError {
            match passed.unwrap_or(0) {
                0 => "test runner failed before reporting tests".to_string(),
                passed => format!(
                    "test runner failed after {passed} tests passed with no assertion failures"
                ),
            }
        } else {
            kind.describe(duration_ms.unwrap_or(0))
        };
        if kind == CheckFailureKind::TimedOut && !names.is_empty() {
            s.push_str(&format!(
                "; still running: {}",
                join_capped(names, MAX_FAILURE_NAMES)
            ));
        }
        return Some(s);
    }
    let (Some(failed), Some(passed)) = (failed, passed) else {
        return None;
    };
    // Mirrors `execution::checks::summary_annotation`: a file that failed to
    // collect ran no test, so it is counted as itself rather than folded into an
    // assertion tally that would read "0 of 881 failed".
    let suites = suite_failures.unwrap_or(0);
    let mut segments = Vec::new();
    if failed > 0 || suites == 0 {
        segments.push(format!("{failed} of {} failed", failed + passed));
    }
    if suites > 0 {
        let noun = if suites == 1 { "suite" } else { "suites" };
        segments.push(format!("{suites} {noun} failed to load"));
    }
    Some(segments.join(", "))
}

/// Comma-join check or test names, capped so a wide fan-out doesn't flood a line.
fn join_capped(names: &[String], max: usize) -> String {
    let shown: Vec<&str> = names.iter().take(max).map(String::as_str).collect();
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
        schema_version: 1,
        complete: false,
        selection: "unknown".to_string(),
        tests: vec![],
        undeclared_skips: 0,
        parser: "node-status".to_string(),
        passed: status.passed.unwrap_or(0),
        failed: status.failed.unwrap_or(status.failure_names.len()),
        skipped: status.skipped.unwrap_or(0),
        suite_failures: status.suite_failures.unwrap_or(0),
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

    fn verdict_row(name: &str, passed: bool) -> CheckResultCacheEntry {
        CheckResultCacheEntry {
            project_id: "project-a".to_string(),
            tree_hash: "tree".to_string(),
            input_hash: format!("ih-{name}"),
            check_name: name.to_string(),
            exit_code: if passed { 0 } else { 1 },
            passed,
            output_tail: String::new(),
            duration_ms: 1_000,
            ran_at: 1,
            target_results_json: None,
            job_id: Some("job".to_string()),
            cached: Some(false),
            failure_kind: None,
            infra_failure_streak: 0,
            executor_id: None,
            executor_device_id: None,
            executor_connection_generation: None,
            executor_cell_id: None,
            executor_lease_epoch: None,
            executor_started_at_unix_ms: None,
            executor_finished_at_unix_ms: None,
            toolchain_fingerprint: None,
            defined_by_commit_sha: Some("commit-a".to_string()),
        }
    }

    fn parsed_json(passed: usize, failures: &[&str]) -> String {
        serde_json::to_string(&ParsedCheckResult {
            schema_version: 1,
            complete: true,
            selection: "full".to_string(),
            tests: vec![],
            undeclared_skips: 0,
            parser: "nextest".to_string(),
            passed,
            failed: failures.len(),
            skipped: 0,
            suite_failures: 0,
            failures: failures
                .iter()
                .map(|name| crate::execution::check_parsers::CheckFailure {
                    name: name.to_string(),
                    message: None,
                })
                .collect(),
        })
        .unwrap()
    }

    /// The whole point of the wake body: an agent roused by a red check learns
    /// WHICH lane went red and WHAT failed in it, without spending a turn
    /// reading somewhere else to find out.
    #[test]
    fn a_checks_wake_names_the_failing_lane_and_its_tests() {
        let mut failing = verdict_row("rust-tests", false);
        failing.target_results_json =
            Some(parsed_json(64, &["cairn_core a::b", "cairn_core c::d"]));
        let rows = vec![
            failing,
            verdict_row("rust-fmt", true),
            verdict_row("lint", true),
        ];

        let body = format_check_wake_body(&rows).unwrap();

        assert!(
            body.starts_with("Project checks on this node: 1 of 3 failing."),
            "{body}"
        );
        assert!(
            body.contains("✗ rust-tests — 2 of 66 failed: cairn_core a::b, cairn_core c::d"),
            "{body}"
        );
        assert!(body.contains("✓ passing: lint, rust-fmt"), "{body}");
    }

    /// A compiler or linter names no test, so without its output tail the lane
    /// would say only that it failed — the pointer-shaped wake this replaced.
    #[test]
    fn a_lane_that_named_no_failure_carries_its_output_tail() {
        let mut failing = verdict_row("typecheck", false);
        failing.output_tail =
            "src/a.ts(3,1): error TS2322: Type 'string' is not assignable.".to_string();

        let body = format_check_wake_body(&[failing]).unwrap();

        assert!(body.contains("✗ typecheck — exit 1"), "{body}");
        assert!(
            body.contains("    src/a.ts(3,1): error TS2322"),
            "the tail is indented under its lane: {body}"
        );
    }

    /// Green rides along passively, so it costs no turn — but it still says what
    /// it covered rather than sending the reader to a resource.
    #[test]
    fn a_clean_run_says_so_in_one_line() {
        let body =
            format_check_wake_body(&[verdict_row("lint", true), verdict_row("api", true)]).unwrap();

        assert_eq!(
            body,
            "Project checks on this node: all 2 passing (api, lint)."
        );
        assert!(
            format_check_wake_body(&[]).is_none(),
            "no recorded verdict leaves the caller its reference-only line"
        );
    }

    /// A suppressed lane produced no verdict at all; the wake must not dress the
    /// last real attempt up as this run's result.
    #[test]
    fn a_suppressed_lane_says_it_was_not_run() {
        let mut row = verdict_row("rust-tests", false);
        row.failure_kind = Some("infrastructure".to_string());
        row.infra_failure_streak = crate::execution::cache::OBSERVED_INFRA_FAILURE_BOUND;

        let body = format_check_wake_body(&[row]).unwrap();

        assert!(body.contains("not run — suppressed after"), "{body}");
    }

    /// The precedence CAIRN-3823 made reachable again, recorded so the next
    /// reader inherits a decision rather than an accident.
    ///
    /// While verdict rows were invisible, a check the impact gate excluded could
    /// only ever render `not applicable`. Now that they are visible, a whole-tree
    /// default-branch attestation can leave a verdict at a tree for a check no
    /// particular diff would select — and a node sitting on that identical tree
    /// shows it, red included, rather than claiming the check has nothing to say.
    #[test]
    fn a_recorded_verdict_outranks_the_impact_gate() {
        assert_eq!(
            lane_evidence(true, true, false),
            LaneEvidence::Recorded,
            "evidence about this exact tree outranks a plan that would not have run it"
        );
        assert_eq!(
            lane_evidence(true, false, true),
            LaneEvidence::Recorded,
            "a returned verdict outranks a lane still reading as started"
        );
        assert_eq!(
            lane_evidence(false, true, false),
            LaneEvidence::NotApplicable
        );
        assert_eq!(lane_evidence(false, false, true), LaneEvidence::Running);
        assert_eq!(lane_evidence(false, false, false), LaneEvidence::Pending);
    }

    #[test]
    fn status_annotation_renders_counts_duration_and_cached() {
        let mut status = NodeCheckStatus {
            job_id: "job".to_string(),
            request_id: None,
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
            suite_failures: None,
            failure_names: Vec::new(),
            output_tail: None,
            failure_kind: None,
            suppressed_after: None,
        };
        assert_eq!(
            format_status_annotation(&status).as_deref(),
            Some("12 tests, 1 skipped, cached")
        );
        status.skipped = Some(0);
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
    fn status_annotation_separates_a_self_skipped_suite_from_a_zero_selection() {
        let mut status = NodeCheckStatus {
            job_id: "job".to_string(),
            request_id: None,
            name: "rust-full".to_string(),
            state: NodeCheckState::Passed,
            policy: "advisory".to_string(),
            when: "review".to_string(),
            cached: Some(false),
            duration_ms: Some(600_000),
            ran_at: Some(1),
            passed: Some(0),
            failed: Some(0),
            skipped: Some(12),
            suite_failures: None,
            failure_names: Vec::new(),
            output_tail: None,
            failure_kind: None,
            suppressed_after: None,
        };
        assert_eq!(
            format_status_annotation(&status).as_deref(),
            Some("no tests ran, 12 skipped")
        );
        status.skipped = Some(0);
        assert_eq!(
            format_status_annotation(&status).as_deref(),
            Some("no tests matched the change")
        );
    }

    /// The whole point of `suite_failures`, at the surface that outlives the run:
    /// a check read back out of `check_result_cache` must not render the zero
    /// tally the immediate summary already refuses to render.
    #[test]
    fn status_annotation_names_a_cached_suite_collection_failure() {
        let mut status = NodeCheckStatus {
            job_id: "job".to_string(),
            request_id: None,
            name: "frontend-partial".to_string(),
            state: NodeCheckState::Failed,
            policy: "advisory".to_string(),
            when: "write".to_string(),
            cached: Some(true),
            duration_ms: Some(30_000),
            ran_at: Some(1),
            passed: Some(881),
            failed: Some(0),
            skipped: Some(0),
            suite_failures: Some(1),
            failure_names: vec!["src/components/FileTabView.test.tsx".to_string()],
            output_tail: Some("Cannot find module".to_string()),
            failure_kind: None,
            suppressed_after: None,
        };
        assert_eq!(
            format_status_annotation(&status).as_deref(),
            Some("1 suite failed to load, cached")
        );

        // Both kinds at once: neither number may absorb the other.
        status.passed = Some(38);
        status.failed = Some(2);
        status.suite_failures = Some(3);
        status.cached = Some(false);
        assert_eq!(
            format_status_annotation(&status).as_deref(),
            Some("2 of 40 failed, 3 suites failed to load")
        );

        // A legacy row (no suite count stored) still reads exactly as before.
        status.suite_failures = None;
        assert_eq!(
            format_status_annotation(&status).as_deref(),
            Some("2 of 40 failed")
        );
    }

    #[test]
    fn failed_annotation_renders_timeout_with_still_running_tests() {
        let status = NodeCheckStatus {
            job_id: "job".to_string(),
            request_id: None,
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
            suite_failures: None,
            failure_names: vec!["mycrate mod::hangs".to_string()],
            output_tail: Some("...".to_string()),
            failure_kind: Some("timed_out".to_string()),
            suppressed_after: None,
        };
        assert_eq!(
            format_status_annotation(&status).as_deref(),
            Some("timed out after 30m; still running: mycrate mod::hangs")
        );
    }

    /// A suppressed check must not borrow the vocabulary of a verdict. The row it
    /// renders from is a real infrastructure failure, so without the counter it
    /// would read as "infrastructure/toolchain failure" — true of the last
    /// attempt, and misleading about the present, where nothing is being retried.
    #[test]
    fn suppressed_annotation_says_not_run_rather_than_naming_a_verdict() {
        let mut status = NodeCheckStatus {
            job_id: "job".to_string(),
            request_id: None,
            name: "rust-full".to_string(),
            state: NodeCheckState::Failed,
            policy: "advisory".to_string(),
            when: "review".to_string(),
            cached: Some(true),
            duration_ms: Some(6),
            ran_at: Some(1),
            passed: None,
            failed: None,
            skipped: None,
            suite_failures: None,
            failure_names: Vec::new(),
            output_tail: Some("sccache: server startup failed".to_string()),
            failure_kind: Some("infrastructure".to_string()),
            suppressed_after: Some(3),
        };
        assert_eq!(
            format_status_annotation(&status).as_deref(),
            Some("suppressed")
        );

        // Below the bound the same row still reads as the retried failure it is,
        // reuse suffix and all.
        status.suppressed_after = None;
        assert_eq!(
            format_status_annotation(&status).as_deref(),
            Some("infrastructure/toolchain failure, cached")
        );
    }

    /// The counter is what makes a row suppressed, and only at or past the bound.
    #[test]
    fn a_row_becomes_suppressed_only_at_the_bound() {
        let mut row = CheckResultCacheEntry {
            project_id: "project-a".to_string(),
            tree_hash: "tree".to_string(),
            input_hash: "ih".to_string(),
            check_name: "rust".to_string(),
            exit_code: 1,
            passed: false,
            output_tail: "sccache died".to_string(),
            duration_ms: 1,
            ran_at: 1,
            target_results_json: None,
            job_id: None,
            cached: None,
            failure_kind: Some("infrastructure".to_string()),
            infra_failure_streak: crate::execution::cache::OBSERVED_INFRA_FAILURE_BOUND - 1,
            executor_id: None,
            executor_device_id: None,
            executor_connection_generation: None,
            executor_cell_id: None,
            executor_lease_epoch: None,
            executor_started_at_unix_ms: None,
            executor_finished_at_unix_ms: None,
            toolchain_fingerprint: None,
            defined_by_commit_sha: Some("commit-a".to_string()),
        };
        assert_eq!(
            status_from_row(
                "job",
                None,
                "rust",
                CheckPolicy::Advisory,
                CheckWhen::Review,
                &row
            )
            .suppressed_after,
            None
        );

        row.infra_failure_streak = crate::execution::cache::OBSERVED_INFRA_FAILURE_BOUND;
        assert_eq!(
            status_from_row(
                "job",
                None,
                "rust",
                CheckPolicy::Advisory,
                CheckWhen::Review,
                &row
            )
            .suppressed_after,
            Some(crate::execution::cache::OBSERVED_INFRA_FAILURE_BOUND)
        );
    }

    #[test]
    fn failed_annotation_renders_spawn_error() {
        let status = NodeCheckStatus {
            job_id: "job".to_string(),
            request_id: None,
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
            suite_failures: None,
            failure_names: Vec::new(),
            output_tail: Some("Failed to spawn command".to_string()),
            failure_kind: Some("spawn_error".to_string()),
            suppressed_after: None,
        };
        assert_eq!(
            format_status_annotation(&status).as_deref(),
            Some("failed to spawn")
        );
    }
}
