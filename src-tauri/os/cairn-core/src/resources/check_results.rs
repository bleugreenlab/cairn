//! Commit-addressable check observation resource.
//!
//! Persistence is intentionally accessed through `execution::cache`; resource
//! code owns only coordinate/query validation and presentation.

use crate::execution::cache::{
    get_check_result_observation, get_check_result_observation_by_handle,
    CheckResultObservationProjection,
};
use crate::mcp::handlers::branch::resolve_for_read;
use crate::mcp::handlers::run_context::project_id_by_key;
use crate::mcp::types::McpCallbackRequest;
use crate::orchestrator::Orchestrator;
use cairn_common::query::{encode_query_params, QueryParam};

use super::paging::RecordPage;
use crate::execution::check_evidence::{
    reconcile_evidence, EvidenceClass, EvidenceInputs, EvidenceReconciliation,
};

const DEFAULT_LIMIT: usize = 100;
const MAX_LIMIT: usize = 1000;
const RESULT_SCHEMA_VERSION: i64 =
    crate::execution::check_identity::CHECK_RESULT_SCHEMA_VERSION as i64;

#[derive(Debug, PartialEq, Eq)]
struct CheckResultsQuery {
    suite: String,
    environment: String,
    environment_fingerprint: String,
    status: Option<String>,
    name: Option<String>,
    limit: usize,
    offset: usize,
}

pub(super) async fn read_project_check_observation(
    orch: &Orchestrator,
    project: &str,
    handle: &str,
) -> String {
    let request = request_handle(handle);
    if request.is_none()
        && (handle.len() != 24 || !handle.bytes().all(|byte| byte.is_ascii_hexdigit()))
    {
        return "Invalid check observation handle: expected a 24-hex ledger handle or the UUID request handle `cairn check run` printed".to_string();
    }
    let db = orch.db.for_project(project).await;
    let project_id = match project_id_by_key(&db, project).await {
        Ok(project_id) => project_id,
        Err(error) => return error,
    };
    if let Some(request) = request {
        return match crate::mcp::handlers::check_run::request_observation_value(
            orch,
            db,
            &project_id,
            &request,
        )
        .await
        {
            Ok(value) => render_request_observation(project, &request, &value),
            Err(error) => format!("Unable to read check request {request}: {error}"),
        };
    }
    match get_check_result_observation_by_handle(db, &project_id, handle) {
        Ok(Some(observation)) => {
            let environment = if observation.environment_fingerprint.is_empty() {
                "unfingerprinted"
            } else {
                observation.environment_fingerprint.as_str()
            };
            let query = CheckResultsQuery {
                suite: observation.check_name.clone(),
                environment: environment.to_string(),
                environment_fingerprint: observation.environment_fingerprint.clone(),
                status: None,
                name: None,
                limit: observation.test_total.max(1),
                offset: 0,
            };
            let citation = format!("cairn://p/{project}/check-observations/{handle}");
            render_observation(
                project,
                &observation.source_commit_sha,
                &observation.source_commit_sha,
                &query,
                &observation,
                Some(&citation),
                ran_on(orch, &observation).as_deref(),
            )
        }
        Ok(None) => "Check observation not found".to_string(),
        Err(error) => format!("Unable to read check observation: {error}"),
    }
}

/// The lowercase hyphenated form of a `cairn check run` request handle, or
/// `None` when `handle` is not a 36-character UUID. Ledger handles are 24 hex
/// characters, so the two never collide.
fn request_handle(handle: &str) -> Option<String> {
    if handle.len() != 36 {
        return None;
    }
    uuid::Uuid::try_parse(handle)
        .ok()
        .map(|uuid| uuid.hyphenated().to_string())
}

/// A request handle's answer: pending progress, a settled verdict with its
/// ledger citation when one was recorded, or the request's error.
fn render_request_observation(project: &str, handle: &str, value: &serde_json::Value) -> String {
    let text = |value: &serde_json::Value| value.as_str().map(str::to_string);
    let state = value["state"].as_str().unwrap_or("unknown");
    let mut lines = vec![
        format!("# Check request {handle}"),
        String::new(),
        format!("- Project: {project}"),
        format!("- Request: cairn://p/{project}/check-observations/{handle}"),
    ];
    match state {
        "queued" | "running" | "parked" => {
            lines.push(format!("- State: {state} (pending, no verdict yet)"));
            if let Some(diagnostic) = text(&value["diagnostic"]) {
                lines.push(format!("- Progress: {diagnostic}"));
            }
            if let Some(executor) = text(&value["executor"]) {
                lines.push(format!("- Executor: {executor}"));
            }
            if let Some(since) = value["sinceUnixMs"].as_i64() {
                lines.push(format!("- Since: {}", millis_stamp(since)));
            }
            lines.push(String::new());
            lines.push("The check is still in flight. Read this resource again for its answer; once it settles, the recorded ledger observation is linked here.".into());
        }
        "complete" => {
            let result = &value["result"];
            let suite = text(&result["checkName"]).unwrap_or_default();
            lines.push("- State: complete".into());
            lines.push(format!("- Suite: {suite}"));
            let verdict = match (&result["noVerdict"], result["passed"].as_bool()) {
                _ if result["placementUnmet"].as_str().is_some() => format!(
                    "no verdict for the requested placement: {}",
                    result["placementUnmet"].as_str().unwrap_or_default()
                ),
                (no_verdict, _) if no_verdict.is_object() => format!(
                    "no verdict ({}): {}",
                    no_verdict["kind"].as_str().unwrap_or("unknown"),
                    no_verdict["cause"].as_str().unwrap_or("")
                ),
                (_, Some(true)) if result["verified"] == false => format!(
                    "passed, unverified: {}",
                    result["unverifiedReason"]
                        .as_str()
                        .unwrap_or("no verdict witness")
                ),
                (_, Some(true)) => "passed".into(),
                _ => "failed".into(),
            };
            lines.push(format!("- Verdict: {verdict}"));
            for (label, key) in [
                ("Disposition", "disposition"),
                ("Commit", "commitSha"),
                ("Tree", "treeHash"),
                ("Tier", "tier"),
                ("Verdict platform", "verdictPlatform"),
                ("Runtime", "runtime"),
                ("Ran at", "ranAt"),
                ("Placement note", "placementNote"),
                ("Placement unmet", "placementUnmet"),
            ] {
                if let Some(value) = text(&result[key]).filter(|value| !value.is_empty()) {
                    lines.push(format!("- {label}: {value}"));
                }
            }
            if let Some(code) = result["exitCode"].as_i64() {
                lines.push(format!("- Exit code: {code}"));
            }
            if let Some(name) = text(&result["executor"]["name"]) {
                let os = text(&result["executor"]["os"]).unwrap_or_default();
                lines.push(format!("- Ran on: {name} ({os})"));
            }
            match text(&result["observationRef"]) {
                Some(reference) => lines.push(format!("- Ledger observation: {reference}")),
                None => lines.push(format!(
                    "- Ledger observation: none. {}",
                    text(&result["notRecorded"])
                        .unwrap_or_else(|| "This verdict was not recorded.".into())
                )),
            }
            if let (Some(_), Some(cause)) = (
                text(&result["observationRef"]),
                text(&result["notRecorded"]),
            ) {
                lines.push(format!("- Not recorded: {cause}"));
            }
            let output = text(&result["outputTail"]).unwrap_or_default();
            if !output.is_empty() {
                let fence = output_fence(&output);
                lines.push(String::new());
                lines.push("## Output tail".into());
                lines.push(String::new());
                lines.push(format!("{fence}\n{output}\n{fence}"));
            }
        }
        "failed" => {
            lines.push("- State: failed".into());
            lines.push(format!(
                "- Error: {}",
                text(&value["error"]).unwrap_or_default()
            ));
        }
        other => {
            lines.push(format!("- State: {other}"));
            lines.push(format!("- Answer: {value}"));
        }
    }
    lines.join("\n")
}

fn parse_query(params: &[QueryParam]) -> Result<CheckResultsQuery, String> {
    let mut suite = None;
    let mut environment = None;
    let mut status = None;
    let mut name = None;
    let mut limit = None;
    let mut offset = None;
    for param in params {
        let slot = match param.key.as_str() {
            "suite" => &mut suite,
            "environment" => &mut environment,
            "status" => &mut status,
            "name" => &mut name,
            _ => match param.key.as_str() {
                "limit" if limit.is_none() => {
                    let parsed = param.value.parse::<usize>().map_err(|_| {
                        "Query parameter 'limit' must be a positive integer".to_string()
                    })?;
                    if parsed == 0 || parsed > MAX_LIMIT {
                        return Err(format!(
                            "Query parameter 'limit' must be between 1 and {MAX_LIMIT}"
                        ));
                    }
                    limit = Some(parsed);
                    continue;
                }
                "offset" if offset.is_none() => {
                    offset = Some(param.value.parse::<usize>().map_err(|_| {
                        "Query parameter 'offset' must be a non-negative integer".to_string()
                    })?);
                    continue;
                }
                "limit" | "offset" => {
                    return Err(format!(
                        "Query parameter '{}' must be specified once",
                        param.key
                    ))
                }
                other => {
                    return Err(format!(
                        "Unsupported query parameter '{}' for project check results",
                        other
                    ))
                }
            },
        };
        if slot.is_some() || param.value.trim().is_empty() {
            return Err(format!(
                "Query parameter '{}' must be specified once with a non-empty value",
                param.key
            ));
        }
        *slot = Some(param.value.trim().to_string());
    }
    let suite = suite.ok_or_else(|| "Missing required query parameter 'suite'".to_string())?;
    let environment =
        environment.ok_or_else(|| "Missing required query parameter 'environment'".to_string())?;
    if let Some(value) = status.as_deref() {
        if !matches!(value, "passed" | "failed" | "skipped" | "deferred") {
            return Err(
                "Query parameter 'status' must be one of: passed, failed, skipped, deferred"
                    .to_string(),
            );
        }
    }
    let environment_fingerprint = if matches!(environment.as_str(), "current" | "unfingerprinted") {
        String::new()
    } else {
        environment.clone()
    };
    Ok(CheckResultsQuery {
        suite,
        environment,
        environment_fingerprint,
        status,
        name,
        limit: limit.unwrap_or(DEFAULT_LIMIT),
        offset: offset.unwrap_or(0),
    })
}

pub(super) async fn read_project_check_results(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    project: &str,
    revision: &str,
    params: &[QueryParam],
) -> String {
    read_project_check_results_page(orch, request, project, revision, params)
        .await
        .map_or_else(|error| error, |page| page.body)
}

/// [`read_project_check_results`] as a page the read view continues in tests.
pub(super) async fn read_project_check_results_page(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    project: &str,
    revision: &str,
    params: &[QueryParam],
) -> Result<RecordPage, String> {
    let query = parse_query(params)?;
    let db = orch.db.for_project(project).await;
    let project_id = project_id_by_key(&db, project).await?;
    let resolution = resolve_for_read(orch, request, revision)
        .await
        .map_err(|error| format!("Unable to resolve requested revision '{revision}': {error}"))?;
    if resolution.project_id != project_id {
        return Err(format!("Requested revision '{revision}' resolved in a different project; refusing to substitute that commit"));
    }
    let mut query = query;
    if query.environment == "current" {
        let config = match crate::mcp::handlers::read::file_at_commit(
            resolution.object_repository_path.clone(),
            resolution.commit_id.clone(),
            ".cairn/config.yaml",
        ) {
            Ok(Some(bytes)) => bytes,
            Ok(None) => return Err(format!("Unable to resolve suite '{}' at requested revision '{revision}': .cairn/config.yaml is unavailable", query.suite)),
            Err(error) => return Err(format!("Unable to resolve suite '{}' at requested revision '{revision}': {error}", query.suite)),
        };
        let settings: crate::config::project_settings::ProjectSettingsFile = match serde_yaml::from_slice(&config) {
            Ok(settings) => settings,
            Err(error) => return Err(format!("Unable to resolve suite '{}' at requested revision '{revision}': invalid .cairn/config.yaml: {error}", query.suite)),
        };
        let Some(check) = settings
            .checks
            .as_ref()
            .and_then(|checks| checks.get(&query.suite))
        else {
            return Err(format!("Unable to resolve suite '{}' at requested revision '{revision}': suite is not configured", query.suite));
        };
        query.environment_fingerprint =
            crate::execution::check_identity::local_environment_identity(
                vec![crate::execution::checks::check_toolchain_identity().to_string()],
                crate::execution::check_identity::verdict_environment_names(check),
            )
            .fingerprint;
    }

    match get_check_result_observation(
        db,
        &project_id,
        &resolution.commit_id,
        &query.suite,
        &query.environment_fingerprint,
        RESULT_SCHEMA_VERSION,
        query.status.as_deref(),
        query.name.as_deref(),
        query.limit,
        query.offset,
    ) {
        Ok(Some(observation)) => Ok(render_observation_page(
            project,
            revision,
            &resolution.commit_id,
            &query,
            &observation,
            None,
            ran_on(orch, &observation).as_deref(),
        )),
        Ok(None) => Ok(RecordPage {
            body: render_miss(
                project,
                revision,
                &resolution.commit_id,
                &query.suite,
                &query.environment_fingerprint,
            ),
            noun: "tests",
            offset: query.offset,
            limit: query.limit,
            shown: 0,
            total: None,
            more: false,
            prelude_lines: None,
        }),
        Err(error) => Err(format!("Unable to read check observation: {error}")),
    }
}

/// A check observation is a forensic record — it gets read next to app logs and
/// transcript turns — so its instants render on the host clock with the clock
/// named. A bare epoch integer told the reader nothing; an unlabelled stamp
/// would have invited them to read it on the wrong clock.
fn millis_stamp(millis: i64) -> String {
    crate::clock::stamp_millis_with_seconds(millis).unwrap_or_else(|| millis.to_string())
}

/// The machine that ran an observation, as `name (os/arch)`, with the platform
/// this runner knows the machine by rather than the one the row records, so a
/// row labelled with the wrong platform reads as wrong beside it.
fn ran_on(orch: &Orchestrator, observation: &CheckResultObservationProjection) -> Option<String> {
    let executor_id = observation.executor_id.as_deref()?;
    let platform = orch.fleet.executor_platform(executor_id).map_or_else(
        || "platform not known to this runner".to_string(),
        |platform| platform.to_string(),
    );
    Some(format!(
        "{} ({platform})",
        orch.fleet.executor_name_for_text(executor_id)
    ))
}

fn verdict_platform_line(
    observation: &CheckResultObservationProjection,
    ran_on: Option<&str>,
) -> String {
    let label = match (
        observation.verdict_platform.as_deref(),
        observation.verdict_arch.as_deref(),
    ) {
        (Some(os), Some(arch)) => format!("{os}/{arch}"),
        (Some(os), None) => os.to_string(),
        (None, _) => "unrecorded".to_string(),
    };
    match ran_on {
        Some(machine) => format!("- Verdict platform: {label} · ran on {machine}"),
        None => format!("- Verdict platform: {label}"),
    }
}

/// The runtime the run measured, beside its platform: a check's runtime
/// instruments are part of its verdict environment, so two rows a Bun release
/// apart are different verdicts and the page says which one this is. Every
/// toolchain the run recorded is named, since the page shows what the run
/// measured rather than which of it the check declared; one the run was never
/// probed for (node, on a row older than its probe) is left out rather than
/// called unrecorded on every such row. Where that Node or Bun came from (the
/// executor's own pinned copy or the host's) is on the executor's page.
fn verdict_runtime_line(observation: &CheckResultObservationProjection) -> String {
    let runtime = observation
        .toolchain_fingerprint
        .as_deref()
        .map(cairn_checkenv::recorded_runtime)
        .unwrap_or_default();
    if runtime.is_empty() {
        return "- Runtime: unrecorded".to_string();
    }
    format!(
        "- Runtime: {}",
        crate::execution::check_identity::runtime_label(&runtime)
    )
}

fn render_observation(
    project: &str,
    revision: &str,
    commit: &str,
    query: &CheckResultsQuery,
    observation: &CheckResultObservationProjection,
    permalink: Option<&str>,
    ran_on: Option<&str>,
) -> String {
    render_observation_page(
        project,
        revision,
        commit,
        query,
        observation,
        permalink,
        ran_on,
    )
    .body
}

/// An observation as a page of its tests: the observation's facts, then one
/// line per test on this page, then the retained output tail.
fn render_observation_page(
    project: &str,
    revision: &str,
    commit: &str,
    query: &CheckResultsQuery,
    observation: &CheckResultObservationProjection,
    permalink: Option<&str>,
    ran_on: Option<&str>,
) -> RecordPage {
    let short_commit = commit.get(..12).unwrap_or(commit);
    let handle = format!("{}@{short_commit}", observation.check_name);
    let params = encode_query_params(&[
        QueryParam {
            key: "suite".into(),
            value: query.suite.clone(),
        },
        QueryParam {
            key: "environment".into(),
            value: query.environment.clone(),
        },
    ]);
    let coordinate = format!("cairn://p/{project}/check-results/{commit}?{params}");
    let citation = permalink.unwrap_or(&coordinate);
    // A cached alias answers this commit with an execution at another one; the
    // populations below are that execution's, never this request's.
    let reused_from = (observation.disposition == "cached").then(|| {
        let source = observation.public_handle.as_deref().map_or_else(
            || "its source observation has no permalink handle".to_string(),
            |handle| format!("cairn://p/{project}/check-observations/{handle}"),
        );
        format!(
            "the execution at {} ({source})",
            observation
                .source_commit_sha
                .get(..12)
                .unwrap_or(&observation.source_commit_sha)
        )
    });
    let (count_lines, evidence) = observation_evidence(observation, reused_from);
    let count_mismatch = evidence.class.revokes_authority();
    let mut body = vec![
        format!("# Check results: {}", observation.check_name),
        String::new(),
        format!("## {} observation", observation.disposition),
        String::new(),
        format!("- Project: {project}"),
        format!("- Requested revision: {revision}"),
        format!("- Resolved commit: {commit}"),
        // Three distinct commits, never collapsed into one line: the commit that
        // was evaluated, the commit whose `.cairn/config.yaml` declared the check
        // at that coordinate, and the commit the reused verdict was produced at.
        // A definition arriving from a tree other than the evaluated one is
        // exactly the shape CAIRN-3333 made invisible.
        format!(
            "- Defined by commit: {}",
            observation
                .defined_by_commit_sha
                .as_deref()
                .unwrap_or("unrecorded (legacy row)")
        ),
        format!("- Evaluated tree: {}", observation.evaluated_tree_hash),
        format!("- Content hash: {}", observation.evaluated_input_hash),
        format!(
            "- Environment fingerprint: {}",
            observation.environment_fingerprint
        ),
        format!("- Citation: [{handle}]({citation})"),
        format!("- Source commit: {}", observation.source_commit_sha),
        format!(
            "- Source defined by commit: {}",
            observation
                .source_defined_by_commit_sha
                .as_deref()
                .unwrap_or("unrecorded (legacy row)")
        ),
        format!("- Source tree: {}", observation.source_tree_hash),
        format!("- Source content hash: {}", observation.source_input_hash),
        format!(
            "- Verdict: {} (exit {})",
            observation.verdict, observation.exit_code
        ),
        verdict_platform_line(observation, ran_on),
        verdict_runtime_line(observation),
        format!("- Complete: {}", observation.complete && !count_mismatch),
        format!("- Reusable: {}", observation.reusable && !count_mismatch),
        format!(
            "- Parser/schema: {}/{}",
            observation.parser_version, observation.result_schema_version
        ),
        format!("- Evaluated at: {}", millis_stamp(observation.evaluated_at)),
        format!("- Ran at: {}", millis_stamp(observation.ran_at)),
        format!("- Duration: {} ms", observation.duration_ms),
        format!("- Cadence: {}", observation.cadence),
        format!(
            "- Selection: {}",
            observation
                .selection
                .as_deref()
                .unwrap_or("unrecorded (legacy row)")
        ),
    ];
    body.extend(count_lines);
    if count_mismatch {
        body.push(format!(
            "- Recorded authority (overridden by count mismatch): complete={}, reusable={}",
            observation.complete, observation.reusable
        ));
    }
    // Deferrals are persisted separately from judged outcomes; the page keeps
    // that distinction even on observations whose enumeration is incomplete.
    if observation.selection.as_deref()
        == Some(crate::execution::check_parsers::FAST_TIER_SELECTION)
    {
        body.push(format!(
            "- Fast tier: {} deferred (ran past the per-test budget; not judged by this verdict)",
            observation.deferred_count
        ));
    }
    if let Some(phases) = &observation.phases {
        body.extend(phase_lines(phases, observation.duration_ms));
    }
    body.extend([
        String::new(),
        "## Evidence accounting".to_string(),
        String::new(),
    ]);
    body.extend(evidence.lines());
    body.push(String::new());
    if let Some(line) = cell_line(
        observation.execution_warmth.as_deref(),
        observation.cell_origin.as_ref(),
    ) {
        body.push(line);
    }
    if let Some(build_jobs) = observation.build_jobs {
        body.push(format!("- Build jobs: {}", build_jobs.describe()));
    }
    if let Some(generation) = observation.executor_connection_generation {
        body.push(format!("- Executor generation: {generation}"));
    }
    if let Some(epoch) = observation.executor_lease_epoch {
        body.push(format!("- Lease epoch: {epoch}"));
    }
    if let Some(started) = observation.executor_started_at_unix_ms {
        body.push(format!("- Executor started at: {}", millis_stamp(started)));
    }
    if let Some(finished) = observation.executor_finished_at_unix_ms {
        body.push(format!(
            "- Executor finished at: {}",
            millis_stamp(finished)
        ));
    }
    for (label, value) in [
        (
            "Non-authoritative reason",
            observation.non_reusable_reason.as_deref(),
        ),
        ("Failure kind", observation.failure_kind.as_deref()),
        ("Source run", observation.run_id.as_deref()),
        ("Source job", observation.job_id.as_deref()),
        ("Executor", observation.executor_id.as_deref()),
        ("Device", observation.executor_device_id.as_deref()),
        ("Cell", observation.executor_cell_id.as_deref()),
        ("Runner build", observation.runner_build_id.as_deref()),
        ("Toolchain", observation.toolchain_fingerprint.as_deref()),
    ] {
        if let Some(value) = value {
            body.push(format!("- {label}: {value}"));
        }
    }
    let passed = observation
        .tests
        .iter()
        .filter(|test| test.status == "passed")
        .count();
    let failed = observation
        .tests
        .iter()
        .filter(|test| test.status == "failed")
        .count();
    let skipped = observation
        .tests
        .iter()
        .filter(|test| test.status == "skipped")
        .count();
    let deferred = observation
        .tests
        .iter()
        .filter(|test| test.status == "deferred")
        .count();
    let deferred = if deferred > 0 || observation.deferred_count > 0 {
        format!(", {deferred} deferred")
    } else {
        String::new()
    };
    body.extend([
        String::new(),
        "## Tests".to_string(),
        String::new(),
        format!(
            "{} passed, {} failed, {} skipped{deferred} on this page",
            passed, failed, skipped
        ),
        format!(
            "- Total: {} (matching current filters)",
            observation.test_total
        ),
        format!("- Offset: {}", observation.test_offset),
        format!("- Limit: {}", query.limit),
    ]);
    let next_offset = observation
        .test_offset
        .saturating_add(observation.tests.len());
    if next_offset < observation.test_total {
        let mut params = vec![
            QueryParam {
                key: "suite".into(),
                value: query.suite.clone(),
            },
            QueryParam {
                key: "environment".into(),
                value: query.environment.clone(),
            },
        ];
        if let Some(status) = &query.status {
            params.push(QueryParam {
                key: "status".into(),
                value: status.clone(),
            });
        }
        if let Some(name) = &query.name {
            params.push(QueryParam {
                key: "name".into(),
                value: name.clone(),
            });
        }
        params.push(QueryParam {
            key: "limit".into(),
            value: query.limit.to_string(),
        });
        params.push(QueryParam {
            key: "offset".into(),
            value: next_offset.to_string(),
        });
        body.push(format!(
            "- Next: cairn://p/{project}/check-results/{revision}?{}",
            encode_query_params(&params)
        ));
    } else {
        body.push("- Next: none".to_string());
    }
    // An element may itself hold several lines; count what the join renders.
    let prelude_lines = body.iter().map(|line| line.split('\n').count()).sum();
    for test in &observation.tests {
        let mut line = format!("- [{}] {}", test.status, test.test_id);
        // The start beside the duration is what dates a test against a host
        // event: `(663979 ms, started 52 m 10 s into the lane)`.
        let timing: Vec<String> = [
            test.duration_ms.map(|duration| format!("{duration} ms")),
            test.started_offset_ms.map(|offset| {
                format!(
                    "started {} into the lane",
                    phase_span(u64::try_from(offset).unwrap_or(0))
                )
            }),
        ]
        .into_iter()
        .flatten()
        .collect();
        if !timing.is_empty() {
            line.push_str(&format!(" ({})", timing.join(", ")));
        }
        if test.flaky {
            line.push_str("; flaky");
        }
        if let Some(attempts) = test.attempt_count.filter(|attempts| *attempts > 1) {
            line.push_str(&format!("; {attempts} attempts"));
        }
        if let Some(reason) = test.skip_reason.as_deref() {
            line.push_str(&format!("; skip: {reason}"));
        }
        if let Some(source) = test.declaration_source.as_deref() {
            line.push_str(&format!("; declared by {source}"));
        }
        if let Some(failure) = test.failure_excerpt.as_deref() {
            line.push_str(&format!("; failure: {}", failure.replace('\n', " ")));
        }
        body.push(line);
    }
    // The retained output is often the only evidence a red carries: a script that
    // exits before reporting any test leaves its reason on stderr and nowhere else.
    let output = observation.output_tail.trim_end();
    if !output.is_empty() {
        let fence = output_fence(output);
        body.extend([
            String::new(),
            "## Output (tail)".to_string(),
            String::new(),
            format!("{fence}text"),
            output.to_string(),
            fence,
        ]);
    }
    RecordPage {
        body: body.join("\n"),
        noun: "tests",
        offset: observation.test_offset,
        limit: query.limit,
        shown: observation.tests.len(),
        total: Some(observation.test_total),
        more: false,
        prelude_lines: Some(prelude_lines),
    }
}

/// The observation's count lines and its evidence reconciliation. A result
/// that carries accounting is reconciled from it, link by link; one recorded
/// before accounting falls back to comparing a retained nextest Summary with
/// the stored rows, and that comparison is one more input to the same
/// classification rather than a verdict of its own.
fn observation_evidence(
    observation: &CheckResultObservationProjection,
    reused_from: Option<String>,
) -> (Vec<String>, EvidenceReconciliation) {
    let parsed = observation.verdict_result.as_ref();
    let accounting = parsed.and_then(|parsed| parsed.accounting.as_ref());
    let (mut lines, retained_tally_mismatch) = if accounting.is_some() {
        (persisted_count_lines(observation), None)
    } else {
        observation_count_lines(observation)
    };
    let evidence = reconcile_evidence(EvidenceInputs {
        parsed,
        failure_kind: observation.failure_kind.as_deref(),
        expects_enumeration: None,
        persisted: Some(&observation.persisted_counts),
        reused_from,
        retained_tally_mismatch,
    });
    if let Some(accounting) = accounting {
        for pass in &accounting.passes {
            lines.push(match &pass.runner {
                Some(runner) => format!(
                    "- Runner-reported ({}, pass `{}`): {} total; {} passed, {} failed, {} skipped, {} timed out",
                    runner.source,
                    pass.pass,
                    runner.total,
                    runner.passed,
                    runner.failed,
                    runner.skipped,
                    runner.timed_out
                ),
                None => format!(
                    "- Runner witness unavailable for pass `{}`: the runner left no tally",
                    pass.pass
                ),
            });
        }
        lines.push(format!(
            "- Count reconciliation: {}",
            if evidence.class.revokes_authority() {
                "MISMATCH (see Evidence accounting). Enumeration is incomplete/non-authoritative; not reusable."
            } else if evidence.class == EvidenceClass::Accounted {
                "matched link by link (see Evidence accounting)"
            } else {
                "see Evidence accounting"
            }
        ));
    }
    (lines, evidence)
}

fn persisted_count_lines(observation: &CheckResultObservationProjection) -> Vec<String> {
    let counts = &observation.persisted_counts;
    vec![
        format!(
            "- Persisted outcomes (global): {} total; {} passed, {} failed, {} skipped, {} deferred",
            counts.total(), counts.passed, counts.failed, counts.skipped, counts.deferred
        ),
        format!(
            "- Self-skips: {} (ledger-reported; transferred from runner passes to persisted skips when reconciling)",
            counts.self_skipped
        ),
    ]
}

/// Compare a single nextest run with the global manifest, never with a page.
/// A composed tier's summaries may overlap, so even a retained single pass is
/// not a witness for the composition. Returns the lines and, on a mismatch,
/// the sentence that names it.
fn observation_count_lines(
    observation: &CheckResultObservationProjection,
) -> (Vec<String>, Option<String>) {
    let counts = &observation.persisted_counts;
    let mut lines = persisted_count_lines(observation);
    // Only nextest prints the Summary this compares; another runner's
    // observation without accounting has no retained tally at all.
    if let Some(parser) = observation
        .verdict_result
        .as_ref()
        .map(|parsed| parsed.parser.as_str())
        .filter(|parser| *parser != "nextest")
    {
        lines.push(format!(
            "- Runner witness unavailable: no runner tally was recorded for this {parser} result; using persisted counts"
        ));
        lines.push("- Count reconciliation: unavailable".into());
        return (lines, None);
    }
    let unavailable = if observation.selection.as_deref()
        == Some(crate::execution::check_parsers::FAST_TIER_SELECTION)
    {
        Some("composed fast tier; pass summaries are not a whole-lane tally")
    } else if observation.phases.as_ref().is_some_and(|phases| {
        phases
            .passes
            .iter()
            .filter(|pass| !matches!(pass.name.as_str(), "warmup" | "build"))
            .count()
            > 1
    }) {
        Some("multiple test passes; no unambiguous whole-lane tally")
    } else {
        None
    };
    let summary = crate::execution::check_parsers::nextest_summary_counts(&observation.output_tail);
    if let Some(reason) = unavailable {
        lines.push(format!(
            "- Runner witness unavailable: {reason}; using persisted counts"
        ));
        lines.push("- Count reconciliation: unavailable".into());
        return (lines, None);
    }
    let Some(summary) = summary else {
        lines.push("- Runner witness unavailable: no single unambiguous nextest Summary retained; using persisted counts".into());
        lines.push("- Count reconciliation: unavailable".into());
        return (lines, None);
    };
    lines.push(format!(
        "- Runner-reported (nextest Summary): {} total; {} passed, {} failed, {} skipped, {} timed out",
        summary.run + summary.skipped, summary.passed, summary.failed, summary.skipped, summary.timed_out
    ));
    if observation.selection.as_deref() == Some("filtered") {
        lines.push("- Count reconciliation: unavailable (filtered selection; runner skips may include unselected tests outside the persisted manifest)".into());
        return (lines, None);
    }
    let matched = counts.passed.checked_add(counts.self_skipped) == Some(summary.passed)
        && Some(counts.failed) == summary.failed.checked_add(summary.timed_out)
        && counts.skipped.checked_sub(counts.self_skipped) == Some(summary.skipped)
        && counts.deferred == 0;
    if matched {
        lines.push("- Count reconciliation: matched after self-skip pass-to-skip transfers (counts only; not proof of test identity)".into());
        return (lines, None);
    }
    let mismatch = format!(
        "retained nextest Summary ({} total) disagrees with the stored rows ({} total) after {} self-skip pass-to-skip transfers",
        summary.run + summary.skipped,
        counts.total(),
        counts.self_skipped
    );
    lines.push(format!(
        "- Count reconciliation: MISMATCH after {} self-skip pass-to-skip transfers; runner total {}, persisted total {}. Enumeration is incomplete/non-authoritative; not reusable.",
        counts.self_skipped, summary.run + summary.skipped, counts.total()
    ));
    (lines, Some(mismatch))
}

/// A backtick fence longer than any backtick run inside the output, so tool
/// output that itself prints a fenced block cannot close this one early.
fn output_fence(output: &str) -> String {
    let longest_run = output
        .split(|character| character != '`')
        .map(str::len)
        .max()
        .unwrap_or(0);
    "`".repeat(longest_run.max(2) + 1)
}

fn render_miss(
    project: &str,
    revision: &str,
    commit: &str,
    suite: &str,
    environment: &str,
) -> String {
    format!(
        "# Check results: {suite}\n\n- Project: {project}\n- Requested revision: {revision}\n- Resolved commit: {commit}\n- Environment fingerprint: {environment}\n- Result schema: {RESULT_SCHEMA_VERSION}\n\n## Miss\n\nNo authoritative observation is recorded for this exact commit, suite, environment, and schema. Run the suite fresh; do not substitute a result from another commit or environment."
    )
}

/// `- Cell: preparedWarmSlot · origin reused (12 files / 3 crates from the
/// commit)`: the executor's warmth and cell origin under its own names, so a
/// compile phase is read beside the cell state that decides it.
fn cell_line(
    warmth: Option<&str>,
    origin: Option<&crate::execution::cache::ObservedCellOrigin>,
) -> Option<String> {
    let mut parts = Vec::new();
    if let Some(warmth) = warmth {
        parts.push(warmth.to_string());
    }
    if let Some(origin) = origin {
        let distance = match (origin.distance_files, origin.distance_crates) {
            (Some(files), Some(crates)) => {
                format!(" ({files} files / {crates} crates from the commit)")
            }
            _ => String::new(),
        };
        parts.push(format!("origin {}{distance}", origin.kind));
    }
    (!parts.is_empty()).then(|| format!("- Cell: {}", parts.join(" · ")))
}

/// `12 s`, `6 m 48 s`, `450 ms`: a phase's length at the precision it is read.
fn phase_span(ms: u64) -> String {
    if ms < 1_000 {
        return format!("{ms} ms");
    }
    let seconds = (ms + 500) / 1_000;
    if seconds < 60 {
        format!("{seconds} s")
    } else {
        format!("{} m {} s", seconds / 60, seconds % 60)
    }
}

/// `- Phases: setup 12 s · compile 6 m 48 s (37 crates compiled) · test 2 m 25 s
/// · publish 4 s`, then the passes when the lane ran more than one. Whatever
/// of `duration_ms` the timeline does not cover is named, so a row whose
/// phases do not account for its duration says so.
fn phase_lines(
    phases: &crate::execution::check_parsers::CheckPhases,
    duration_ms: i64,
) -> Vec<String> {
    let compiled = match phases.compiled_units {
        Some(0) => " (nothing stale)".to_string(),
        Some(1) => " (1 crate compiled)".to_string(),
        Some(units) => format!(" ({units} crates compiled)"),
        None => String::new(),
    };
    let mut parts = vec![
        format!("setup {}", phase_span(phases.setup_ms)),
        format!("compile {}{compiled}", phase_span(phases.compile_ms)),
        format!("test {}", phase_span(phases.test_ms)),
    ];
    if phases.doctest_ms > 0 {
        parts.push(format!("doctest {}", phase_span(phases.doctest_ms)));
    }
    parts.push(format!("publish {}", phase_span(phases.publish_ms)));
    let measured = phases.setup_ms
        + phases.compile_ms
        + phases.test_ms
        + phases.doctest_ms
        + phases.publish_ms;
    let outside = u64::try_from(duration_ms)
        .unwrap_or(0)
        .saturating_sub(measured);
    if outside >= 1_000 {
        parts.push(format!("outside the timeline {}", phase_span(outside)));
    }
    let origin = if phases.origin == "executor" {
        ""
    } else {
        " (timed from the runner's start; the command's prelude is outside it)"
    };
    let mut lines = vec![format!("- Phases: {}{origin}", parts.join(" · "))];
    if phases.passes.len() > 1 {
        let passes: Vec<String> = phases
            .passes
            .iter()
            .map(|pass| {
                // Passes that run no test to a verdict (see lane-phases.ts).
                let run = if matches!(pass.name.as_str(), "warmup" | "build") {
                    "run"
                } else {
                    "test"
                };
                let summary = pass
                    .summary_ms
                    .map(|ms| format!(" (nextest {})", phase_span(ms)))
                    .unwrap_or_default();
                format!(
                    "{} compile {}, {run} {}{summary}",
                    pass.name,
                    phase_span(pass.compile_ms),
                    phase_span(pass.test_ms)
                )
            })
            .collect();
        lines.push(format!("- Passes: {}", passes.join(" · ")));
    }
    lines
}

#[cfg(test)]
mod tests {
    use super::*;

    fn param(key: &str, value: &str) -> QueryParam {
        QueryParam {
            key: key.into(),
            value: value.into(),
        }
    }

    async fn request_fixture() -> (tempfile::TempDir, Orchestrator) {
        let db = crate::storage::migrated_test_db("check-request-resource.db").await;
        db.execute_script(
            "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at)
               VALUES('p','w','P','proj','/unused',1,1),('q','w','Q','other','/unused',1,1);
             INSERT INTO runs(id,project_id,status,created_at,updated_at)
               VALUES('run-1','p','running',1,1);",
        )
        .await
        .unwrap();
        let root = tempfile::tempdir().unwrap();
        let search = std::sync::Arc::new(
            crate::storage::SearchIndex::open_or_create(root.path().join("search")).unwrap(),
        );
        let orch = Orchestrator::builder(
            std::sync::Arc::new(crate::db::DbState::new(std::sync::Arc::new(db), search)),
            std::sync::Arc::new(crate::services::testing::TestServicesBuilder::new().build()),
            root.path().to_path_buf(),
        )
        .build();
        (root, orch)
    }

    async fn request_handle_row(
        orch: &Orchestrator,
        response: Option<serde_json::Value>,
    ) -> String {
        let handle = uuid::Uuid::new_v4().to_string();
        orch.db
            .local
            .execute(
                "INSERT INTO manual_check_handles(handle,project_id,created_at,response_json) VALUES(?1,'p',1,?2)",
                cairn_db::turso::params![
                    handle.as_str(),
                    response.map(|value| value.to_string())
                ],
            )
            .await
            .unwrap();
        handle
    }

    #[tokio::test]
    async fn request_handle_reads_pending_progress_and_is_project_fenced() {
        let (_root, orch) = request_fixture().await;
        let handle = request_handle_row(&orch, None).await;
        crate::mcp::handlers::check_run::record_progress_for_test(
            "p",
            &handle,
            crate::execution::checks::ManualCheckProgressUpdate {
                state: "running".into(),
                diagnostic: "running rust-tests".into(),
                executor: Some("fedora".into()),
                since_unix_ms: Some(1_700_000_000_000),
            },
        );
        let page = read_project_check_observation(&orch, "proj", &handle).await;
        assert!(
            page.contains("- State: running (pending, no verdict yet)"),
            "{page}"
        );
        assert!(page.contains("- Progress: running rust-tests"), "{page}");
        assert!(page.contains("- Executor: fedora"), "{page}");
        assert!(page.contains("- Since: "), "{page}");
        // An uppercase spelling names the same request.
        let upper = read_project_check_observation(&orch, "proj", &handle.to_uppercase()).await;
        assert!(upper.contains("running (pending"), "{upper}");
        // A warm progress cache is not authority to read another project's request.
        let foreign = read_project_check_observation(&orch, "other", &handle).await;
        assert!(foreign.contains("unknown in this project"), "{foreign}");
        assert!(!foreign.contains("running"), "{foreign}");
    }

    /// The reference a start acknowledgement prints is the resource a reader
    /// can open: it parses as this resource, names the run's own project, and
    /// reads as the request rather than as an invalid or unknown handle.
    #[tokio::test]
    async fn start_ack_observation_ref_reads_back_as_its_request() {
        let (_root, orch) = request_fixture().await;
        let request = McpCallbackRequest {
            answer_bound_ms: None,
            thread_id: None,
            cwd: String::new(),
            run_id: Some("run-1".into()),
            tool: "check_run".into(),
            payload: serde_json::json!({ "suite": "no-such-suite" }),
            tool_use_id: None,
            origin: None,
        };
        let ack: serde_json::Value = serde_json::from_str(
            &crate::mcp::handlers::check_run::handle_check_run(&orch, &request).await,
        )
        .unwrap();
        assert_eq!(ack["ok"], true, "{ack}");
        let handle = ack["observationHandle"].as_str().unwrap();
        assert!(uuid::Uuid::try_parse(handle).is_ok(), "{ack}");
        let reference = ack["observationRef"].as_str().unwrap();
        let Some(cairn_uri::CairnResource::ProjectCheckObservation {
            project,
            handle: parsed,
        }) = cairn_uri::parse_uri(reference)
        else {
            panic!("{reference} is not a check observation resource");
        };
        assert_eq!(project, "proj");
        assert_eq!(parsed, handle);
        let page = read_project_check_observation(&orch, &project, &parsed).await;
        assert!(
            page.starts_with(&format!("# Check request {handle}")),
            "{page}"
        );
        assert!(!page.contains("Invalid check observation handle"), "{page}");
        assert!(!page.contains("unknown in this project"), "{page}");
        assert!(page.contains(&format!("- Request: {reference}")), "{page}");
        // The unknown suite settles as an explicit failure or is still in
        // flight; either way the request reads as itself.
        assert!(
            ["- State: queued", "- State: running", "- State: failed"]
                .iter()
                .any(|state| page.contains(state)),
            "{page}"
        );
        let foreign = read_project_check_observation(&orch, "other", &parsed).await;
        assert!(foreign.contains("unknown in this project"), "{foreign}");
    }

    #[tokio::test]
    async fn request_handle_without_memory_or_answer_reads_as_interrupted() {
        let (_root, orch) = request_fixture().await;
        let handle = request_handle_row(&orch, None).await;
        let page = read_project_check_observation(&orch, "proj", &handle).await;
        assert!(page.contains("- State: failed"), "{page}");
        assert!(page.contains("interrupted"), "{page}");
        let failed = request_handle_row(
            &orch,
            Some(serde_json::json!({"state":"failed","error":"runner refused"})),
        )
        .await;
        let page = read_project_check_observation(&orch, "proj", &failed).await;
        assert!(page.contains("- Error: runner refused"), "{page}");
        let unknown = uuid::Uuid::new_v4().to_string();
        let page = read_project_check_observation(&orch, "proj", &unknown).await;
        assert!(page.contains("unknown in this project"), "{page}");
    }

    #[tokio::test]
    async fn request_handle_reads_a_complete_exit_code_only_verdict_with_its_ledger_link() {
        let (_root, orch) = request_fixture().await;
        let ledger = "cairn://p/proj/check-observations/0123456789abcdef01234567";
        let handle = request_handle_row(
            &orch,
            Some(serde_json::json!({"state":"complete","result":{
                "checkName":"typecheck","commitSha":"evaluated-commit","treeHash":"evaluated-tree",
                "inputHash":"input","environmentFingerprint":"env","observationRef":ledger,
                "ranAt":"1970-01-01 00:00:00 UTC","reusable":true,"notRecorded":null,
                "disposition":"fresh","passed":true,"verified":true,"unverifiedReason":null,
                "tier":null,"exitCode":0,"noVerdict":null,"outputTail":"tsc ok",
                "executor":{"name":"fedora","os":"linux"},"verdictPlatform":"linux",
                "runtime":null,"placementNote":null,"placementUnmet":null
            }})),
        )
        .await;
        let page = read_project_check_observation(&orch, "proj", &handle).await;
        for line in [
            "- State: complete",
            "- Suite: typecheck",
            "- Verdict: passed",
            "- Commit: evaluated-commit",
            "- Exit code: 0",
            "- Ran on: fedora (linux)",
            &format!("- Ledger observation: {ledger}"),
            "tsc ok",
        ] {
            assert!(page.contains(line), "missing {line:?} in {page}");
        }
        assert!(!page.contains("Tier"), "{page}");
        let foreign = read_project_check_observation(&orch, "other", &handle).await;
        assert!(foreign.contains("unknown in this project"), "{foreign}");
    }

    #[tokio::test]
    async fn ledger_handles_keep_their_shape_and_other_handles_are_refused() {
        let (_root, orch) = request_fixture().await;
        let page = read_project_check_observation(&orch, "proj", "0123456789abcdef01234567").await;
        assert_eq!(page, "Check observation not found");
        let page = read_project_check_observation(&orch, "proj", "not-a-handle").await;
        assert!(
            page.starts_with("Invalid check observation handle"),
            "{page}"
        );
        // A 32-hex UUID spelling is neither shape: request handles are hyphenated.
        let simple = uuid::Uuid::new_v4().simple().to_string();
        let page = read_project_check_observation(&orch, "proj", &simple).await;
        assert!(
            page.starts_with("Invalid check observation handle"),
            "{page}"
        );
    }

    #[test]
    fn request_answer_with_unmet_placement_is_not_a_pass() {
        let page = render_request_observation(
            "proj",
            "h",
            &serde_json::json!({"state":"complete","result":{
                "checkName":"build","passed":true,"verified":true,
                "placementUnmet":"requested windows; recorded linux"
            }}),
        );
        assert!(
            page.contains("- Verdict: no verdict for the requested placement"),
            "{page}"
        );
        assert!(!page.contains("- Verdict: passed"), "{page}");
    }

    #[test]
    fn request_answer_without_a_ledger_row_names_why() {
        let page = render_request_observation(
            "proj",
            "h",
            &serde_json::json!({"state":"complete","result":{
                "checkName":"lint","passed":false,"verified":true,"exitCode":1,
                "observationRef":null,"notRecorded":"ledger write failed","outputTail":""
            }}),
        );
        assert!(page.contains("- Verdict: failed"), "{page}");
        assert!(
            page.contains("- Ledger observation: none. ledger write failed"),
            "{page}"
        );
        assert!(!page.contains("## Output tail"), "{page}");
    }

    #[test]
    fn query_requires_suite_and_environment() {
        assert_eq!(
            parse_query(&[]),
            Err("Missing required query parameter 'suite'".into())
        );
        assert_eq!(
            parse_query(&[param("suite", "rust-tests")]),
            Err("Missing required query parameter 'environment'".into())
        );
        assert!(parse_query(&[
            param("suite", "rust-tests"),
            param("environment", "fingerprint")
        ])
        .is_ok());
    }

    /// The parser's other tests hand it pre-split params; this one starts from
    /// the target string an agent writes, so a grammar that folds
    /// `&environment=` into the suite value fails here as "Missing environment".
    #[test]
    fn query_parses_from_the_raw_target_in_either_order() {
        for target in [
            "cairn://p/cairn/check-results/main?suite=rust-tests&environment=current",
            "cairn://p/cairn/check-results/main?environment=current&suite=rust-tests",
        ] {
            let split = cairn_common::query::split_target_query(target).unwrap();
            let query = parse_query(&split.params).unwrap();
            assert_eq!(query.suite, "rust-tests", "{target}");
            assert_eq!(query.environment, "current", "{target}");
        }
    }

    #[test]
    fn query_rejects_unknown_and_duplicate_parameters() {
        assert!(parse_query(&[
            param("suite", "rust"),
            param("environment", "fp"),
            param("fallback", "true")
        ])
        .unwrap_err()
        .contains("Unsupported"));
        assert!(parse_query(&[
            param("suite", "rust"),
            param("suite", "api"),
            param("environment", "fp")
        ])
        .is_err());
    }

    #[test]
    fn query_validates_pagination_and_filters() {
        let base = [param("suite", "rust"), param("environment", "fp")];
        for (key, value) in [
            ("limit", "0"),
            ("limit", "1001"),
            ("limit", "many"),
            ("offset", "-1"),
            ("status", "unknown"),
            ("name", " "),
        ] {
            let mut params = base.to_vec();
            params.push(param(key, value));
            assert!(parse_query(&params).is_err(), "accepted {key}={value}");
        }
        let mut valid = base.to_vec();
        valid.extend([
            param("status", "failed"),
            param("name", "parser"),
            param("limit", "25"),
            param("offset", "50"),
        ]);
        let query = parse_query(&valid).unwrap();
        assert_eq!(
            parse_query(&[
                param("suite", "rust"),
                param("environment", "unfingerprinted"),
            ])
            .unwrap()
            .environment_fingerprint,
            ""
        );
        assert_eq!(
            (
                query.status.as_deref(),
                query.name.as_deref(),
                query.limit,
                query.offset
            ),
            (Some("failed"), Some("parser"), 25, 50)
        );
    }

    #[test]
    fn miss_names_exact_coordinate_and_fresh_action() {
        let body = render_miss("cairn", "main", "abc123", "rust-tests", "env123");
        assert!(body.contains("Resolved commit: abc123"));
        assert!(body.contains("Environment fingerprint: env123"));
        assert!(body.contains("Run the suite fresh"));
        assert!(body.contains("do not substitute"));
    }

    fn cached_observation() -> CheckResultObservationProjection {
        CheckResultObservationProjection {
            disposition: "cached".into(),
            defined_by_commit_sha: Some("target-commit".into()),
            source_defined_by_commit_sha: Some("source-commit".into()),
            evaluated_tree_hash: "target-tree".into(),
            evaluated_input_hash: "target-input".into(),
            evaluated_at: 1_754_246_321_000,
            observation_id: "obs-1".into(),
            project_id: "project-1".into(),
            source_commit_sha: "source-commit".into(),
            source_tree_hash: "source-tree".into(),
            check_name: "rust-tests".into(),
            source_input_hash: "source-input".into(),
            environment_fingerprint: "env-1".into(),
            exit_code: 1,
            verdict: "failed".into(),
            failure_kind: None,
            complete: true,
            reusable: false,
            non_reusable_reason: Some("red verdict".into()),
            parser_version: 2,
            result_schema_version: 1,
            ran_at: 1_754_246_320_000,
            duration_ms: 50,
            job_id: Some("job-1".into()),
            run_id: Some("run-1".into()),
            cadence: "review".into(),
            executor_id: Some("executor-1".into()),
            executor_device_id: Some("device-1".into()),
            executor_connection_generation: Some(3),
            executor_cell_id: Some("cell-1".into()),
            executor_lease_epoch: Some(4),
            executor_started_at_unix_ms: Some(90),
            executor_finished_at_unix_ms: Some(100),
            runner_build_id: Some("runner-1".into()),
            toolchain_fingerprint: Some("tools-1".into()),
            verdict_platform: Some("linux".into()),
            verdict_arch: Some("x86_64".into()),
            output_tail: "failed".into(),
            tests: vec![
                crate::execution::cache::CheckTestResultRow {
                    test_id: "crate::passes".into(),
                    status: "passed".into(),
                    duration_ms: Some(10),
                    attempt_count: Some(1),
                    failure_excerpt: None,
                    skip_reason: None,
                    declaration_source: None,
                    flaky: false,
                    started_offset_ms: Some(1_500),
                },
                crate::execution::cache::CheckTestResultRow {
                    test_id: "crate::fails".into(),
                    status: "failed".into(),
                    duration_ms: Some(20),
                    attempt_count: Some(2),
                    failure_excerpt: Some("assertion failed".into()),
                    skip_reason: None,
                    declaration_source: None,
                    flaky: true,
                    started_offset_ms: None,
                },
            ],
            selection: Some("full".into()),
            deferred_count: 0,
            phases: None,
            execution_warmth: None,
            cell_origin: None,
            build_jobs: None,
            persisted_counts: crate::execution::cache::ObservationTestCounts {
                passed: 2,
                failed: 1,
                ..Default::default()
            },
            verdict_result: None,
            public_handle: Some("0123456789abcdef01234567".into()),
            test_total: 3,
            test_offset: 0,
        }
    }

    fn counts_page(observation: &CheckResultObservationProjection) -> String {
        let query = parse_query(&[
            param("suite", "rust-tests"),
            param("environment", "env-1"),
            param("status", "failed"),
            param("name", "crate"),
            param("limit", "2"),
        ])
        .unwrap();
        render_observation(
            "cairn",
            "main",
            "target-commit",
            &query,
            observation,
            None,
            None,
        )
    }

    fn compact(value: serde_json::Value) -> crate::execution::check_parsers::ParsedCheckResult {
        serde_json::from_value(value).unwrap()
    }

    #[test]
    fn a_vitest_observation_without_accounting_is_not_described_in_nextest_terms() {
        let mut observation = cached_observation();
        observation.verdict_result = Some(compact(serde_json::json!({
            "parser": "vitest", "complete": true, "selection": "full",
            "passed": 2, "failed": 1, "skipped": 0, "failures": []
        })));
        let page = counts_page(&observation);
        assert!(
            page.contains("no runner tally was recorded for this vitest result"),
            "{page}"
        );
        assert!(!page.contains("nextest Summary"), "{page}");
        assert!(page.contains("## Evidence accounting"));
        assert!(page.contains("- Class: uninstrumented"), "{page}");
    }

    #[test]
    fn stored_rows_short_of_the_accounting_revoke_the_shown_authority() {
        let mut observation = cached_observation();
        observation.reusable = true;
        observation.verdict_result = Some(compact(serde_json::json!({
            "parser": "vitest", "complete": true, "selection": "full",
            "passed": 3, "failed": 1, "skipped": 0, "failures": [],
            "ingestion": {"received": 4, "kept": 4},
            "accounting": {
                "version": 1,
                "inventory": {"state": "listed", "unit": "file", "count": 2, "unreported": 0, "unreportedIds": []},
                "passes": [{
                    "pass": "vitest", "processes": 1, "started": null,
                    "enumerated": {"passed": 3, "failed": 1, "skipped": 0, "deferred": 0},
                    "runner": {"source": "vitest-report", "total": 4, "passed": 3, "failed": 1, "skipped": 0, "timedOut": 0},
                    "runnerReconciled": true
                }],
                "final": {"passed": 3, "failed": 1, "skipped": 0, "deferred": 0, "rows": 4, "duplicateIds": 0}
            }
        })));
        // The fixture stores 2 passed + 1 failed: one row short of the 4 kept.
        let page = counts_page(&observation);
        assert!(page.contains("- Class: lost evidence"), "{page}");
        assert!(
            page.contains("- Runner-reported (vitest-report, pass `vitest`): 4 total"),
            "{page}"
        );
        assert!(page.contains("- Complete: false"), "{page}");
        assert!(page.contains("- Reusable: false"), "{page}");
        assert!(page.contains(
            "Recorded authority (overridden by count mismatch): complete=true, reusable=true"
        ));
        assert!(
            page.contains("- Expected inventory: listed: 2 files, 0 unreported"),
            "{page}"
        );
        // A cached alias says whose execution these populations are.
        assert!(
            page.contains("- Execution: reused from the execution at source-commi (cairn://p/cairn/check-observations/0123456789abcdef01234567) (not counted as a fresh execution)"),
            "{page}"
        );

        observation.persisted_counts.passed = 3;
        let page = counts_page(&observation);
        assert!(page.contains("- Class: accounted"), "{page}");
        assert!(
            page.contains("- Count reconciliation: matched link by link"),
            "{page}"
        );
        assert!(page.contains("- Reusable: true"), "{page}");
    }

    #[test]
    fn historical_summary_mismatch_keeps_independent_counts_and_revokes_page_authority() {
        let mut observation = cached_observation();
        observation.reusable = true;
        observation.output_tail =
            "Summary [ 144.635s] 13587 tests run: 13586 passed, 1 failed, 51 skipped".into();
        observation.persisted_counts = crate::execution::cache::ObservationTestCounts {
            passed: 13565,
            failed: 1,
            skipped: 44,
            deferred: 0,
            self_skipped: 21,
        };
        // Only one filtered row on the page, not the comparison denominator.
        observation.tests.retain(|test| test.status == "failed");
        observation.test_total = 1;
        let body = counts_page(&observation);
        for expected in [
            "Runner-reported (nextest Summary): 13638 total; 13586 passed, 1 failed, 51 skipped",
            "Persisted outcomes (global): 13610 total; 13565 passed, 1 failed, 44 skipped",
            "Self-skips: 21",
            "MISMATCH after 21 self-skip",
            "- Complete: false",
            "- Reusable: false",
            "Recorded authority (overridden by count mismatch): complete=true, reusable=true",
            "0 passed, 1 failed, 0 skipped on this page",
            "Total: 1 (matching current filters)",
        ] {
            assert!(body.contains(expected), "missing {expected:?}: {body}");
        }
        assert!(
            observation.complete,
            "rendering never mutates the historical row"
        );
    }

    #[test]
    fn matching_summary_accounts_for_self_skip_transfers_not_page_counts() {
        let mut observation = cached_observation();
        observation.output_tail =
            "Summary [ 1.000s] 13587 tests run: 13586 passed, 1 failed, 51 skipped".into();
        observation.persisted_counts = crate::execution::cache::ObservationTestCounts {
            passed: 13565,
            failed: 1,
            skipped: 72,
            deferred: 0,
            self_skipped: 21,
        };
        let body = counts_page(&observation);
        assert!(body.contains("Count reconciliation: matched after self-skip"));
        assert!(!body.contains("MISMATCH"));
        assert!(body.contains("- Complete: true"));
        assert!(body.contains("1 passed, 1 failed, 0 skipped on this page"));
        assert!(body.contains("Persisted outcomes (global): 13638 total"));
        assert!(body.contains("Total: 3 (matching current filters)"));
    }

    #[test]
    fn filtered_selection_shows_both_counts_without_treating_unselected_skips_as_missing() {
        let mut observation = cached_observation();
        observation.selection = Some("filtered".into());
        observation.reusable = true;
        observation.output_tail =
            "Summary [ 1.000s] 3 tests run: 2 passed, 1 failed, 100 skipped".into();
        let body = counts_page(&observation);
        assert!(body.contains("Runner-reported (nextest Summary): 103 total"));
        assert!(body.contains("Persisted outcomes (global): 3 total"));
        assert!(body.contains("Count reconciliation: unavailable (filtered selection"));
        assert!(body.contains("- Complete: true"));
        assert!(body.contains("- Reusable: true"));
        assert!(!body.contains("MISMATCH"));
    }

    #[test]
    fn no_summary_uses_global_persisted_counts_with_unavailable_runner_witness() {
        let observation = cached_observation();
        let body = counts_page(&observation);
        assert!(body.contains("Persisted outcomes (global): 3 total; 2 passed, 1 failed"));
        assert!(body.contains("Runner witness unavailable"));
        assert!(body.contains("Count reconciliation: unavailable"));
        assert!(!body.contains("Runner-reported"));
    }

    #[test]
    fn multiple_or_composed_passes_do_not_borrow_the_last_summary() {
        let mut observation = cached_observation();
        let summary = "Summary [ 1.000s] 2 tests run: 1 passed, 1 failed, 1 skipped";
        observation.output_tail =
            format!("{summary}\nSummary [ 2.000s] 3 tests run: 3 passed, 0 skipped");
        let body = counts_page(&observation);
        assert!(body.contains("Runner witness unavailable"));
        assert!(!body.contains("Count reconciliation: matched"));
        observation.output_tail = summary.into();
        observation.selection = Some(crate::execution::check_parsers::FAST_TIER_SELECTION.into());
        let body = counts_page(&observation);
        assert!(body.contains("composed fast tier"));
        assert!(!body.contains("Runner-reported"));
    }

    #[test]
    fn multiple_recorded_passes_make_a_lone_retained_summary_ambiguous() {
        let mut observation = cached_observation();
        observation.output_tail =
            "Summary [ 1.000s] 3 tests run: 2 passed, 1 failed, 0 skipped".into();
        observation.phases = Some(
            serde_json::from_value(serde_json::json!({
                "origin": "executor", "setupMs": 0, "compileMs": 0, "testMs": 1, "publishMs": 0,
                "passes": [
                    {"name": "local", "wallMs": 1, "compileMs": 0, "testMs": 1},
                    {"name": "global", "wallMs": 1, "compileMs": 0, "testMs": 1}
                ]
            }))
            .unwrap(),
        );
        let body = counts_page(&observation);
        assert!(body.contains("multiple test passes"));
        assert!(!body.contains("Count reconciliation: matched"));
    }

    #[test]
    fn single_pass_timeouts_reconcile_as_failed_outcomes_not_deferrals() {
        let mut observation = cached_observation();
        observation.output_tail =
            "Summary [ 1.000s] 3 tests run: 2 passed, 1 timed out, 0 skipped".into();
        let body = counts_page(&observation);
        assert!(body.contains("0 failed, 0 skipped, 1 timed out"));
        assert!(body.contains("Count reconciliation: matched"));
        observation.persisted_counts.failed = 0;
        observation.persisted_counts.deferred = 1;
        assert!(counts_page(&observation).contains("MISMATCH"));
    }

    #[test]
    fn duplicate_retained_summary_is_one_witness_and_status_mismatch_is_not_hidden_by_total() {
        let mut observation = cached_observation();
        let summary = "Summary [ 1.000s] 3 tests run: 2 passed, 1 failed, 0 skipped";
        observation.output_tail = format!("{summary}\n…\n{summary}");
        assert!(counts_page(&observation).contains("Count reconciliation: matched"));
        observation.persisted_counts.passed = 1;
        observation.persisted_counts.failed = 2;
        let body = counts_page(&observation);
        assert!(body.contains("MISMATCH"));
        assert!(body.contains("runner total 3, persisted total 3"));
    }

    #[test]
    fn observation_renders_provenance_authority_and_every_test_name() {
        let observation = cached_observation();
        let query = parse_query(&[
            param("suite", "rust-tests"),
            param("environment", "env-1"),
            param("limit", "2"),
        ])
        .unwrap();
        let body = render_observation(
            "cairn",
            "main",
            "target-commit",
            &query,
            &observation,
            None,
            None,
        );
        for expected in [
            "cached observation",
            "Citation: [rust-tests@target-commi](cairn://p/cairn/check-results/target-commit?suite=rust-tests&environment=env-1)",
            "Resolved commit: target-commit",
            "Defined by commit: target-commit",
            "Source commit: source-commit",
            "Source defined by commit: source-commit",
            "Evaluated at: 2025-",
            "Ran at: 2025-",
            "Non-authoritative reason: red verdict",
            "1 passed, 1 failed, 0 skipped",
            "- [passed] crate::passes (10 ms, started 2 s into the lane)",
            "- [failed] crate::fails (20 ms); flaky",
            "flaky",
            "2 attempts",
            "Total: 3",
            "offset=2",
        ] {
            assert!(body.contains(expected), "missing {expected:?} from {body}");
        }
        assert!(!body.contains("obs-1"), "internal UUID/key leaked: {body}");
    }

    /// The verdict platform is shown with the machine that ran the lane and
    /// the platform that machine is known by, so the two can be compared.
    #[test]
    fn observation_names_its_verdict_platform_beside_the_machine_that_ran_it() {
        let mut observation = cached_observation();
        let query =
            parse_query(&[param("suite", "rust-tests"), param("environment", "env-1")]).unwrap();
        let body = render_observation(
            "cairn",
            "main",
            "target-commit",
            &query,
            &observation,
            None,
            Some("fedora (linux/x86_64)"),
        );
        assert!(
            body.contains("- Verdict platform: linux/x86_64 · ran on fedora (linux/x86_64)"),
            "{body}"
        );

        observation.verdict_platform = None;
        observation.verdict_arch = None;
        let body = render_observation(
            "cairn",
            "main",
            "target-commit",
            &query,
            &observation,
            None,
            None,
        );
        assert!(body.contains("- Verdict platform: unrecorded\n"), "{body}");
    }

    /// The runtime the row was produced under is named right under its
    /// platform, so a Bun 1.3.14 red and a Bun 1.4.4 green read as the two
    /// different verdicts they are.
    #[test]
    fn observation_names_its_runtime_beside_its_platform() {
        let mut observation = cached_observation();
        observation.toolchain_fingerprint = Some(
            "rustc=rustc 1.97.1 (8bab26f4f 2026-07-14)\nhost: x86_64-unknown-linux-gnu;bun=1.3.14"
                .into(),
        );
        let query =
            parse_query(&[param("suite", "rust-tests"), param("environment", "env-1")]).unwrap();
        let render = |observation: &CheckResultObservationProjection| {
            render_observation(
                "cairn",
                "main",
                "target-commit",
                &query,
                observation,
                None,
                Some("bginf-sp1 (linux/x86_64)"),
            )
        };
        let body = render(&observation);
        assert!(
            body.contains(
                "- Verdict platform: linux/x86_64 · ran on bginf-sp1 (linux/x86_64)\n\
                 - Runtime: rustc 1.97.1 (8bab26f4f 2026-07-14), bun 1.3.14\n"
            ),
            "{body}"
        );
        observation.toolchain_fingerprint = Some(
            "rustc=rustc 1.97.1 (8bab26f4f 2026-07-14)\nhost: x86_64-unknown-linux-gnu;bun=1.4.4;node=18.19.1"
                .into(),
        );
        assert!(
            render(&observation).contains(
                "- Runtime: rustc 1.97.1 (8bab26f4f 2026-07-14), bun 1.4.4, node 18.19.1\n"
            ),
            "a run that measured node names it"
        );
        observation.toolchain_fingerprint = None;
        assert!(render(&observation).contains("- Runtime: unrecorded\n"));
        observation.toolchain_fingerprint = Some("tools-1".into());
        assert!(render(&observation).contains("- Runtime: unrecorded\n"));
    }

    /// Specimen c47874ea's split and a tiered lane's three passes, as the page
    /// shows them.
    #[test]
    fn observation_renders_where_its_minutes_went() {
        use crate::execution::check_parsers::{CheckPhasePass, CheckPhases};
        let pass = |name: &str, compile_ms, test_ms, summary_ms| CheckPhasePass {
            name: name.into(),
            wall_ms: compile_ms + test_ms,
            compile_ms,
            test_ms,
            compiled_units: 0,
            compiled_crates: Vec::new(),
            summary_ms,
        };
        let mut observation = cached_observation();
        observation.duration_ms = 571_214;
        observation.execution_warmth = Some("preparedWarmSlot".into());
        observation.cell_origin = Some(crate::execution::cache::ObservedCellOrigin {
            kind: "reused".into(),
            distance_files: Some(12),
            distance_crates: Some(3),
        });
        observation.phases = Some(CheckPhases {
            origin: "executor".into(),
            setup_ms: 12_000,
            compile_ms: 408_000,
            test_ms: 149_735,
            doctest_ms: 0,
            publish_ms: 1_479,
            compiled_units: Some(37),
            passes: vec![pass("full", 408_000, 149_735, Some(144_635))],
        });
        let query =
            parse_query(&[param("suite", "rust-tests"), param("environment", "env-1")]).unwrap();
        let render = |observation: &CheckResultObservationProjection| {
            render_observation(
                "cairn",
                "main",
                "target-commit",
                &query,
                observation,
                None,
                None,
            )
        };
        let body = render(&observation);
        assert!(
            body.contains(
                "- Phases: setup 12 s · compile 6 m 48 s (37 crates compiled) · test 2 m 30 s · publish 1 s\n"
            ),
            "{body}"
        );
        assert!(
            body.contains(
                "- Cell: preparedWarmSlot · origin reused (12 files / 3 crates from the commit)\n"
            ),
            "{body}"
        );
        assert!(
            !body.contains("- Passes:"),
            "one pass needs no breakdown: {body}"
        );
        assert!(!body.contains("- Build jobs:"), "{body}");
        const GIB: u64 = 1 << 30;
        observation.build_jobs = Some(cairn_common::executor_protocol::BuildJobs::new(
            8,
            Some(cairn_common::executor_protocol::MemoryJobBound::new(
                12 * GIB,
                0,
                5 * GIB / 2,
                1,
            )),
        ));
        let body = render(&observation);
        assert!(
            body.contains(
                "- Build jobs: 4 build jobs, held below the CPU width of 8 by memory: 12.0 GiB available at start / 2.5 GiB per job\n"
            ),
            "{body}"
        );

        observation.duration_ms = 836_000;
        observation.phases = Some(CheckPhases {
            origin: "process".into(),
            setup_ms: 11_000,
            compile_ms: 430_000,
            test_ms: 388_000,
            doctest_ms: 0,
            publish_ms: 3_000,
            compiled_units: Some(0),
            passes: vec![
                pass("local", 25_000, 65_000, Some(63_900)),
                pass("warmup", 404_000, 6_000, Some(5_400)),
                pass("global", 1_000, 323_000, Some(312_004)),
            ],
        });
        let body = render(&observation);
        assert!(
            body.contains(
                "- Phases: setup 11 s · compile 7 m 10 s (nothing stale) · test 6 m 28 s · publish 3 s · outside the timeline 4 s (timed from the runner's start; the command's prelude is outside it)\n"
            ),
            "{body}"
        );
        assert!(
            body.contains(
                "- Passes: local compile 25 s, test 1 m 5 s (nextest 1 m 4 s) · warmup compile 6 m 44 s, run 6 s (nextest 5 s) · global compile 1 s, test 5 m 23 s (nextest 5 m 12 s)\n"
            ),
            "{body}"
        );
    }

    /// A legacy row has no truthful defining commit. It says so, rather than
    /// letting the reader infer the evaluated commit defined the check.
    #[test]
    fn a_row_without_definition_provenance_names_the_gap() {
        let mut observation = cached_observation();
        observation.defined_by_commit_sha = None;
        observation.source_defined_by_commit_sha = None;
        let query =
            parse_query(&[param("suite", "rust-tests"), param("environment", "env-1")]).unwrap();
        let body = render_observation(
            "cairn",
            "main",
            "target-commit",
            &query,
            &observation,
            None,
            None,
        );
        assert!(body.contains("Defined by commit: unrecorded (legacy row)"));
        assert!(body.contains("Source defined by commit: unrecorded (legacy row)"));
    }

    /// A script that exits before reporting a test leaves its reason only in the
    /// retained output, so the observation shows it.
    #[test]
    fn observation_renders_its_retained_output() {
        let mut observation = cached_observation();
        observation.tests.clear();
        observation.output_tail =
            "stderr:\n```\nExecutor protocol check could not evaluate\n".into();
        let query =
            parse_query(&[param("suite", "rust-tests"), param("environment", "env-1")]).unwrap();
        let body = render_observation(
            "cairn",
            "main",
            "target-commit",
            &query,
            &observation,
            None,
            None,
        );
        assert!(body.contains("## Output (tail)"), "{body}");
        assert!(body.contains("Executor protocol check could not evaluate"));
        assert!(
            body.contains("````text"),
            "fence must outrun the output's own: {body}"
        );

        observation.output_tail = "  \n".into();
        let body = render_observation(
            "cairn",
            "main",
            "target-commit",
            &query,
            &observation,
            None,
            None,
        );
        assert!(!body.contains("## Output"));
    }
}
