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
    if handle.len() != 24 || !handle.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return "Invalid check observation handle".to_string();
    }
    let db = orch.db.for_project(project).await;
    let project_id = match project_id_by_key(&db, project).await {
        Ok(project_id) => project_id,
        Err(error) => return error,
    };
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
    let query = match parse_query(params) {
        Ok(query) => query,
        Err(error) => return error,
    };
    let db = orch.db.for_project(project).await;
    let project_id = match project_id_by_key(&db, project).await {
        Ok(project_id) => project_id,
        Err(error) => return error,
    };
    let resolution = match resolve_for_read(orch, request, revision).await {
        Ok(resolution) => resolution,
        Err(error) => return format!("Unable to resolve requested revision '{revision}': {error}"),
    };
    if resolution.project_id != project_id {
        return format!("Requested revision '{revision}' resolved in a different project; refusing to substitute that commit");
    }
    let mut query = query;
    if query.environment == "current" {
        let config = match crate::mcp::handlers::read::file_at_commit(
            resolution.object_repository_path.clone(),
            resolution.commit_id.clone(),
            ".cairn/config.yaml",
        ) {
            Ok(Some(bytes)) => bytes,
            Ok(None) => return format!("Unable to resolve suite '{}' at requested revision '{revision}': .cairn/config.yaml is unavailable", query.suite),
            Err(error) => return format!("Unable to resolve suite '{}' at requested revision '{revision}': {error}", query.suite),
        };
        let settings: crate::config::project_settings::ProjectSettingsFile = match serde_yaml::from_slice(&config) {
            Ok(settings) => settings,
            Err(error) => return format!("Unable to resolve suite '{}' at requested revision '{revision}': invalid .cairn/config.yaml: {error}", query.suite),
        };
        let Some(check) = settings
            .checks
            .as_ref()
            .and_then(|checks| checks.get(&query.suite))
        else {
            return format!("Unable to resolve suite '{}' at requested revision '{revision}': suite is not configured", query.suite);
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
        Ok(Some(observation)) => render_observation(
            project,
            revision,
            &resolution.commit_id,
            &query,
            &observation,
            None,
            ran_on(orch, &observation).as_deref(),
        ),
        Ok(None) => render_miss(
            project,
            revision,
            &resolution.commit_id,
            &query.suite,
            &query.environment_fingerprint,
        ),
        Err(error) => format!("Unable to read check observation: {error}"),
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

fn render_observation(
    project: &str,
    revision: &str,
    commit: &str,
    query: &CheckResultsQuery,
    observation: &CheckResultObservationProjection,
    permalink: Option<&str>,
    ran_on: Option<&str>,
) -> String {
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
        format!("- Complete: {}", observation.complete),
        format!("- Reusable: {}", observation.reusable),
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
    // A fast-tier observation has a row for every test it deferred, so a test
    // with no row here was not selected; it was never silently unobserved.
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
    if let Some(line) = cell_line(
        observation.execution_warmth.as_deref(),
        observation.cell_origin.as_ref(),
    ) {
        body.push(line);
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
        format!("- Total: {}", observation.test_total),
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
    for test in &observation.tests {
        let mut line = format!("- [{}] {}", test.status, test.test_id);
        if let Some(duration) = test.duration_ms {
            line.push_str(&format!(" ({duration} ms)"));
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
    body.join("\n")
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
                let run = if pass.name == "warmup" { "run" } else { "test" };
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
                },
            ],
            selection: Some("filtered".into()),
            deferred_count: 0,
            phases: None,
            execution_warmth: None,
            cell_origin: None,
            test_total: 3,
            test_offset: 0,
        }
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
            "crate::passes",
            "crate::fails",
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
