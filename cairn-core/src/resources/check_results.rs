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
        if !matches!(value, "passed" | "failed" | "skipped") {
            return Err(
                "Query parameter 'status' must be one of: passed, failed, skipped".to_string(),
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

fn render_observation(
    project: &str,
    revision: &str,
    commit: &str,
    query: &CheckResultsQuery,
    observation: &CheckResultObservationProjection,
    permalink: Option<&str>,
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
    ];
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
    body.extend([
        String::new(),
        "## Tests".to_string(),
        String::new(),
        format!(
            "{} passed, {} failed, {} skipped on this page",
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
    body.join("\n")
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
        let body = render_observation("cairn", "main", "target-commit", &query, &observation, None);
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

    /// A legacy row has no truthful defining commit. It says so, rather than
    /// letting the reader infer the evaluated commit defined the check.
    #[test]
    fn a_row_without_definition_provenance_names_the_gap() {
        let mut observation = cached_observation();
        observation.defined_by_commit_sha = None;
        observation.source_defined_by_commit_sha = None;
        let query =
            parse_query(&[param("suite", "rust-tests"), param("environment", "env-1")]).unwrap();
        let body = render_observation("cairn", "main", "target-commit", &query, &observation, None);
        assert!(body.contains("Defined by commit: unrecorded (legacy row)"));
        assert!(body.contains("Source defined by commit: unrecorded (legacy row)"));
    }
}
