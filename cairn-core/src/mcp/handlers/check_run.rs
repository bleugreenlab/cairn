//! Authenticated manual configured-check producer.
//!
//! This tool is intentionally narrower than `run`: callers select a configured
//! suite and an optional logical coordinate, but cannot supply executable text,
//! identity hashes, environment claims, or a verdict.

use serde::Deserialize;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use crate::mcp::types::McpCallbackRequest;
use crate::orchestrator::Orchestrator;

/// Why a caller with no run cannot have an observation.
///
/// This producer's whole value is that its result is *recorded* — keyed to a
/// sealed commit and reusable as review evidence — and the coordinate it records
/// against is the run's job and logical head. A caller with no run has no such
/// coordinate, and inventing one from the process's working directory is exactly
/// the influence [`crate::execution::checks::manual_check_cache_context`]
/// excludes by construction. So the refusal states the contract rather than the
/// missing field: a shell that should have a run and does not has a plumbing
/// problem worth naming, and a shell that legitimately has none is asking for a
/// capability that does not exist yet.
const MISSING_RUN: &str = "cairn check run records an observation against the sealed commit of the run whose work it is, and this request carries no run. Cairn hands every agent shell its run; a shell started outside a run has no job or logical head to record against.";

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CheckRunPayload {
    #[serde(default)]
    suite: Option<String>,
    #[serde(default)]
    branch: Option<String>,
    #[serde(default)]
    observation_handle: Option<String>,
    #[serde(default)]
    retry: bool,
}

#[derive(Clone, serde::Serialize)]
#[serde(rename_all = "camelCase", tag = "state")]
enum ManualCheckObservation {
    Queued {
        diagnostic: String,
    },
    Running {
        diagnostic: String,
    },
    Parked {
        diagnostic: String,
    },
    Complete {
        result: Box<crate::execution::checks::ManualConfiguredCheckResult>,
    },
    Failed {
        error: String,
    },
}

struct RetainedObservation {
    observation: ManualCheckObservation,
    updated_at: Instant,
}

const SETTLED_OBSERVATION_RETENTION: Duration = Duration::from_secs(10 * 60);

static MANUAL_CHECKS: LazyLock<Arc<Mutex<HashMap<String, RetainedObservation>>>> =
    LazyLock::new(|| Arc::new(Mutex::new(HashMap::new())));

fn is_settled(observation: &ManualCheckObservation) -> bool {
    matches!(
        observation,
        ManualCheckObservation::Complete { .. } | ManualCheckObservation::Failed { .. }
    )
}

fn retain_observation(
    checks: &mut HashMap<String, RetainedObservation>,
    handle: String,
    observation: ManualCheckObservation,
) {
    let now = Instant::now();
    checks.retain(|_, retained| {
        !is_settled(&retained.observation)
            || now.duration_since(retained.updated_at) < SETTLED_OBSERVATION_RETENTION
    });
    checks.insert(
        handle,
        RetainedObservation {
            observation,
            updated_at: now,
        },
    );
}

fn load_observation(handle: &str) -> Result<ManualCheckObservation, String> {
    let mut checks = MANUAL_CHECKS
        .lock()
        .map_err(|_| "manual check observation registry is unavailable".to_string())?;
    let now = Instant::now();
    checks.retain(|_, retained| {
        !is_settled(&retained.observation)
            || now.duration_since(retained.updated_at) < SETTLED_OBSERVATION_RETENTION
    });
    checks
        .get(handle)
        .map(|retained| retained.observation.clone())
        .ok_or_else(|| format!("manual check observation {handle} is unknown or expired"))
}

pub(crate) async fn handle_check_run(orch: &Orchestrator, request: &McpCallbackRequest) -> String {
    let result = async {
        let run_id = request
            .run_id
            .as_deref()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| MISSING_RUN.to_string())?;
        let payload: CheckRunPayload = serde_json::from_value(request.payload.clone())
            .map_err(|error| format!("invalid check_run payload: {error}"))?;
        if let Some(handle) = payload.observation_handle.as_deref() {
            return load_observation(handle).map(|observation| (None, observation));
        }
        let suite = payload
            .suite
            .as_deref()
            .filter(|suite| !suite.trim().is_empty())
            .ok_or_else(|| "configured suite name must not be empty".to_string())?;
        let handle = uuid::Uuid::new_v4().to_string();
        retain_observation(
            &mut *MANUAL_CHECKS
                .lock()
                .map_err(|_| "manual check observation registry is unavailable".to_string())?,
            handle.clone(),
            ManualCheckObservation::Queued {
                diagnostic: format!("queued {suite}"),
            },
        );
        let task_orch = orch.clone();
        let task_run_id = run_id.to_string();
        let task_suite = suite.to_string();
        let task_branch = payload.branch.clone();
        let task_retry = payload.retry;
        let task_handle = handle.clone();
        tokio::spawn(async move {
            let progress_handle = task_handle.clone();
            let progress = Arc::new(move |state: &str, diagnostic: String| {
                let next = if state == "parked" {
                    ManualCheckObservation::Parked { diagnostic }
                } else {
                    ManualCheckObservation::Running { diagnostic }
                };
                if let Ok(mut checks) = MANUAL_CHECKS.lock() {
                    retain_observation(&mut checks, progress_handle.clone(), next);
                }
            });
            let final_state =
                match crate::execution::checks::run_manual_configured_check_with_progress(
                    &task_orch,
                    &task_run_id,
                    &task_suite,
                    task_branch.as_deref(),
                    task_retry,
                    progress,
                )
                .await
                {
                    Ok(result) => ManualCheckObservation::Complete {
                        result: Box::new(result),
                    },
                    Err(error) => ManualCheckObservation::Failed { error },
                };
            if let Ok(mut checks) = MANUAL_CHECKS.lock() {
                retain_observation(&mut checks, task_handle, final_state);
            }
        });
        Ok((
            Some(handle),
            ManualCheckObservation::Queued {
                diagnostic: format!("queued {suite}"),
            },
        ))
    }
    .await;

    match result {
        Ok((Some(handle), observation)) => serde_json::json!({
            "ok": true, "observationHandle": handle, "observation": observation
        })
        .to_string(),
        Ok((None, observation)) => {
            serde_json::json!({ "ok": true, "observation": observation }).to_string()
        }
        Err(error) => serde_json::json!({ "ok": false, "error": error }).to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_accepts_only_suite_and_optional_branch() {
        let payload: CheckRunPayload = serde_json::from_value(serde_json::json!({
            "suite": "rust-tests",
            "branch": "main"
        }))
        .unwrap();
        assert_eq!(payload.suite.as_deref(), Some("rust-tests"));
        assert_eq!(payload.branch.as_deref(), Some("main"));

        let asserted_command = serde_json::from_value::<CheckRunPayload>(serde_json::json!({
            "suite": "rust-tests",
            "command": "cargo test"
        }));
        assert!(
            asserted_command.is_err(),
            "raw commands must not cross the trusted producer boundary"
        );
    }

    #[test]
    fn parked_observation_is_a_first_class_state() {
        let value = serde_json::to_value(ManualCheckObservation::Parked {
            diagnostic: "parked behind frontend-build".into(),
        })
        .unwrap();
        assert_eq!(value["state"], "parked");
        assert_eq!(value["diagnostic"], "parked behind frontend-build");
    }

    #[test]
    fn pruning_expires_only_settled_observations() {
        let old = Instant::now() - SETTLED_OBSERVATION_RETENTION - Duration::from_secs(1);
        let mut checks = HashMap::from([
            (
                "active".into(),
                RetainedObservation {
                    observation: ManualCheckObservation::Parked {
                        diagnostic: "waiting".into(),
                    },
                    updated_at: old,
                },
            ),
            (
                "settled".into(),
                RetainedObservation {
                    observation: ManualCheckObservation::Failed {
                        error: "done".into(),
                    },
                    updated_at: old,
                },
            ),
        ]);
        retain_observation(
            &mut checks,
            "new".into(),
            ManualCheckObservation::Running {
                diagnostic: "running".into(),
            },
        );
        assert!(checks.contains_key("active"));
        assert!(!checks.contains_key("settled"));
    }
}
