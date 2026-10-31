//! Writes to services.
//!
//! A project service (`cairn://p/<project>/services/<id>`) is declared in the
//! repository, so its resource takes `patch {action}` to start, stop, or
//! restart it and `append {method, params?, timeoutSeconds?}` to call it.
//! An agent run in the service's own project, or the operator (a request with
//! no run), may act.
//!
//! A thread's service (`cairn://p/<project>/<thread>/services/<name>`) is
//! written like a page: `create` replaces its package and `patch` revises it
//! (files merge per path), each appending a version that is deployed as it
//! lands. `patch {action}` alone acts on it, and `append` calls it. Only the
//! owning thread (its sessions and their tasks) writes or acts on it, or the
//! operator acts; only callers acting for that thread call it.
//!
//! A call needs a calling agent, because the call executes as the service
//! intersected with that caller. Pages call through the bridge's `service`
//! verb, not this arm. A service's own runs never reach here: the dispatch
//! gate refuses every write to a service from one.

use std::time::Duration;

use super::super::{build_failure, mode_name, payload_str, ResourceMutationResult};
use super::append_payload;
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::Orchestrator;
use crate::project_services::protocol::{CallerFrame, CallerKind};
use crate::project_services::supervisor::{
    self, CallRequest, ServiceRef, ServiceState, ServiceStatus,
};
use crate::project_services::thread_store;
use cairn_common::uri::CairnResource;

/// The longest call value a change report carries.
const MAX_REPORTED_VALUE_BYTES: usize = 64 * 1024;

/// The keys a revision carries, as against `{action}`.
const REVISION_KEYS: &[&str] = &["title", "description", "files", "entry", "manifest"];

/// Who is acting: the operator (no run) or an agent, with the caller frame a
/// call from that agent carries and the agent's job.
struct Actor {
    who: String,
    caller: Option<CallerFrame>,
    job_id: Option<String>,
}

async fn actor(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    target: &ServiceRef,
) -> Result<Actor, String> {
    let Some(run_id) = request.run_id.as_deref().filter(|run| !run.is_empty()) else {
        return Ok(Actor {
            who: "operator".to_string(),
            caller: None,
            job_id: None,
        });
    };
    let agent = crate::mcp::handlers::run_context::authenticated_agent(&orch.db, request).await?;
    let (run, _) = crate::mcp::handlers::run_context::lookup_run_routed(&orch.db, request).await?;
    if run.project_key != target.project_key {
        return Err(format!(
            "a service is acted on from its own project; this run belongs to {}",
            run.project_key
        ));
    }
    let caller = CallerFrame {
        kind: CallerKind::Agent,
        acting_for: agent.home.clone(),
        agent: Some(cairn_common::identity::PrincipalRef::Agent {
            node: agent.home.clone(),
            run_id: Some(run_id.to_string()),
        }),
        viewer: None,
        instrument: None,
    };
    Ok(Actor {
        who: agent.home,
        caller: Some(caller),
        job_id: Some(agent.job_id),
    })
}

/// A thread's service is acted on by its thread or the operator.
async fn require_owner(
    orch: &Orchestrator,
    target: &ServiceRef,
    actor: &Actor,
) -> Result<(), String> {
    let (Some(thread_id), Some(job_id)) = (target.owner.thread_id(), actor.job_id.as_deref())
    else {
        return Ok(());
    };
    let db = crate::execution::routing::owning_db_for_project(&orch.db, &target.project_id)
        .await
        .map_err(|error| error.to_string())?;
    if thread_store::is_owners_job(&db, thread_id, job_id).await? {
        Ok(())
    } else {
        Err(format!(
            "{} is its thread's: only that thread (its sessions and their tasks) writes, starts, \
             stops, or restarts it",
            target.owner.home(&target.project_key, &target.service)
        ))
    }
}

fn report_value(value: &serde_json::Value, orch: &Orchestrator) -> String {
    // A service's answer is untrusted output that becomes model-visible here.
    let policy = crate::security::redaction_policy(&orch.config_dir);
    let mut value = value.clone();
    crate::security::Sanitizer::structural(&policy).json(&mut value);
    let mut text = serde_json::to_string_pretty(&value).unwrap_or_else(|_| "null".to_string());
    if text.len() > MAX_REPORTED_VALUE_BYTES {
        let mut end = MAX_REPORTED_VALUE_BYTES;
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        text.truncate(end);
        text.push_str("\n… (truncated)");
    }
    text
}

/// The service a resource names, resolved to its project and owner.
async fn resolve(
    orch: &Orchestrator,
    resource: &CairnResource,
) -> Option<Result<ServiceRef, String>> {
    let (project, thread, service) = match resource {
        CairnResource::ProjectService { project, service } => (project, None, service),
        CairnResource::ThreadService {
            project,
            name,
            service,
        } => (project, Some(name), service),
        _ => return None,
    };
    let project_key = cairn_common::uri::canonical_project(project);
    Some(match thread {
        None => crate::project_services::project_id_for_key(orch, project)
            .await
            .map(|id| ServiceRef::project(&id, &project_key, service)),
        Some(thread) => {
            let db = orch.db.for_project(project).await;
            thread_store::resolve_owner(&db, &project_key, thread)
                .await
                .map(|(project_id, owner)| ServiceRef {
                    project_id,
                    project_key,
                    owner,
                    service: service.clone(),
                })
        }
    })
}

/// What a deploy left, said for the author: is the new version serving, and
/// if not, why and what serves instead.
fn deploy_report(
    uri: &str,
    written: &thread_store::WrittenVersion,
    status: Option<&ServiceStatus>,
) -> String {
    let version = written.version;
    let state =
        match status {
            None => format!("Version {version} is stored and has not started."),
            Some(status) if status.sticky_stop => format!(
                "The service is stopped explicitly, so version {version} waits: patch \
             {{action:\"start\"}} to serve it."
            ),
            Some(status)
                if status.state == ServiceState::Running
                    && status.source_hash.as_deref() == Some(written.source_hash.as_str()) =>
            {
                format!(
                    "Version {version} is serving as generation {} on {}.",
                    status.generation.unwrap_or_default(),
                    status.executor.as_deref().unwrap_or("its executor")
                )
            }
            Some(status) if status.state == ServiceState::UpdateFailed => format!(
            "Version {version} did not start: {}. Generation {} keeps serving the version before \
             it (state update_failed).",
            status.last_error.as_deref().unwrap_or("no reason was recorded"),
            status.generation.unwrap_or_default()
        ),
            Some(status) => match &status.last_error {
                Some(error) if status.generation.is_none() => {
                    format!("Version {version} did not start: {error}. Nothing is serving.")
                }
                _ => format!(
                    "State {}{}.",
                    status.state.as_str(),
                    status
                        .generation
                        .map_or(String::new(), |g| format!(", generation {g}"))
                ),
            },
        };
    format!("{state} Boot errors, protocol violations, and stderr are at {uri}/log; status at {uri}/status.")
}

fn call_payload(
    index: usize,
    item: &ChangeItem,
) -> ResourceMutationResult<(String, serde_json::Value, Option<Duration>)> {
    let payload = append_payload(index, item)?;
    let method = payload_str(payload, "method", &[])
        .filter(|method| !method.trim().is_empty())
        .ok_or_else(|| build_failure(index, item, "payload.method is required"))?
        .to_string();
    let params = payload
        .get("params")
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    let timeout = match payload.get("timeoutSeconds") {
        None | Some(serde_json::Value::Null) => None,
        Some(value) => Some(
            value
                .as_u64()
                .filter(|seconds| *seconds > 0)
                .map(Duration::from_secs)
                .ok_or_else(|| {
                    build_failure(
                        index,
                        item,
                        "payload.timeoutSeconds must be a positive integer",
                    )
                })?,
        ),
    };
    Ok((method, params, timeout))
}

pub(super) async fn dispatch(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    index: usize,
    item: &ChangeItem,
    dry_run: bool,
    resource: &CairnResource,
) -> ResourceMutationResult<Option<String>> {
    let Some(target) = resolve(orch, resource).await else {
        return Ok(None);
    };
    let target = target.map_err(|error| build_failure(index, item, error))?;
    let service = target.service.clone();
    let uri = target.owner.home(&target.project_key, &service);
    let is_thread = target.owner.thread_id().is_some();
    let payload = item.payload.as_ref();
    let action = payload.and_then(|payload| payload_str(payload, "action", &[]));
    let revises = payload
        .and_then(serde_json::Value::as_object)
        .is_some_and(|object| REVISION_KEYS.iter().any(|key| object.contains_key(*key)));

    let summary = match item.mode {
        ChangeMode::Patch if action.is_some() || !is_thread => {
            if revises {
                return Err(build_failure(
                    index,
                    item,
                    "a patch either revises the service (files, title, description, entry, \
                     manifest) or acts on it ({action}), not both",
                ));
            }
            let action = action.ok_or_else(|| {
                build_failure(
                    index,
                    item,
                    "payload.action must be start, stop, or restart",
                )
            })?;
            if !matches!(action, "start" | "stop" | "restart") {
                return Err(build_failure(
                    index,
                    item,
                    format!("payload.action `{action}` must be start, stop, or restart"),
                ));
            }
            if dry_run {
                return Ok(Some(format!("Would {action} service {service}")));
            }
            let actor = actor(orch, request, &target)
                .await
                .map_err(|error| build_failure(index, item, error))?;
            require_owner(orch, &target, &actor)
                .await
                .map_err(|error| build_failure(index, item, error))?;
            let outcome = match action {
                "start" => supervisor::start(orch, &target, &actor.who).await,
                "stop" => supervisor::stop(orch, &target, &actor.who).await,
                _ => supervisor::restart(orch, &target, &actor.who).await,
            };
            outcome.map_err(|error| build_failure(index, item, error.to_string()))?;
            let state = target
                .status(orch)
                .map_or("stopped", |status| status.state.as_str());
            format!("Service {service}: {action} done; state {state}")
        }
        mode @ (ChangeMode::Create | ChangeMode::Patch) if is_thread => {
            let payload = payload.ok_or_else(|| {
                build_failure(
                    index,
                    item,
                    format!("mode={} requires a service payload", mode_name(mode)),
                )
            })?;
            // Written by its owning thread, so the writer is always a run Cairn
            // can resolve; a run-less write cannot be told apart from an agent
            // that dropped its own.
            if request.run_id.as_deref().is_none_or(str::is_empty) {
                return Err(build_failure(
                    index,
                    item,
                    "A thread's services are written by that thread: this request carries no run \
                     identity, and a run-less write cannot be told apart from an agent that \
                     dropped its own.",
                ));
            }
            let actor = actor(orch, request, &target)
                .await
                .map_err(|error| build_failure(index, item, error))?;
            let writer = thread_store::ServiceWriter {
                job_id: actor.job_id.clone().unwrap_or_default(),
            };
            let db = crate::execution::routing::owning_db_for_project(&orch.db, &target.project_id)
                .await
                .map_err(|error| build_failure(index, item, error.to_string()))?;
            let written = thread_store::write(
                &db,
                &target.project_id,
                &target.owner,
                &service,
                &uri,
                &writer,
                payload,
                matches!(mode, ChangeMode::Patch),
                dry_run,
            )
            .await
            .map_err(|error| build_failure(index, item, error))?;
            if dry_run {
                return Ok(Some(format!(
                    "Would write version {} of {uri} and deploy it",
                    written.version
                )));
            }
            let status = supervisor::deploy(orch, &target, &actor.who).await;
            format!(
                "Service written ({uri}, version {}). {}",
                written.version,
                deploy_report(&uri, &written, status.as_ref())
            )
        }
        ChangeMode::Append => {
            let (method, params, timeout) = call_payload(index, item)?;
            if dry_run {
                return Ok(Some(format!("Would call {service}.{method}")));
            }
            let actor = actor(orch, request, &target)
                .await
                .map_err(|error| build_failure(index, item, error))?;
            let caller = actor.caller.ok_or_else(|| {
                build_failure(
                    index,
                    item,
                    "a service call needs a calling agent run: the call executes as the service \
                     intersected with its caller",
                )
            })?;
            let started = std::time::Instant::now();
            let value = supervisor::call(
                orch,
                CallRequest {
                    project_id: target.project_id.clone(),
                    project_key: target.project_key.clone(),
                    owner: target.owner.clone(),
                    service: service.clone(),
                    method: method.clone(),
                    params,
                    caller,
                    timeout,
                },
            )
            .await
            .map_err(|error| build_failure(index, item, format!("{service}.{method}: {error}")))?;
            format!(
                "Called {service}.{method} ({} ms). Value:\n{}",
                started.elapsed().as_millis(),
                report_value(&value, orch)
            )
        }
        _ => return Ok(None),
    };
    Ok(Some(summary))
}
