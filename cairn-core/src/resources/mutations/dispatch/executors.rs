//! Fleet management mutation dispatch: enroll, configure, remove.
//!
//! Parsing and name resolution only. Every side effect — SSH provisioning, the
//! enrollment claim, settings, supervision, the live fenced controls — belongs to
//! `crate::fleet::management`, which is the same code the authenticated invoke
//! surface calls. This module exists so an agent writing `cairn://executors` and
//! an operator clicking in Settings reach one implementation, not two that drift.

use super::super::{build_failure, ResourceMutationResult};
use crate::fleet::management;
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::Orchestrator;
use cairn_common::uri::CairnResource;

/// The controls a patch may carry. Deliberately narrow: a machine's host,
/// identity, paths, tunnel, and project membership have no safe reconnect
/// lifecycle behind them, so they are not writable here at all rather than
/// writable and quietly ineffective.
#[derive(serde::Deserialize, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ConfigureRequest {
    #[serde(alias = "new_name")]
    new_name: Option<String>,
    #[serde(alias = "runtime_policy")]
    runtime_policy: Option<cairn_common::executor_protocol::ExecutorRuntimePolicy>,
    draining: Option<bool>,
    #[serde(alias = "expected_generation")]
    expected_generation: Option<u64>,
}

pub(super) async fn dispatch(
    orch: &Orchestrator,
    _request: &McpCallbackRequest,
    index: usize,
    item: &ChangeItem,
    dry_run: bool,
    resource: &CairnResource,
) -> ResourceMutationResult<Option<String>> {
    if !matches!(
        resource,
        CairnResource::Executors | CairnResource::Executor { .. }
    ) {
        return Ok(None);
    }
    // No separate authorization gate here, deliberately. This write arrives over
    // the runner's MCP callback, authenticated by its own local secret — and a
    // caller holding that secret can already run arbitrary commands on this host
    // through `run`, which is strictly more than enrolling an SSH executor. The
    // operator check that does mean something lives on `/api/invoke`, where a
    // request carries a person.

    let summary = match (resource, item.mode) {
        (CairnResource::Executors, ChangeMode::Create) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| build_failure(index, item, "mode=create requires payload"))?;
            let request: management::EnrollmentRequest = serde_json::from_value(payload.clone())
                .map_err(|error| {
                    build_failure(index, item, format!("invalid enrollment request: {error}"))
                })?;
            if request.host.trim().is_empty() || request.ssh_user.trim().is_empty() {
                return Err(build_failure(
                    index,
                    item,
                    "enrollment needs a host and an sshUser; everything else is derived",
                ));
            }
            if dry_run {
                let declaration = management::build_declaration(orch, request)
                    .await
                    .map_err(|error| build_failure(index, item, error))?;
                format!(
                    "Would enroll {}@{} as cairn://executors/{} on tunnel port {}",
                    declaration.ssh_user,
                    declaration.host,
                    declaration.display_name,
                    declaration.tunnel_port
                )
            } else {
                let started = management::enroll(orch, request)
                    .await
                    .map_err(|error| build_failure(index, item, error))?;
                format!(
                    "Enrolling {}: operation {} started. Read {} for its phase; it is not targetable until it reports ready.",
                    started.name, started.operation_id, started.uri
                )
            }
        }
        (CairnResource::Executor { name }, ChangeMode::Patch) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| build_failure(index, item, "mode=patch requires payload"))?;
            let request: ConfigureRequest =
                serde_json::from_value(payload.clone()).map_err(|error| {
                    build_failure(
                        index,
                        item,
                        format!("invalid executor configuration: {error}. A patch accepts newName, runtimePolicy, draining, and expectedGeneration."),
                    )
                })?;
            if request.new_name.is_none()
                && request.runtime_policy.is_none()
                && request.draining.is_none()
            {
                return Err(build_failure(
                    index,
                    item,
                    "a patch changes newName, runtimePolicy, or draining; none was supplied",
                ));
            }
            // Resolve first, so an unknown machine is refused identically whether
            // or not the write would have had an effect.
            management::resolve_executor_reference(orch, name)
                .map_err(|error| build_failure(index, item, error))?;
            let fenced = request.runtime_policy.is_some() || request.draining.is_some();
            if fenced && request.expected_generation.is_none() {
                return Err(build_failure(
                    index,
                    item,
                    "runtimePolicy and draining are live controls: send the expectedGeneration you read from cairn://executors/<name>, so an edit cannot land on a connection that has since been replaced",
                ));
            }
            if dry_run {
                let mut planned = Vec::new();
                if let Some(new_name) = &request.new_name {
                    planned.push(format!("rename {name} to {new_name}"));
                }
                if request.runtime_policy.is_some() {
                    planned.push("apply a runtime policy".to_string());
                }
                if let Some(draining) = request.draining {
                    planned.push(if draining {
                        "enable draining".to_string()
                    } else {
                        "stop draining".to_string()
                    });
                }
                format!("Would {}", planned.join(", "))
            } else {
                let mut applied = Vec::new();
                if let Some(policy) = request.runtime_policy {
                    management::set_runtime_policy(
                        orch,
                        name,
                        request.expected_generation.unwrap_or_default(),
                        policy,
                    )
                    .await
                    .map_err(|error| build_failure(index, item, error))?;
                    applied.push("runtime policy applied".to_string());
                }
                if let Some(draining) = request.draining {
                    management::set_drain_mode(
                        orch,
                        name,
                        request.expected_generation.unwrap_or_default(),
                        draining,
                    )
                    .await
                    .map_err(|error| build_failure(index, item, error))?;
                    applied.push(if draining {
                        "draining: new work is refused, resident work is left alone".to_string()
                    } else {
                        "draining stopped; new work is admitted again".to_string()
                    });
                }
                // Renaming last: it moves the address the other controls were
                // just addressed by, so doing it first would strand them.
                if let Some(new_name) = &request.new_name {
                    let result = management::rename(orch, name, new_name)
                        .await
                        .map_err(|error| build_failure(index, item, error))?;
                    applied.push(format!(
                        "renamed to {} — placement requests must use the new name; cairn://executors/{} is the address now",
                        result.config.display_name, new_name
                    ));
                }
                format!("{name}: {}", applied.join("; "))
            }
        }
        (CairnResource::Executor { name }, ChangeMode::Delete) => {
            let executor_id = management::resolve_executor_reference(orch, name)
                .map_err(|error| build_failure(index, item, error))?;
            if dry_run {
                let occupancy = management::occupancy(orch, &executor_id);
                if occupancy.is_empty() {
                    format!("Would remove {name} and revoke its enrollment")
                } else {
                    format!(
                        "Would refuse to remove {name}: it still has {}",
                        occupancy.summary()
                    )
                }
            } else {
                management::remove(orch, name)
                    .await
                    .map_err(|error| build_failure(index, item, error))?;
                format!(
                    "Removed {name}: supervision stopped, remote cleaned up, enrollment revoked, configuration removed"
                )
            }
        }
        (CairnResource::Executors, _) | (CairnResource::Executor { .. }, _) => return Ok(None),
        _ => return Ok(None),
    };
    Ok(Some(summary))
}
