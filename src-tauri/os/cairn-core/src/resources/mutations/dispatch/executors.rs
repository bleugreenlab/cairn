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
    /// What the machine is for: buildsOnly, general, or inferenceOnly. Runner
    /// state like draining, so it is durable and unfenced.
    role: Option<cairn_common::executor_protocol::MachineRole>,
    #[serde(alias = "desktop_automation")]
    desktop_automation: Option<bool>,
    /// Declare this machine repaired, re-arming every check its old environment
    /// had infrastructure-suppressed. For a repair the executor's own attach
    /// probes can see, restarting it does the same thing; this is for the ones
    /// they cannot — a cleared cache, a replaced helper, a fixed permission.
    repaired: Option<bool>,
    #[serde(alias = "expected_generation")]
    expected_generation: Option<u64>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ListenerRotationRequest {
    #[serde(default)]
    regenerate_executor_listener_certificate: bool,
    #[serde(default)]
    force_listener_rotation_cutover: bool,
    #[serde(default)]
    confirm: bool,
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
                    "Would enroll {}@{} as cairn://executors/{}, which then dials this runner on its own",
                    declaration.ssh_user, declaration.host, declaration.display_name,
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
        (CairnResource::Executors, ChangeMode::Patch) => {
            let request: ListenerRotationRequest = serde_json::from_value(
                item.payload
                    .clone()
                    .ok_or_else(|| build_failure(index, item, "mode=patch requires payload"))?,
            )
            .map_err(|error| {
                build_failure(
                    index,
                    item,
                    format!("invalid listener rotation request: {error}"),
                )
            })?;
            if request.regenerate_executor_listener_certificate
                == request.force_listener_rotation_cutover
            {
                return Err(build_failure(index, item, "set exactly one of regenerateExecutorListenerCertificate or forceListenerRotationCutover"));
            }
            if request.regenerate_executor_listener_certificate {
                if dry_run {
                    "Would stage a new executor listener certificate and wait for every enrolled machine to acknowledge it".to_string()
                } else {
                    let status = orch
                        .fleet
                        .regenerate_listener_certificate()
                        .map_err(|error| build_failure(index, item, error))?;
                    format!(
                        "Staged executor listener certificate {}; waiting for {}",
                        status.staged_fingerprint,
                        if status.pending_executor_ids.is_empty() {
                            "no machines".to_string()
                        } else {
                            status.pending_executor_ids.join(", ")
                        }
                    )
                }
            } else {
                let pending = orch
                    .fleet
                    .listener_rotation_status()
                    .map_err(|error| build_failure(index, item, error))?
                    .ok_or_else(|| {
                        build_failure(index, item, "no executor listener rotation is pending")
                    })?
                    .pending_executor_ids;
                if !pending.is_empty() && !request.confirm {
                    return Err(build_failure(index, item, format!("force cutover would strand these machines until they are re-bootstrapped: {}. Retry with confirm:true", pending.join(", "))));
                }
                if dry_run {
                    format!("Would force listener rotation cutover; machines requiring re-bootstrap: {}", pending.join(", "))
                } else {
                    let casualties = orch
                        .fleet
                        .force_listener_rotation_cutover()
                        .map_err(|error| build_failure(index, item, error))?;
                    format!(
                        "Forced listener rotation cutover; machines requiring re-bootstrap: {}",
                        if casualties.is_empty() {
                            "none".to_string()
                        } else {
                            casualties.join(", ")
                        }
                    )
                }
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
                        format!("invalid executor configuration: {error}. A patch accepts newName, runtimePolicy, draining, role (buildsOnly, general, or inferenceOnly), desktopAutomation, repaired, and expectedGeneration."),
                    )
                })?;
            if request.new_name.is_none()
                && request.runtime_policy.is_none()
                && request.draining.is_none()
                && request.role.is_none()
                && request.desktop_automation.is_none()
                && request.repaired.is_none()
            {
                return Err(build_failure(
                    index,
                    item,
                    "a patch changes newName, runtimePolicy, draining, role, desktopAutomation, or repaired; none was supplied",
                ));
            }
            // Resolve first, so an unknown machine is refused identically whether
            // or not the write would have had an effect.
            let executor_id = management::resolve_executor_reference(orch, name)
                .map_err(|error| build_failure(index, item, error))?;
            // Only the runtime policy is fenced. It is configuration applied to
            // a running process, so it must not land on a replacement of the
            // one the caller read. Draining is the runner's own refusal to
            // place work and is deliberately not fenced: requiring a live
            // generation for it made the control unusable at the only moment it
            // matters, when the machine has stopped answering (CAIRN-4360).
            if request.runtime_policy.is_some() && request.expected_generation.is_none() {
                return Err(build_failure(
                    index,
                    item,
                    "runtimePolicy is a live control: send the expectedGeneration you read from cairn://executors/<name>, so an edit cannot land on a connection that has since been replaced",
                ));
            }
            if dry_run {
                let mut planned = Vec::new();
                if let Some(new_name) = &request.new_name {
                    planned.push(format!("rename {name} to {new_name}"));
                }
                if let Some(enabled) = request.desktop_automation {
                    planned.push(format!(
                        "turn desktop automation {}",
                        if enabled { "on" } else { "off" }
                    ));
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
                if let Some(role) = request.role {
                    planned.push(format!(
                        "make {name} {}, which {}",
                        role.label(),
                        role.describe_accepted()
                    ));
                }
                if request.repaired == Some(true) {
                    planned.push(format!(
                        "declare {name} repaired, re-arming every check suppressed on its environment"
                    ));
                }
                format!("Would {}", planned.join(", "))
            } else {
                let mut applied = Vec::new();
                if let Some(enabled) = request.desktop_automation {
                    let mut fleet = crate::config::settings::load_fleet(orch.mcp_auth.config_dir());
                    let public_name =
                        cairn_common::executor_protocol::normalize_executor_name(name).ok_or_else(
                            || build_failure(index, item, "executor name has no public form"),
                        )?;
                    fleet
                        .desktop_automation
                        .machines
                        .entry(public_name)
                        .or_default()
                        .enabled = Some(enabled);
                    crate::config::settings::set_fleet(orch.mcp_auth.config_dir(), &fleet)
                        .map_err(|error| build_failure(index, item, error))?;
                    if enabled {
                        let orch = orch.clone();
                        tokio::spawn(async move {
                            crate::fleet::desktop::probe_attached_desktops(&orch).await;
                        });
                    } else if let Some(gateway) = orch.mcp_gateway() {
                        gateway.close_placed(name).await;
                    }
                    applied.push(format!(
                        "desktop automation {}",
                        if enabled { "enabled" } else { "disabled" }
                    ));
                }
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
                    let outcome = management::set_drain_mode(orch, name, draining)
                        .await
                        .map_err(|error| build_failure(index, item, error))?;
                    applied.push(outcome.summary());
                }
                if let Some(role) = request.role {
                    let outcome = management::set_machine_role(orch, name, role)
                        .map_err(|error| build_failure(index, item, error))?;
                    applied.push(outcome.summary());
                }
                if request.repaired == Some(true) {
                    let epoch = crate::fleet::repair::mark_repaired(&orch.db.local, &executor_id)
                        .await
                        .map_err(|error| build_failure(index, item, error))?;
                    orch.fleet
                        .clear_preparation_refusals(&executor_id)
                        .await
                        .map_err(|error| build_failure(index, item, error))?;
                    applied.push(format!(
                        "declared repaired (repair {epoch}) — every check infrastructure-suppressed on this machine's environment runs again, and it is eligible again for projects it had failed to prepare; a MISSING build-prerequisite verdict still stands until the machine re-attaches"
                    ));
                }
                // Renaming last: it moves the address the other controls were
                // just addressed by, so doing it first would strand them.
                if let Some(new_name) = &request.new_name {
                    let result = management::rename(orch, name, new_name)
                        .await
                        .map_err(|error| build_failure(index, item, error))?;
                    applied.push(format!(
                        "renamed to {} — placement requests must use the new name; cairn://executors/{} is the address now",
                        result.display_name, new_name
                    ));
                }
                format!("{name}: {}", applied.join("; "))
            }
        }
        (CairnResource::Executor { name }, ChangeMode::Delete) => {
            let executor_id = management::resolve_executor_reference(orch, name)
                .map_err(|error| build_failure(index, item, error))?;
            let acknowledged = item
                .payload
                .as_ref()
                .and_then(|payload| payload.get("confirm"))
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false);
            if dry_run {
                let reading = management::occupancy(orch, &executor_id);
                if reading.is_empty() || acknowledged {
                    format!("Would remove {name} and revoke its enrollment")
                } else {
                    format!(
                        "Would refuse to remove {name}: it still has {}{}",
                        reading.summary(),
                        if reading.is_current() {
                            ""
                        } else {
                            " as of the last time it reported"
                        }
                    )
                }
            } else {
                let result = management::remove(orch, name, acknowledged)
                    .await
                    .map_err(|error| build_failure(index, item, error))?;
                if result.config.is_none() {
                    format!(
                        "Removed {name}: cleared the failed enrollment record. Its rollback had already completed, so there was no configuration, supervision, credential, or remote process left to remove."
                    )
                } else {
                    match result.unverified_remote_cleanup {
                        None => format!(
                            "Removed {name}: supervision stopped, remote cleaned up, enrollment revoked, configuration removed"
                        ),
                        // Removal completed without the machine, so the summary says
                        // which half happened rather than claiming a teardown that
                        // never reached the host.
                        Some(reason) => format!(
                            "Removed {name}: supervision stopped, enrollment revoked, configuration removed. Its host could not be reached ({reason}), so no remote cleanup ran there; the revoked enrollment means anything still running on it cannot reattach."
                        ),
                    }
                }
            }
        }
        (CairnResource::Executors, _) | (CairnResource::Executor { .. }, _) => return Ok(None),
        _ => return Ok(None),
    };
    Ok(Some(summary))
}
