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
use cairn_uri::CairnResource;

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
    reason: Option<String>,
    /// What the machine is for, one of [`MachineRole::ALL`]. Runner state like
    /// draining, so it is durable and unfenced.
    ///
    /// [`MachineRole::ALL`]: cairn_common::executor_protocol::MachineRole::ALL
    role: Option<cairn_common::executor_protocol::MachineRole>,
    /// Pack-declared machine services to select (`{}`, or
    /// `{override:{command}}`) or unselect (`null`) on this machine, keyed
    /// `<pack>/<service>`. Stored fleet configuration, not an authorization gate.
    #[serde(alias = "machine_services")]
    machine_services: Option<management::MachineServiceChanges>,
    /// Declare this machine repaired, re-arming every check its old environment
    /// had infrastructure-suppressed. For a repair the executor's own attach
    /// probes can see, restarting it does the same thing; this is for the ones
    /// they cannot — a cleared cache, a replaced helper, a fixed permission.
    repaired: Option<bool>,
    #[serde(alias = "expected_generation")]
    expected_generation: Option<u64>,
    /// Where the runner's own executor keeps its cells: a directory on another
    /// volume, or `null` for the home. Absent leaves it as it is.
    #[serde(alias = "build_root", default, deserialize_with = "present_or_null")]
    build_root: Option<Option<String>>,
    #[serde(default, deserialize_with = "present_diagnostic")]
    memory_diagnostic: Option<Option<management::MemoryDiagnosticSelection>>,
}

impl ConfigureRequest {
    fn diagnostic_generation(&self) -> Result<u64, &'static str> {
        if self.new_name.is_some()
            || self.runtime_policy.is_some()
            || self.draining.is_some()
            || self.reason.is_some()
            || self.role.is_some()
            || self.machine_services.is_some()
            || self.repaired.is_some()
            || self.build_root.is_some()
        {
            return Err("memoryDiagnostic cannot be combined with configuration changes");
        }
        self.expected_generation
            .ok_or("memoryDiagnostic requires expectedGeneration from cairn://executors/local")
    }
}

/// Tell an absent key (`None`) from an explicit `null` (`Some(None)`).
fn present_or_null<'de, D>(deserializer: D) -> Result<Option<Option<String>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    <Option<String> as serde::Deserialize>::deserialize(deserializer).map(Some)
}

fn present_diagnostic<'de, D>(
    deserializer: D,
) -> Result<Option<Option<management::MemoryDiagnosticSelection>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    <Option<management::MemoryDiagnosticSelection> as serde::Deserialize>::deserialize(deserializer)
        .map(Some)
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
    callback: &McpCallbackRequest,
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
                        format!(
                            "invalid executor configuration: {error}. A patch accepts newName, runtimePolicy, draining, reason, role ({}), machineServices ({{\"<pack>/<service>\": {{}} | {{override:{{command}}}} | null}}), repaired, buildRoot, memoryDiagnostic ({{runId,durationMs}} or null), and expectedGeneration.",
                            cairn_common::executor_protocol::MachineRole::ALL
                                .map(cairn_common::executor_protocol::MachineRole::as_str)
                                .join(", ")
                        ),
                    )
                })?;
            if request.reason.is_some() && request.draining.is_none() {
                return Err(build_failure(
                    index,
                    item,
                    "reason requires a draining change",
                ));
            }
            // Authenticate before any other field of a combined patch can act.
            if request.draining.is_some() {
                management::thread_drain_actor(orch, callback)
                    .await
                    .map_err(|error| build_failure(index, item, error))?;
            }
            if let Some(selection) = &request.memory_diagnostic {
                let generation = request
                    .diagnostic_generation()
                    .map_err(|error| build_failure(index, item, error))?;
                management::resolve_memory_diagnostic(orch, name, generation, selection.as_ref())
                    .map_err(|error| build_failure(index, item, error))?;
                if !dry_run {
                    management::set_memory_diagnostic(orch, name, generation, selection.as_ref())
                        .await
                        .map_err(|error| build_failure(index, item, error))?;
                }
                return Ok(Some(format!(
                    "{} memory diagnostic {}",
                    if dry_run { "Would change" } else { "Changed" },
                    if selection.is_some() {
                        "(attached for at most 60 seconds; cached evidence only)"
                    } else {
                        "(detached)"
                    },
                )));
            }
            if request.new_name.is_none()
                && request.runtime_policy.is_none()
                && request.draining.is_none()
                && request.role.is_none()
                && request.machine_services.is_none()
                && request.repaired.is_none()
                && request.build_root.is_none()
            {
                return Err(build_failure(
                    index,
                    item,
                    "a patch changes newName, runtimePolicy, draining, role, machineServices, repaired, or buildRoot; none was supplied",
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
                for (key, selection) in request.machine_services.iter().flatten() {
                    planned.push(
                        match selection.as_ref().map(|chosen| chosen.override_command()) {
                            Some(Some(command)) => {
                                format!("select {key} on {name} running {command}")
                            }
                            Some(None) => format!("select {key} on {name}"),
                            None => format!("unselect {key} on {name}"),
                        },
                    );
                }
                if request.runtime_policy.is_some() {
                    planned.push("apply a runtime policy".to_string());
                }
                if let Some(draining) = request.draining {
                    planned.push(if draining {
                        "enable draining".to_string()
                    } else {
                        "clear only this thread's drain and imported legacy; other owners remain"
                            .to_string()
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
                if let Some(build_root) = &request.build_root {
                    planned.push(match build_root {
                        Some(root) => {
                            format!("move {name}'s build root to {root} from the next runner start")
                        }
                        None => format!(
                            "move {name}'s build root back to its home from the next runner start"
                        ),
                    });
                }
                format!("Would {}", planned.join(", "))
            } else {
                let mut applied = Vec::new();
                if let Some(changes) = &request.machine_services {
                    let summaries = management::set_machine_services(orch, name, changes)
                        .map_err(|error| build_failure(index, item, error))?;
                    applied.extend(summaries);
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
                    let outcome = management::set_thread_drain(
                        orch,
                        name,
                        draining,
                        request.reason.as_deref(),
                        callback,
                    )
                    .await
                    .map_err(|error| build_failure(index, item, error))?;
                    applied.push(outcome.summary());
                }
                if let Some(role) = request.role {
                    let outcome = management::set_machine_role(orch, name, role)
                        .map_err(|error| build_failure(index, item, error))?;
                    applied.push(outcome.summary());
                }
                if let Some(build_root) = &request.build_root {
                    let outcome =
                        management::set_colocated_build_root(orch, name, build_root.as_deref())
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
            let thread_actor = management::thread_drain_actor(orch, callback)
                .await
                .map_err(|error| build_failure(index, item, error))?;
            let actor = management::RemovalActor::from(&thread_actor);
            let acknowledged = item
                .payload
                .as_ref()
                .and_then(|payload| payload.get("confirm"))
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false);
            if dry_run {
                let executor_id = management::resolve_executor_reference(orch, name)
                    .map_err(|error| build_failure(index, item, error))?;
                preview_removal_drains(&orch.config_dir, &executor_id)
                    .map_err(|error| build_failure(index, item, error))?;
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
                let result = management::remove(
                    orch,
                    name,
                    acknowledged,
                    &actor,
                    item.payload
                        .as_ref()
                        .and_then(|value| value.get("operationId"))
                        .and_then(serde_json::Value::as_str),
                )
                .await
                .map_err(|error| build_failure(index, item, error))?;
                if result.replayed {
                    format!(
                        "Removal reconciled from durable receipt; no effect repeated for {name}"
                    )
                } else if result.result.config.is_none() {
                    format!(
                        "Removed {name}: cleared the failed enrollment record. Its rollback had already completed, so there was no configuration, supervision, credential, or remote process left to remove."
                    )
                } else {
                    // Removal is a registry change that never reaches the host,
                    // so the summary says what the host still holds rather than
                    // claiming a teardown.
                    match result.result.left_on_host {
                        None => {
                            format!("Removed {name}: enrollment revoked, configuration removed")
                        }
                        Some(left) => format!(
                            "Removed {name}: enrollment revoked, configuration removed. {}",
                            left.summary
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

// Preview must not persist a legacy import or publish a live projection. The
// actual removal rechecks the records under the serialized mutation lock.
fn preview_removal_drains(config_dir: &std::path::Path, executor_id: &str) -> Result<(), String> {
    let config = crate::config::settings::read_fleet_strict(config_dir)?;
    if !config.drains.records(executor_id).is_empty() {
        return Err(
            "thread removal cannot clear any standing drain; confirm acknowledges occupancy only"
                .into(),
        );
    }
    Ok(())
}

#[cfg(test)]
mod removal_preview_tests {
    use super::preview_removal_drains;

    #[test]
    fn removal_preview_never_creates_or_rewrites_settings() {
        let dir = tempfile::tempdir().unwrap();
        let path = crate::config::settings::get_settings_path(dir.path());
        preview_removal_drains(dir.path(), "remote").unwrap();
        assert!(!path.exists(), "preview must not initialize settings");

        let legacy =
            "# preserve formatting and migration state\nbuildSlots:\n  drainedMachines: [remote]\n";
        std::fs::write(&path, legacy).unwrap();
        assert!(preview_removal_drains(dir.path(), "remote")
            .unwrap_err()
            .contains("cannot clear any standing drain"));
        preview_removal_drains(dir.path(), "other").unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), legacy);
    }

    #[test]
    fn removal_preview_fails_closed_without_rewriting_an_undecodable_fleet() {
        let dir = tempfile::tempdir().unwrap();
        let path = crate::config::settings::get_settings_path(dir.path());
        let yaml = "buildSlots:\n  drains:\n    version: 99\n";
        std::fs::write(&path, yaml).unwrap();
        assert!(preview_removal_drains(dir.path(), "remote").is_err());
        assert_eq!(std::fs::read_to_string(&path).unwrap(), yaml);
    }
}

#[cfg(test)]
mod memory_diagnostic_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn memory_diagnostic_patch_requires_generation_and_cannot_mix_configuration() {
        let valid: ConfigureRequest = serde_json::from_value(json!({
            "memoryDiagnostic":null, "expectedGeneration":7
        }))
        .unwrap();
        assert_eq!(valid.diagnostic_generation().unwrap(), 7);
        let missing: ConfigureRequest =
            serde_json::from_value(json!({"memoryDiagnostic":null})).unwrap();
        assert!(missing
            .diagnostic_generation()
            .unwrap_err()
            .contains("expectedGeneration"));
        for change in [
            json!({"draining":false}),
            json!({"newName":"replacement"}),
            json!({"runtimePolicy": cairn_common::executor_protocol::ExecutorRuntimePolicy::default()}),
            json!({"role":"general"}),
            json!({"machineServices":{}}),
            json!({"repaired":true}),
            json!({"buildRoot":null}),
        ] {
            let mut payload = json!({"memoryDiagnostic":null,"expectedGeneration":7});
            payload
                .as_object_mut()
                .unwrap()
                .extend(change.as_object().unwrap().clone());
            let request: ConfigureRequest = serde_json::from_value(payload).unwrap();
            assert!(request
                .diagnostic_generation()
                .unwrap_err()
                .contains("cannot be combined"));
        }
    }

    #[test]
    fn memory_diagnostic_patch_distinguishes_absence_detach_and_attach() {
        let absent: ConfigureRequest = serde_json::from_value(json!({})).unwrap();
        assert!(absent.memory_diagnostic.is_none());
        let detach: ConfigureRequest = serde_json::from_value(json!({
            "memoryDiagnostic": null, "expectedGeneration": 7
        }))
        .unwrap();
        assert!(matches!(detach.memory_diagnostic, Some(None)));
        let attach: ConfigureRequest = serde_json::from_value(json!({
            "memoryDiagnostic": {"runId":"registered", "durationMs":60000},
            "expectedGeneration": 7
        }))
        .unwrap();
        let selection = attach.memory_diagnostic.unwrap().unwrap();
        assert_eq!(selection.run_id, "registered");
        assert_eq!(selection.duration_ms, 60_000);
        assert!(
            serde_json::from_value::<ConfigureRequest>(json!({
                "memoryDiagnostic": {"runId":"registered", "durationMs":60000,
                    "anchor":{"pid":42,"startToken":"1"}}
            }))
            .is_err(),
            "resource callers cannot supply a PID or start identity"
        );
        assert!(serde_json::from_value::<ConfigureRequest>(json!({
            "memoryDiagnostic": {"pid":42,"durationMs":60000}
        }))
        .is_err());
    }
}
