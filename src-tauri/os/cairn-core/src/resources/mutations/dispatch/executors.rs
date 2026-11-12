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
    /// `<pack>/<service>`. An authorization gate: selecting runs a standing
    /// process outside the caller's fence that outlives the run, so a change
    /// that widens what runs is the operator's to approve, one exact act at a
    /// time ([`machine_services_authority`]); unselecting and clearing an
    /// override are not.
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
    /// Whether the patch changes anything besides its machine services. A
    /// widening selection must come alone, so the card that approves it shows
    /// one act and nothing else rides on its approval.
    fn changes_more_than_machine_services(&self) -> bool {
        self.new_name.is_some()
            || self.runtime_policy.is_some()
            || self.draining.is_some()
            || self.reason.is_some()
            || self.role.is_some()
            || self.repaired.is_some()
            || self.build_root.is_some()
            || self.memory_diagnostic.is_some()
    }

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

/// Refused alongside other fields: see
/// [`ConfigureRequest::changes_more_than_machine_services`].
const SELECT_ALONE: &str = "Refused: a machineServices change that selects a service needs the operator's approval of exactly that act, so send machineServices on its own patch and the other fields in another. Nothing changed.";

/// The authority a `machineServices` patch crosses, for
/// `authorization::prepare`: `None` unless the patch selects something that
/// was not running. The dispatcher derives the same request from the same plan
/// and admits it immediately before the write.
///
/// A patch that cannot be planned names no authority here; the dispatcher
/// refuses it with the planner's reason, so no card is raised for a change
/// that could not be made.
pub(crate) async fn machine_services_authority(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    item: &ChangeItem,
) -> Option<crate::authorization::prepare::Prepared> {
    use crate::authorization::prepare::Prepared;
    if item.mode != ChangeMode::Patch {
        return None;
    }
    let CairnResource::Executor { name } =
        super::super::target_resource_for_request(orch, request, item)
            .await
            .ok()?
    else {
        return None;
    };
    let configure: ConfigureRequest = serde_json::from_value(item.payload.clone()?).ok()?;
    let changes = configure.machine_services.as_ref()?;
    let plan = management::plan_machine_services(orch, &name, changes).ok()?;
    if !plan.widens() {
        return None;
    }
    if configure.changes_more_than_machine_services() {
        return Some(Prepared::Refused(SELECT_ALONE.to_string()));
    }
    Some(Prepared::Authority(management::machine_service_request(
        orch, &plan,
    )))
}

/// Who is making a `machineServices` change through the resource door, as its
/// plan requires: the approved agent for a widening plan, admitted here
/// against the request the card was raised for; anyone for reductions.
async fn machine_service_authority(
    orch: &Orchestrator,
    callback: &McpCallbackRequest,
    plan: &management::MachineServicePlan,
) -> Result<management::MachineServiceAuthority, String> {
    use cairn_common::authorization::AuthorityDecision;
    let actor = crate::authorization::resolve_actor(orch, callback).await;
    let asker = actor.as_ref().and_then(|actor| {
        actor
            .principal
            .node_uri
            .clone()
            .or_else(|| actor.principal.run_id.clone())
    });
    if !plan.widens() {
        return Ok(management::MachineServiceAuthority::Reduction(
            match asker {
                Some(node_uri) => management::ReductionActor::Agent { node_uri },
                None => management::ReductionActor::Unproven {
                    label: "a caller with no run".to_string(),
                },
            },
        ));
    }
    let request = management::machine_service_request(orch, plan);
    let Some(actor) = actor else {
        return Err(format!(
            "Denied: selecting a machine service is the operator's to approve, and this request comes from no run that could ask. The operator selects it from Settings, or an agent asks with this same patch, which raises their card. Asked: {}",
            request.summary
        ));
    };
    let (decision, _event) = crate::authorization::authorize_recorded(&actor, &request).await?;
    match decision {
        AuthorityDecision::AllowedByGrant { grant_id, .. } => {
            Ok(management::MachineServiceAuthority::Approved {
                node_uri: asker.unwrap_or_else(|| "an unnamed run".to_string()),
                approval: grant_id,
            })
        }
        // Policy classifies every machine-service selection as a boundary,
        // so direct here would be a classification nobody made.
        AuthorityDecision::Direct => Err(
            "Refused: a machine-service selection was classified as direct work, which it never is; nothing changed"
                .to_string(),
        ),
        AuthorityDecision::ApprovalRequired(reason) | AuthorityDecision::Forbidden(reason) => {
            Err(crate::authorization::refusal_message(&request, reason))
        }
    }
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
    // Holding the runner's MCP callback secret is not authority. A run's
    // `run` is fenced, and so is everything it starts, so an act that starts
    // something outside the fence is gated here: selecting a machine service
    // (`machine_service_authority`) is admitted only on the operator's
    // approval of exactly that act. The person-carrying check for the other
    // controls lives on `/api/invoke`.

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
            let machine_plan = match &request.machine_services {
                Some(changes) => {
                    let plan = management::plan_machine_services(orch, name, changes)
                        .map_err(|error| build_failure(index, item, error))?;
                    if plan.widens() && request.changes_more_than_machine_services() {
                        return Err(build_failure(index, item, SELECT_ALONE));
                    }
                    Some(plan)
                }
                None => None,
            };
            if dry_run {
                let mut planned = Vec::new();
                if let Some(new_name) = &request.new_name {
                    planned.push(format!("rename {name} to {new_name}"));
                }
                if let Some(plan) = &machine_plan {
                    planned.extend(plan.sentences());
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
                        "declare {name} repaired, re-arming every check suppressed on its environment and forgetting the out-of-memory kills held against it"
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
                if let (Some(changes), Some(plan)) = (&request.machine_services, &machine_plan) {
                    let authority = machine_service_authority(orch, callback, plan)
                        .await
                        .map_err(|error| build_failure(index, item, error))?;
                    let summaries =
                        management::set_machine_services(orch, name, changes, &authority, plan)
                            .await
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
                    let suites =
                        crate::fleet::repair::forget_kills_on(&orch.db.local, &executor_id)
                            .await
                            .map_err(|error| build_failure(index, item, error))?;
                    applied.push(format!(
                        "declared repaired (repair {epoch}) — every check infrastructure-suppressed on this machine's environment runs again, it is eligible again for projects it had failed to prepare, and the out-of-memory kills it held against {suites} suite(s) and their commands are forgotten; a MISSING build-prerequisite verdict still stands until the machine re-attaches"
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

#[cfg(test)]
mod machine_service_door_tests {
    use super::super::issue_mutation_tests::{change_item, seed_running_node};
    use super::*;
    use crate::authorization::prepare::Prepared;
    use crate::authorization::{issue_grant, resolve_actor, GrantIssue};
    use cairn_common::authorization::{
        AuthorityConstraintSet, AuthorityLifetimeKind, AuthorityProvenance, AuthorityRequest,
    };
    use serde_json::json;

    const FLEET_PROBE: &str = "name: Probe\nruntime: native\nplacement: fleet\ntransport: mcp-stdio\nargs: [serve]\nartifacts:\n  version: 1.0.0\n  prove: [--version]\n  targets:\n    x86_64-unknown-linux-gnu:\n      url: https://example.invalid/probe\n      sha256: eb5765e715747caccda94ba30266d6d8616861bc9893ca152700546db938df54\n      size: 10\n      executable: probe\n";

    fn as_run(run_id: &str) -> McpCallbackRequest {
        McpCallbackRequest {
            answer_bound_ms: None,
            thread_id: None,
            cwd: "/tmp".to_string(),
            run_id: Some(run_id.to_string()),
            tool: "change".to_string(),
            payload: json!({}),
            tool_use_id: None,
            origin: None,
        }
    }

    async fn fixture() -> (
        Orchestrator,
        String,
        tempfile::TempDir,
        tokio::sync::mpsc::UnboundedReceiver<cairn_common::executor_protocol::ExecutorMessage>,
    ) {
        let orch = crate::orchestrator::models::tests::seeded_orchestrator().await;
        let (_number, _job, run_id) = seed_running_node(&orch).await;
        let (sender, link) = tokio::sync::mpsc::unbounded_channel();
        orch.fleet.attach_executor(sender);
        let resources = tempfile::tempdir().unwrap();
        let root = resources.path().join("packs/kit");
        std::fs::create_dir_all(root.join("services/probe")).unwrap();
        std::fs::write(
            root.join("cairn-pack.yaml"),
            "id: kit\nname: Kit\nversion: 1.0.0\n",
        )
        .unwrap();
        std::fs::write(root.join("services/probe/service.yaml"), FLEET_PROBE).unwrap();
        cairn_packs::sync_one_pack(
            &crate::services::RealGitClient,
            &crate::services::RealFileSystem,
            resources.path(),
            &orch.config_dir,
            "kit",
        )
        .unwrap();
        (orch, run_id, resources, link)
    }

    fn patch(command: Option<&str>) -> ChangeItem {
        let selection = match command {
            Some(command) => json!({"override": {"command": command}}),
            None => serde_json::Value::Null,
        };
        change_item(
            "cairn://executors/local",
            ChangeMode::Patch,
            Some(json!({"machineServices": {"kit/probe": selection}})),
        )
    }

    async fn write(orch: &Orchestrator, run_id: &str, item: &ChangeItem) -> Result<String, String> {
        dispatch(
            orch,
            &as_run(run_id),
            0,
            item,
            false,
            &CairnResource::Executor {
                name: "local".to_string(),
            },
        )
        .await
        .map(|summary| summary.unwrap_or_default())
        .map_err(|failure| failure.error)
    }

    async fn card(orch: &Orchestrator, run_id: &str, item: &ChangeItem) -> AuthorityRequest {
        match machine_services_authority(orch, &as_run(run_id), item).await {
            Some(Prepared::Authority(request)) => request,
            other => panic!("a widening patch raises a card: {other:?}"),
        }
    }

    fn held(orch: &Orchestrator) -> Option<cairn_config::fleet::MachineSelection> {
        crate::config::settings::load_fleet(&orch.config_dir)
            .machine_services
            .selection(&"kit/probe".parse().unwrap(), "local")
            .cloned()
    }

    async fn approve_once(orch: &Orchestrator, run_id: &str, request: &AuthorityRequest) -> String {
        let actor = resolve_actor(orch, &as_run(run_id)).await.unwrap();
        issue_grant(
            &orch.db.local,
            GrantIssue {
                request: request.clone(),
                principal: actor.principal.clone(),
                audience: actor.audience.clone(),
                lifetime: AuthorityLifetimeKind::Once,
                request_id: Some("perm-1".to_string()),
                turn_id: actor.context.turn_id.clone(),
                session_id: actor.context.session_id.clone(),
                expires_at: None,
                provenance: AuthorityProvenance {
                    issuer: "operator_prompt".to_string(),
                    ..Default::default()
                },
                constraints: AuthorityConstraintSet::for_approval(
                    request,
                    request.bind_act().as_ref(),
                    AuthorityLifetimeKind::Once,
                )
                .unwrap(),
            },
        )
        .await
        .unwrap()
        .id
    }

    /// An agent's override raises a card naming the machine, the key, the
    /// action and the command, and writes nothing until it is approved; the
    /// approval admits exactly that act, and the row then names the agent and
    /// the grant. Another command is not covered. Clearing needs no card.
    #[tokio::test]
    async fn an_agents_override_is_carded_written_only_on_approval_and_attributed() {
        let (orch, run_id, _resources, _link) = fixture().await;
        let item = patch(Some("/opt/probe"));
        let request = card(&orch, &run_id, &item).await;
        assert!(request.requires_act_binding());
        for says in [
            "select kit/probe on local, running /opt/probe",
            "on this runner's host as a child of the runner",
            "Cairn approves this path, not the file's bytes",
        ] {
            assert!(
                request.summary.contains(says),
                "{says}: {}",
                request.summary
            );
        }

        let error = write(&orch, &run_id, &item).await.unwrap_err();
        assert!(
            error.contains("Refused") || error.contains("approv"),
            "{error}"
        );
        assert!(held(&orch).is_none(), "nothing is written before approval");

        let grant = approve_once(&orch, &run_id, &request).await;
        let other = patch(Some("/opt/other"));
        assert!(
            write(&orch, &run_id, &other).await.is_err(),
            "another command is not covered"
        );
        assert!(held(&orch).is_none());

        let summary = write(&orch, &run_id, &item).await.unwrap();
        assert!(summary.contains("runs override /opt/probe"), "{summary}");
        assert!(
            summary.contains(&format!("approved by the operator ({grant})")),
            "{summary}"
        );
        let set_by = held(&orch).unwrap().set_by.unwrap();
        assert_eq!(set_by.kind, cairn_config::fleet::SetByKind::Agent);
        assert_eq!(set_by.approval.as_deref(), Some(grant.as_str()));
        assert!(set_by.node_uri.is_some());

        // Clearing it is a reduction: no card, no grant.
        assert!(
            machine_services_authority(&orch, &as_run(&run_id), &patch(None))
                .await
                .is_none()
        );
        write(&orch, &run_id, &patch(None)).await.unwrap();
        assert!(held(&orch).is_none());
    }

    /// A widening selection must come alone, so the card shows one act.
    #[tokio::test]
    async fn a_widening_selection_beside_other_fields_is_refused() {
        let (orch, run_id, _resources, _link) = fixture().await;
        let item = change_item(
            "cairn://executors/local",
            ChangeMode::Patch,
            Some(json!({
                "machineServices": {"kit/probe": {"override": {"command": "/opt/probe"}}},
                "role": "general"
            })),
        );
        match machine_services_authority(&orch, &as_run(&run_id), &item).await {
            Some(Prepared::Refused(reason)) => {
                assert!(
                    reason.contains("send machineServices on its own"),
                    "{reason}"
                )
            }
            other => panic!("refused before any card: {other:?}"),
        }
        let error = write(&orch, &run_id, &item).await.unwrap_err();
        assert!(error.contains("send machineServices on its own"), "{error}");
        assert!(held(&orch).is_none());
    }
}
