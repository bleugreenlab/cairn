//! Which pack artifacts the runner serves a machine: the third family on the
//! executor artifact pull, beside the executor and its tools.

use cairn_common::executor_protocol::MachineServiceArtifact;
use cairn_config::fleet::MachineServiceKey;

use crate::orchestrator::Orchestrator;
use crate::project_services::declaration::{Placement, ServicePackage};

/// Start every selected machine service the runner can, at runner start. Each
/// cell's own probe then keeps it in step with its machine's comings and goings.
pub fn start(orch: &Orchestrator) {
    crate::project_services::supervisor::machine::spawn_apply(orch, "runner start");
}

/// Bring machine services in step after a write that changed which machine
/// names exist (a rename moves a machine's selections, a removal drops them),
/// so the old name's cells drain and the new name's start.
pub fn reapply(orch: &Orchestrator, reason: &'static str) {
    crate::project_services::supervisor::machine::spawn_apply(orch, reason);
}

/// The artifacts `executor_name` is served for `target`, with each `url` the
/// pack's upstream one: every per-machine service the fleet row selects there,
/// and every fleet service, unless the row overrides it there. A service whose
/// pack ships nothing for `target` is left out, and status says why.
pub fn wanted_artifacts(
    orch: &Orchestrator,
    executor_name: &str,
    target: &str,
) -> Vec<MachineServiceArtifact> {
    let row = crate::config::settings::load_fleet(&orch.config_dir).machine_services;
    let mut wanted: Vec<MachineServiceArtifact> = row
        .selected_on(executor_name)
        .into_iter()
        .filter(|(_, selection)| selection.override_command().is_none())
        .filter_map(|(key, _)| {
            let declared = crate::project_services::pack_source::declared_machine_service(
                &orch.config_dir,
                key,
            )?;
            if declared.refusal.is_some() {
                return None;
            }
            let package = declared.package.ok()?;
            // A fleet key's row entry is an override, served by the arm below.
            if package.declaration.placement == Placement::Fleet {
                return None;
            }
            artifact_for(key, &package, target)
        })
        .collect();
    for service in crate::project_services::supervisor::machine::fleet_services(orch) {
        let overridden = row
            .selection(&service.key, executor_name)
            .is_some_and(|selection| selection.override_command().is_some());
        if service.refusal.is_some() || overridden {
            continue;
        }
        wanted.extend(artifact_for(&service.key, &service.package, target));
    }
    wanted
}

/// What a machine on `target` is served to install `key`, if its pack ships
/// anything there.
fn artifact_for(
    key: &MachineServiceKey,
    package: &ServicePackage,
    target: &str,
) -> Option<MachineServiceArtifact> {
    let native = package.declaration.native()?;
    let artifact = native.targets.get(target)?;
    Some(MachineServiceArtifact {
        pack: key.pack.clone(),
        service: key.service.clone(),
        target: target.to_string(),
        url: artifact.url.clone(),
        sha256: artifact.sha256.clone(),
        size: artifact.size,
        version: native.version.clone(),
        archive: artifact.archive,
        executable: artifact.executable.clone(),
        prove_args: native.prove.clone(),
    })
}
