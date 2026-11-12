//! A machine service as pack content: taking one out of an installed pack.
//!
//! Which machines run a service is fleet state, not pack state, so removing the
//! service is also what clears its selections: an entry in the row for a service
//! no pack declares would be a machine showing a state nothing can produce.
//! Restoring the item or reinstalling the pack starts with no machine selected.

use std::collections::BTreeMap;
use std::path::Path;

use cairn_config::fleet::MachineServiceKey;

use super::mcp_servers::{MachineVerdict, McpServerConfig, NotReady};
use super::pack::{self, PackItemKind};
use crate::orchestrator::Orchestrator;
use crate::project_services::declaration::{NativeLaunch, Placement, Transport};
use crate::project_services::pack_source::declared_machine_service;
use crate::project_services::supervisor::machine::{self, InstanceState};

/// One machine service on one selected machine, as readiness and the Library
/// see it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstanceView {
    pub machine: String,
    pub state: InstanceState,
    pub installed_version: Option<String>,
    pub override_command: Option<String>,
    /// Who made the selection, when the row recorded it.
    pub set_by: Option<cairn_config::fleet::SetBy>,
    /// The instance's own sentence for what it runs and who chose it.
    pub provenance: String,
    /// Whether it serves MCP to this runner's gateway.
    pub serves_mcp: bool,
    /// `placement: fleet`: delivered to the machine and launched on demand.
    pub fleet: bool,
    /// For a fleet service's ready host instance, what the host launches.
    pub(crate) launch: Option<NativeLaunch>,
}

/// A machine a service could be selected on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MachineCandidate {
    pub name: String,
    /// The artifact plane's target triple, when anything can name it.
    pub target: Option<String>,
    pub attached: bool,
}

/// Every selected machine service's instances, and every machine there is, as
/// they stand at one read.
///
/// Computed on each read and never persisted: the row and the supervisor stay
/// the only stores. Everything that needs an instance's state takes this value,
/// so a test can decide readiness from an instance set it builds itself.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MachineServiceSnapshot {
    pub instances: BTreeMap<MachineServiceKey, Vec<InstanceView>>,
    pub machines: Vec<MachineCandidate>,
}

impl MachineServiceSnapshot {
    /// No machine and no instance: what readiness sees with nothing selected.
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn capture(orch: &Orchestrator) -> Self {
        let fleet = super::settings::load_fleet(&orch.config_dir);
        let mut machines: BTreeMap<String, MachineCandidate> = machine::attached_now(orch)
            .into_iter()
            .map(|view| {
                let candidate = MachineCandidate {
                    name: view.name.clone(),
                    target: view.target,
                    attached: true,
                };
                (view.name, candidate)
            })
            .collect();
        for (executor_id, config) in &fleet.remote_executors {
            let name = crate::fleet::management::public_name(orch, executor_id);
            let target = config.platform.target().to_string();
            let candidate = machines
                .entry(name.clone())
                .or_insert_with(|| MachineCandidate {
                    name,
                    target: None,
                    attached: false,
                });
            candidate.target.get_or_insert(target);
        }
        let mut instances = BTreeMap::new();
        for key in machine::selected_keys(orch) {
            let views: Vec<InstanceView> = machine::instances(orch, &key)
                .into_iter()
                .map(|instance| InstanceView {
                    machine: instance.machine,
                    state: instance.state,
                    installed_version: instance.installed_version,
                    override_command: instance.override_command,
                    set_by: instance.set_by,
                    provenance: instance.provenance,
                    serves_mcp: instance.serves_mcp,
                    fleet: instance.fleet,
                    launch: instance.launch,
                })
                .collect();
            for view in &views {
                machines
                    .entry(view.machine.clone())
                    .or_insert_with(|| MachineCandidate {
                        name: view.machine.clone(),
                        target: None,
                        attached: false,
                    });
            }
            instances.insert(key, views);
        }
        Self {
            instances,
            machines: machines.into_values().collect(),
        }
    }

    /// The instances of `key`: its selected machines, each with its state.
    pub fn views(&self, key: &MachineServiceKey) -> &[InstanceView] {
        self.instances.get(key).map_or(&[], Vec::as_slice)
    }

    /// The machines where `key` is ready to serve MCP now.
    pub fn ready_on(&self, key: &MachineServiceKey) -> Vec<&str> {
        self.views(key)
            .iter()
            .filter(|view| view.state == InstanceState::Ready && view.serves_mcp)
            .map(|view| view.machine.as_str())
            .collect()
    }

    /// Why `key` cannot be listed, or `None` when one machine has it ready. Only
    /// ready admits: a degraded instance is a crash backoff or a probe that says
    /// no, and neither is a server worth advertising. A fleet service is
    /// launched only by the host, so only the host's state is a reason.
    pub fn not_ready(&self, key: &MachineServiceKey) -> Option<NotReady> {
        if !self.ready_on(key).is_empty() {
            return None;
        }
        Some(NotReady::MachineService {
            service: key.service.clone(),
            machines: self
                .views(key)
                .iter()
                .filter(|view| !view.fleet || view.machine == machine::HOST_MACHINE)
                .map(|view| view.state.verdict(&view.machine))
                .collect::<Vec<MachineVerdict>>(),
        })
    }

    /// The host's ready view of the fleet service `key` and what it launches:
    /// the launch with the machine, the override and the setter beside it, so
    /// the gateway's spawn can be floored and recorded. Read by MCP
    /// resolution, so it stays in memory and credential-free.
    pub(crate) fn host_launch(
        &self,
        key: &MachineServiceKey,
    ) -> Option<(&InstanceView, &NativeLaunch)> {
        self.views(key)
            .iter()
            .filter(|view| view.state == InstanceState::Ready && view.serves_mcp)
            .find_map(|view| view.launch.as_ref().map(|launch| (view, launch)))
    }
}

/// The service a `machine-service` entry from `pack_id` reaches, or why it
/// cannot reach one, naming the key. This reads the one declaration loader; an
/// entry that fails here is withheld with this sentence as its reason.
pub(crate) fn machine_service_entry(
    config_dir: &Path,
    pack_id: &str,
    config: &McpServerConfig,
) -> Result<MachineServiceKey, String> {
    if let Some(error) = config.machine_service_shape_error() {
        return Err(error);
    }
    let service = config.service.as_deref().unwrap_or_default();
    if pack::lock::read_lock(config_dir, pack_id)
        .is_some_and(|lock| lock.source.kind != pack::PackSourceKind::Bundled)
    {
        return Err(pack::manifest::NON_BUNDLED_SERVICES_NOTE.to_string());
    }
    let key = MachineServiceKey::new(pack_id, service)?;
    let declared = declared_machine_service(config_dir, &key)
        .ok_or_else(|| format!("`service: {service}` names no service this pack ships"))?;
    let package = declared.package?;
    match package.declaration.transport {
        Transport::McpStdio => Ok(key),
        Transport::None => Err(format!(
            "`service: {service}` declares `transport: none`; a machine-service entry needs \
             `transport: mcp-stdio`"
        )),
        Transport::Ndjson => Err(format!(
            "`service: {service}` declares `transport: ndjson`; a machine-service entry needs \
             `transport: mcp-stdio`"
        )),
    }
}

/// Whether `key` is declared `placement: fleet`: a program the host launches on
/// demand, not a resident that a call is routed to.
pub(crate) fn is_fleet(config_dir: &Path, key: &MachineServiceKey) -> bool {
    declared_machine_service(config_dir, key)
        .and_then(|declared| declared.package.ok())
        .is_some_and(|package| package.declaration.placement == Placement::Fleet)
}

/// Remove the service `id` from `pack_id`, leaving the pack installed.
///
/// The caller announces the change, which is what drains the service's cells.
/// Take a machine service out of its pack: record the removal (a service runs
/// from the app bundle's declaration, so there is no copy to delete) and clear
/// every machine's selection of it.
pub fn delete_pack_service(orch: &Orchestrator, pack_id: &str, id: &str) -> Result<(), String> {
    pack::lock::record_removal_from(
        &crate::services::RealFileSystem,
        &orch.config_dir,
        pack_id,
        PackItemKind::Service,
        id,
    )?;
    clear_selections(orch, [MachineServiceKey::new(pack_id, id)?])
}

/// Clear every machine's selection of `keys`, overrides included. Writes nothing
/// unless one of them is selected: saving the fleet settles its defaults into
/// `settings.yaml`, and uninstalling a pack with no selection must not do that.
pub fn clear_selections(
    orch: &Orchestrator,
    keys: impl IntoIterator<Item = MachineServiceKey>,
) -> Result<(), String> {
    let row = super::settings::load_fleet(&orch.config_dir).machine_services;
    let selected: Vec<MachineServiceKey> = keys
        .into_iter()
        .filter(|key| row.0.contains_key(key))
        .collect();
    if selected.is_empty() {
        return Ok(());
    }
    crate::fleet::management::mutate_fleet(orch, |config| {
        for key in &selected {
            config.machine_services.0.remove(key);
        }
        Ok(())
    })
}

/// The key of every service `lock` holds or has recorded as removed.
pub(crate) fn service_keys(lock: &pack::PackLock) -> Vec<MachineServiceKey> {
    lock.items
        .iter()
        .map(pack::PackLockItem::manifest_item)
        .chain(lock.removed.iter().cloned())
        .filter(|item| item.kind == PackItemKind::Service)
        .filter_map(|item| MachineServiceKey::new(&lock.id, &item.id).ok())
        .collect()
}
