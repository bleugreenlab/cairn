//! Machine services: a pack's native service, run as one cell of the services
//! supervisor on each machine the fleet row (`buildSlots.machineServices`)
//! selects it on.
//!
//! Everything that is a service's lifecycle stays the supervisor's: the cell,
//! its generations, the crash window, backoff and `failed`, the lease, the log.
//! What is particular to a machine service lives here:
//!
//! - its source, [`MachineSource`]: the pack's declaration plus what this one
//!   machine runs (the operator's override, or the pack artifact the executor
//!   installed), hashed together so a bump or a new override is a rolling
//!   replace;
//! - [`apply_machine_services`], which turns "the row, the installed packs, the
//!   attached machines or their installed artifacts may have changed" into
//!   starts, replacements and drains, as `apply_source` does for a project;
//! - the probe, which says whether the resident is ready, and why not;
//! - the facade, through which the MCP gateway reaches an `mcp-stdio`
//!   resident's stdio, and which the cell owns, so there is one per instance;
//! - the instance set, which every surface reads state from. State is derived
//!   here, live, and never stored.

use super::super::declaration::{NativeSpec, Placement};
use super::*;
use crate::config::mcp_servers::{MachineStateTag, MachineVerdict};
use crate::mcp::gateway::{DuplexIo, McpToolDef, PlacedFacade};
use cairn_common::executor_protocol::{
    managed_program_path, InstalledMachineService, MachineServiceInstallOutcome, ManagedProgram,
    RemotePlatform, LOCAL_EXECUTOR_NAME,
};
use cairn_config::fleet::{MachineSelection, MachineServiceKey, MachineServicesConfig};
use sha2::{Digest, Sha256};
use std::sync::{OnceLock, Weak};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

/// How long one probe one-shot may take.
const PROBE_TIMEOUT: Duration = Duration::from_secs(20);
/// How often a cell looks at what the fleet says about its machine. Attaching,
/// detaching and a changed install report arrive unannounced, and a resident
/// that stays ready is only probed every `probe.every`.
const MACHINE_POLL: Duration = Duration::from_secs(1);
/// How often a cell that cannot start (nothing installed, crashed) looks again.
const BLOCKED_POLL: Duration = Duration::from_secs(10);
/// The longest a resident that is not ready goes between probes, so a
/// permission an operator grants is noticed promptly.
const NOT_READY_POLL: Duration = Duration::from_secs(30);
const DUPLEX_CAPACITY: usize = 64 * 1024;

/// A machine as machine services see it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MachineView {
    /// Its public name, the row's key.
    pub name: String,
    /// The artifact plane's target triple, when the machine runs one.
    pub target: Option<String>,
    /// What its executor reports holding.
    pub installed: Vec<InstalledMachineService>,
    /// Whether its executor keeps a service resident's credential floor. One
    /// that predates it runs every service resident unconfined, so no override
    /// is started there.
    pub floors_service_residents: bool,
}

/// Why an override is not started on a machine whose executor cannot floor it.
const OVERRIDE_NEEDS_FLOOR: &str =
    "this executor predates floored overrides, so the override is not started; update it";

/// The machines attached to this runner now.
pub(crate) fn attached_machines(orch: &Orchestrator) -> Vec<MachineView> {
    orch.fleet
        .inspect_executors(crate::fleet::unix_time_ms())
        .into_iter()
        .map(|executor| {
            let capabilities = &executor.health.advertisement.capabilities;
            MachineView {
                target: RemotePlatform::from_os_arch(&capabilities.os, &capabilities.arch)
                    .map(|platform| platform.target().to_string()),
                installed: capabilities.machine_services.clone(),
                floors_service_residents: capabilities.credential_floor_service_residents,
                name: executor.name,
            }
        })
        .collect()
}

/// The machines attached now, through the host every instance is derived from.
pub(crate) fn attached_now(orch: &Orchestrator) -> Vec<MachineView> {
    supervisor(orch).host().machines(orch)
}

/// What the last probe of a resident found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ProbeOutcome {
    Ready,
    NotReady {
        reason: Option<String>,
        /// The reason is one the declaration says a person must act on.
        waiting_for_operator: bool,
    },
    /// The probe ran and did not answer: no JSON, an error, a timeout.
    NoAnswer {
        reason: String,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct ProbeReport {
    pub at_ms: i64,
    pub outcome: ProbeOutcome,
    /// The resident's MCP tools, listed when it was ready.
    pub catalog: Vec<McpToolDef>,
}

/// A machine service cell's own state.
pub(crate) struct MachineCell {
    key: MachineServiceKey,
    machine: String,
    pub(super) probe: StdMutex<Option<ProbeReport>>,
    /// Why the service cannot start here, from the last attempt to read what
    /// this machine runs; cleared when one succeeds.
    blocked: StdMutex<Option<String>>,
    /// The state last announced to open windows, so an unchanged probe stays
    /// silent.
    pub(super) announced: StdMutex<Option<InstanceState>>,
    probe_task: AtomicBool,
    pub(super) wake: Notify,
    /// No longer selected: its probe task ends and nothing starts it again.
    retired: AtomicBool,
    facade: OnceLock<Arc<MachineServiceFacade>>,
}

impl MachineCell {
    pub(super) fn for_owner(owner: &ServiceOwner, service: &str) -> Option<Self> {
        let ServiceOwner::Pack { pack, machine, .. } = owner else {
            return None;
        };
        Some(Self {
            key: MachineServiceKey::new(pack, service).ok()?,
            machine: machine.clone(),
            probe: StdMutex::new(None),
            blocked: StdMutex::new(None),
            announced: StdMutex::new(None),
            probe_task: AtomicBool::new(false),
            wake: Notify::new(),
            retired: AtomicBool::new(false),
            facade: OnceLock::new(),
        })
    }

    /// `<pack>/<service>@<machine>`: the instance's name everywhere it is
    /// keyed, the gateway's pool included.
    fn instance_key(&self) -> String {
        format!("{}@{}", self.key, self.machine)
    }
}

/// Open the source a machine service cell reads: the pack's declaration as
/// this machine runs it.
pub(super) fn open_source(
    orch: &Orchestrator,
    packs: Vec<(String, Arc<dyn ServiceSource>)>,
    pack: &str,
    machine: &str,
) -> Result<Arc<dyn ServiceSource>, String> {
    let base = packs
        .into_iter()
        .find(|(id, _)| id == pack)
        .map(|(_, source)| source)
        .ok_or_else(|| format!("the pack {pack} is not installed"))?;
    Ok(Arc::new(MachineSource {
        pack: pack.to_string(),
        machine: machine.to_string(),
        row: crate::config::settings::load_fleet(&orch.config_dir).machine_services,
        view: supervisor(orch)
            .host()
            .machines(orch)
            .into_iter()
            .find(|view| view.name == machine),
        base,
    }))
}

/// The sentence a machine service's state takes when its machine is away.
const MACHINE_ABSENT: &str = "machine absent";

/// One pack's services as one machine runs them.
struct MachineSource {
    pack: String,
    machine: String,
    base: Arc<dyn ServiceSource>,
    row: MachineServicesConfig,
    view: Option<MachineView>,
}

impl ServiceSource for MachineSource {
    fn revision(&self) -> &str {
        self.base.revision()
    }

    fn list(&self) -> Result<Vec<super::super::declaration::ServiceListing>, String> {
        self.base.list()
    }

    fn refusal(&self, id: &str) -> Option<String> {
        self.base.refusal(id)
    }

    fn package(&self, id: &str) -> Result<Option<ServicePackage>, String> {
        let Some(mut package) = self.base.package(id)? else {
            return Ok(None);
        };
        let Some(native) = package.declaration.native() else {
            return Ok(Some(package));
        };
        let key = MachineServiceKey::new(&self.pack, id)?;
        let Some(selection) = self.row.selection(&key, &self.machine) else {
            return Ok(None);
        };
        if selection.override_command().is_none() {
            if let Some(refusal) = self.base.refusal(id) {
                return Err(refusal);
            }
        }
        let (program, provenance) =
            resolve_program(&key, &self.machine, selection, native, self.view.as_ref())?;
        let mut hasher = Sha256::new();
        hasher.update(package.source_hash.as_bytes());
        hasher.update([0]);
        hasher.update(provenance.as_bytes());
        package.source_hash = format!("{:x}", hasher.finalize());
        let set_by = selection.set_by.clone();
        package.launch = Some(match selection.override_command() {
            Some(_) => NativeLaunch::overriding(program, native.args.clone(), set_by),
            None => NativeLaunch::artifact(program, native.args.clone(), set_by),
        });
        Ok(Some(package))
    }
}

/// What `machine` runs for `key`, and the part of the source it is: the
/// override path, or the artifact's sha256. An error is why nothing can run.
fn resolve_program(
    key: &MachineServiceKey,
    machine: &str,
    selection: &MachineSelection,
    native: &super::super::declaration::NativeSpec,
    view: Option<&MachineView>,
) -> Result<(String, String), String> {
    if let Some(command) = selection.override_command() {
        if view.is_some_and(|view| !view.floors_service_residents) {
            return Err(OVERRIDE_NEEDS_FLOOR.to_string());
        }
        return Ok((command.to_string(), format!("override:{command}")));
    }
    let view = view.ok_or_else(|| MACHINE_ABSENT.to_string())?;
    let target = view.target.as_deref().ok_or_else(|| {
        format!("not installed: {machine} runs a platform no pack artifact targets")
    })?;
    let artifact = native
        .targets
        .get(target)
        .ok_or_else(|| format!("not installed: {key} ships no artifact for {target}"))?;
    let report = view.installed.iter().find(|installed| {
        installed.pack == key.pack
            && installed.service == key.service
            && installed.sha256 == artifact.sha256
    });
    match report.map(|report| &report.outcome) {
        Some(MachineServiceInstallOutcome::Installed) => Ok((
            ManagedProgram {
                pack: key.pack.clone(),
                service: key.service.clone(),
                sha256: artifact.sha256.clone(),
                executable: artifact.executable.clone(),
            }
            .intent(),
            format!("artifact:{}", artifact.sha256),
        )),
        Some(MachineServiceInstallOutcome::Installing) => Err("installing".to_string()),
        Some(MachineServiceInstallOutcome::NotInstalled { reason }) => {
            Err(format!("not installed: {reason}"))
        }
        None => Err(format!(
            "not installed: {machine} has not reported installing {} {}",
            key, native.version
        )),
    }
}

/// Bring every machine service in step with the row, the installed packs,
/// and the attached machines: start what is selected and runnable, roll what
/// changed, and drain what is no longer selected. Each cell's own probe task
/// keeps it there between passes.
pub(crate) async fn apply_machine_services(orch: &Orchestrator, reason: &str) {
    let _apply = supervisor(orch).machine_apply.lock().await;
    let row = crate::config::settings::load_fleet(&orch.config_dir).machine_services;
    let mut wanted: Vec<(String, String, String)> = Vec::new();
    for (pack, source) in supervisor(orch).sources().packs(orch) {
        let listed = tokio::task::spawn_blocking(move || {
            source.list().map(|listings| {
                listings
                    .into_iter()
                    .map(|listing| {
                        let refused = source.refusal(&listing.id).is_some();
                        (listing, refused)
                    })
                    .collect::<Vec<_>>()
            })
        })
        .await;
        let listings = match listed {
            Ok(Ok(listings)) => listings,
            Ok(Err(error)) => {
                tracing::warn!(%error, pack, "could not list a pack's services");
                continue;
            }
            Err(_) => continue,
        };
        for (listing, refused) in listings {
            let Ok(declaration) = &listing.declaration else {
                continue;
            };
            if declaration.placement != super::super::declaration::Placement::PerMachine {
                continue;
            }
            let Ok(key) = MachineServiceKey::new(&pack, &listing.id) else {
                continue;
            };
            for (machine, selection) in row.machines_for(&key) {
                // Left out rather than blocked, so one already running drains.
                if refused && selection.override_command().is_none() {
                    continue;
                }
                wanted.push((pack.clone(), listing.id.clone(), machine.to_string()));
            }
        }
    }
    let held: Vec<Arc<Cell>> = supervisor(orch)
        .cells
        .lock()
        .unwrap()
        .values()
        .filter(|cell| cell.machine.is_some())
        .cloned()
        .collect();
    for cell in held {
        let ServiceOwner::Pack { pack, machine, .. } = &cell.owner else {
            continue;
        };
        let still = wanted
            .iter()
            .any(|(p, s, m)| p == pack && s == &cell.service && m == machine);
        if !still {
            drain(orch, &cell, reason).await;
        }
    }
    for (pack, service, machine) in wanted {
        let cell = supervisor(orch).cell("", "", &ServiceOwner::pack(&pack, &machine), &service);
        ensure_cell(orch, &cell, reason).await;
        spawn_probe(orch, &cell);
    }
}

/// Stop a cell that is no longer selected (or whose pack went away), give
/// back its lease, and forget it.
async fn drain(orch: &Orchestrator, cell: &Arc<Cell>, reason: &str) {
    let Some(machine_cell) = cell.machine.as_ref() else {
        return;
    };
    machine_cell.retired.store(true, Ordering::Release);
    machine_cell.wake.notify_one();
    let _ops = cell.ops.lock().await;
    let current = {
        let mut state = cell.state.lock().unwrap();
        state.status = ServiceState::Stopped;
        state.current.take()
    };
    supervisor(orch).cells.lock().unwrap().remove(&cell_key(
        &cell.project_id,
        &cell.owner,
        &cell.service,
    ));
    if let Some(current) = current {
        lifecycle(
            orch,
            cell,
            current.generation,
            "no longer selected on this machine; stopping",
            reason,
        )
        .await;
        retire(current).await;
    }
    supervisor(orch)
        .host()
        .release(orch, &cell.project_id, &cell.owner, &cell.service)
        .await;
    if let Some(gateway) = orch.mcp_gateway() {
        gateway.close_placed(&machine_cell.instance_key()).await;
    }
}

/// Start the cell when it should be serving and is not, or replace it when
/// what the machine runs has changed. Does nothing while the machine is away,
/// while the cell backs off or has failed, or once it is retired.
async fn ensure_cell(orch: &Orchestrator, cell: &Arc<Cell>, reason: &str) {
    let Some(machine_cell) = cell.machine.as_ref() else {
        return;
    };
    if machine_cell.retired.load(Ordering::Acquire) || !attached(orch, &machine_cell.machine) {
        return;
    }
    let _ops = cell.ops.lock().await;
    if machine_cell.retired.load(Ordering::Acquire) {
        return;
    }
    let package = match read_package(orch, cell).await {
        Ok(package) => {
            *machine_cell.blocked.lock().unwrap() = None;
            package
        }
        Err(error) => {
            *machine_cell.blocked.lock().unwrap() = Some(error.message);
            return;
        }
    };
    let serving_hash = {
        let mut state = cell.state.lock().unwrap();
        // An incarnation that ended without the cell being told (a retired
        // one the gateway let go of) holds nothing.
        if state
            .current
            .as_ref()
            .is_some_and(|current| current.phase() == Phase::Exited)
        {
            state.current = None;
            if state.status == ServiceState::Running {
                state.status = ServiceState::Stopped;
            }
        }
        state
            .current
            .as_ref()
            .filter(|current| current.serving())
            .map(|current| current.source_hash.clone())
    };
    match serving_hash {
        Some(hash) if hash == package.source_hash => {}
        Some(_) => {
            let _ = replace(orch, cell, &package, reason).await;
        }
        None => {
            let startable = {
                let mut state = cell.state.lock().unwrap();
                if state.last_source_hash.as_deref() != Some(package.source_hash.as_str()) {
                    // What the machine runs changed: whatever failed before
                    // was about something else.
                    state.crashes.clear();
                    if matches!(
                        state.status,
                        ServiceState::Failed | ServiceState::UpdateFailed | ServiceState::Backoff
                    ) {
                        state.status = ServiceState::Stopped;
                    }
                    state.last_source_hash = Some(package.source_hash.clone());
                }
                !state.sticky_stop && state.status == ServiceState::Stopped
            };
            if !startable {
                return;
            }
            match boot(orch, cell, &package, reason).await {
                Ok(incarnation) => install(orch, cell, incarnation),
                // A resident that cannot start is crashing as far as backoff
                // is concerned: retrying it every pass would be a hot loop.
                Err(error) => {
                    let generation = cell.state.lock().unwrap().generation;
                    crashed(
                        orch,
                        cell,
                        generation,
                        &format!("start failed: {}", error.message),
                    )
                    .await;
                }
            }
        }
    }
}

fn machine_view(orch: &Orchestrator, machine: &str) -> Option<MachineView> {
    attached_now(orch)
        .into_iter()
        .find(|view| view.name == machine)
}

fn attached(orch: &Orchestrator, machine: &str) -> bool {
    machine_view(orch, machine).is_some()
}

/// Whether this cell runs the pack's artifact and its pack refuses that.
async fn refused_here(orch: &Orchestrator, machine_cell: &MachineCell) -> bool {
    let orch = orch.clone();
    let key = machine_cell.key.clone();
    let machine = machine_cell.machine.clone();
    tokio::task::spawn_blocking(move || {
        let row = crate::config::settings::load_fleet(&orch.config_dir).machine_services;
        row.selection(&key, &machine)
            .is_some_and(|selection| selection.override_command().is_none())
            && declared(&orch, &key).refusal.is_some()
    })
    .await
    .unwrap_or(false)
}

/// Start the cell's probe task, or wake the running one.
fn spawn_probe(orch: &Orchestrator, cell: &Arc<Cell>) {
    let Some(machine_cell) = cell.machine.as_ref() else {
        return;
    };
    if machine_cell.probe_task.swap(true, Ordering::AcqRel) {
        machine_cell.wake.notify_one();
        return;
    }
    tokio::spawn(probe_loop(orch.clone(), cell.clone()));
}

/// One cell's probe, for as long as it is selected: keep it started, and ask
/// the resident what state it is in, every `probe.every` while it is ready
/// and sooner while it is not. A probe that does not answer degrades the
/// cell; it is never counted as a crash.
async fn probe_loop(orch: Orchestrator, cell: Arc<Cell>) {
    let machine_cell = cell.machine.as_ref().expect("only machine cells probe");
    loop {
        if machine_cell.retired.load(Ordering::Acquire) {
            break;
        }
        // Read before the pass acts on it, so a change during the pass ends the
        // wait after it.
        let seen = machine_view(&orch, &machine_cell.machine);
        // Nothing watches the copy, so this pass is where an edit is noticed.
        // Apply is what leaves a refused cell out, and it drains this one.
        if refused_here(&orch, machine_cell).await {
            apply_machine_services(&orch, "its copy was edited").await;
        }
        ensure_cell(&orch, &cell, "runner (machine service)").await;
        // A resident survives a link gap, and a probe of a machine that is away
        // can only time out.
        let pause = match cell
            .serving()
            .filter(|_| attached(&orch, &machine_cell.machine))
        {
            Some(incarnation) => {
                let every = incarnation
                    .declaration
                    .native()
                    .and_then(|native| native.resident_probe())
                    .map_or(NOT_READY_POLL, |probe| probe.every);
                let report = probe_once(&orch, &cell, &incarnation).await;
                let ready = report.outcome == ProbeOutcome::Ready;
                *machine_cell.probe.lock().unwrap() = Some(report);
                if ready {
                    every
                } else {
                    every.min(NOT_READY_POLL)
                }
            }
            None => BLOCKED_POLL,
        };
        announce_state(&orch, &cell);
        wait_for_next_pass(&orch, machine_cell, seen, pause).await;
    }
    machine_cell.probe_task.store(false, Ordering::Release);
}

/// Wait out `pause`, ending early if the cell is woken or its machine no longer
/// looks the way it did in `seen`.
async fn wait_for_next_pass(
    orch: &Orchestrator,
    machine_cell: &MachineCell,
    seen: Option<MachineView>,
    pause: Duration,
) {
    let deadline = tokio::time::Instant::now() + pause;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        tokio::select! {
            () = tokio::time::sleep(remaining.min(MACHINE_POLL)) => {}
            () = machine_cell.wake.notified() => return,
        }
        if tokio::time::Instant::now() >= deadline
            || machine_view(orch, &machine_cell.machine) != seen
        {
            return;
        }
    }
}

fn spec_for(cell: &Cell, incarnation: &Incarnation) -> LaunchSpec {
    LaunchSpec {
        project_id: cell.project_id.clone(),
        project_key: cell.project_key.clone(),
        owner: cell.owner.clone(),
        service: cell.service.clone(),
        generation: incarnation.generation,
        declaration: incarnation.declaration.clone(),
        files: Vec::new(),
        env: Vec::new(),
        native: incarnation.launch.clone(),
    }
}

/// Run the declaration's probe and read its answer.
async fn probe_status(orch: &Orchestrator, spec: &LaunchSpec) -> ProbeOutcome {
    let (Some(probe), Some(launch)) = (
        spec.declaration
            .native()
            .and_then(|native| native.resident_probe()),
        spec.native.as_ref(),
    ) else {
        return ProbeOutcome::NoAnswer {
            reason: "the service declares no probe".to_string(),
        };
    };
    match supervisor(orch)
        .host()
        .probe(
            orch,
            spec,
            &launch.program,
            probe.args.clone(),
            PROBE_TIMEOUT,
        )
        .await
    {
        Ok(output) => interpret(probe, &output),
        Err(error) => ProbeOutcome::NoAnswer { reason: error },
    }
}

/// Read a probe's stdout through the declaration's JSON pointers. Any exit
/// that printed JSON is an answer; the declaration says what it means.
fn interpret(probe: &super::super::declaration::ProbeSpec, output: &ProbeOutput) -> ProbeOutcome {
    let Ok(status) = serde_json::from_slice::<Value>(&output.stdout) else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return ProbeOutcome::NoAnswer {
            reason: format!(
                "its probe exited {} without JSON{}",
                output
                    .exit_code
                    .map_or("with no code".to_string(), |code| code.to_string()),
                match stderr.trim() {
                    "" => String::new(),
                    stderr => format!(": {}", stderr.lines().next().unwrap_or(stderr)),
                }
            ),
        };
    };
    if status
        .pointer(&probe.ready)
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return ProbeOutcome::Ready;
    }
    let reason = probe
        .reason
        .iter()
        .find_map(|pointer| status.pointer(pointer).and_then(Value::as_str))
        .map(str::to_string);
    let waiting_for_operator = reason.as_deref().is_some_and(|reason| {
        probe
            .waiting_for_operator
            .iter()
            .any(|known| known == reason)
    });
    ProbeOutcome::NotReady {
        reason,
        waiting_for_operator,
    }
}

/// Probe a serving resident, and list its tools when it is ready and speaks
/// MCP. A listing that fails keeps the catalog it had.
pub(super) async fn probe_once(
    orch: &Orchestrator,
    cell: &Arc<Cell>,
    incarnation: &Arc<Incarnation>,
) -> ProbeReport {
    let outcome = probe_status(orch, &spec_for(cell, incarnation)).await;
    let previous = cell
        .machine
        .as_ref()
        .and_then(|machine_cell| machine_cell.probe.lock().unwrap().clone())
        .map(|report| report.catalog)
        .unwrap_or_default();
    let catalog = match (&outcome, incarnation.transport, orch.mcp_gateway()) {
        (ProbeOutcome::Ready, Transport::McpStdio, Some(gateway)) => match facade_for(orch, cell) {
            Some(facade) => match gateway.list_placed_tools(facade).await {
                Ok(catalog) => catalog.tools,
                Err(error) => {
                    tracing::warn!(service = %cell.service, %error, "could not list a machine service's tools");
                    previous
                }
            },
            None => previous,
        },
        (ProbeOutcome::Ready, _, _) => previous,
        _ => Vec::new(),
    };
    ProbeReport {
        at_ms: now_ms(),
        outcome,
        catalog,
    }
}

/// A native resident is booted once its process has started and its probe
/// has answered within [`READY_TIMEOUT`], whatever state the answer reports.
pub(super) async fn first_probe(
    orch: &Orchestrator,
    cell: &Arc<Cell>,
    spec: &LaunchSpec,
    incarnation: &Incarnation,
    phase_rx: &mut watch::Receiver<Phase>,
) -> Result<(), String> {
    let answered = async {
        match probe_status(orch, spec).await {
            ProbeOutcome::NoAnswer { reason } => Err(reason),
            outcome => Ok(outcome),
        }
    };
    let exited = async {
        loop {
            if *phase_rx.borrow_and_update() == Phase::Exited {
                return;
            }
            if phase_rx.changed().await.is_err() {
                return;
            }
        }
    };
    let outcome = tokio::time::timeout(READY_TIMEOUT, async {
        tokio::select! {
            outcome = answered => outcome,
            () = exited => Err("the process exited before its probe answered".to_string()),
        }
    })
    .await
    .unwrap_or_else(|_| {
        Err(format!(
            "its probe did not answer within {}s",
            READY_TIMEOUT.as_secs()
        ))
    })?;
    if let Some(machine_cell) = cell.machine.as_ref() {
        *machine_cell.probe.lock().unwrap() = Some(ProbeReport {
            at_ms: now_ms(),
            outcome,
            catalog: Vec::new(),
        });
    }
    let _ = incarnation.phase.send(Phase::Ready);
    Ok(())
}

/// The cell's one facade, made on first use.
pub(super) fn facade_for(orch: &Orchestrator, cell: &Arc<Cell>) -> Option<Arc<dyn PlacedFacade>> {
    let machine_cell = cell.machine.as_ref()?;
    let facade = machine_cell.facade.get_or_init(|| {
        Arc::new(MachineServiceFacade {
            orch: orch.clone(),
            cell: Arc::downgrade(cell),
            key: machine_cell.instance_key(),
            label: format!("{} on {}", machine_cell.key, machine_cell.machine),
        })
    });
    Some(facade.clone())
}

/// The gateway's way into an `mcp-stdio` resident: the cell's serving
/// incarnation's stdio, one connection at a time.
///
/// The cell owns it, which is what makes "one facade per instance" a fact
/// rather than a convention: a second facade on one instance was a second
/// claimant on its lease, and the claimants' renewal loops displaced one
/// another until the machine's admission queue filled (CAIRN-4205).
pub(crate) struct MachineServiceFacade {
    orch: Orchestrator,
    /// Weak, because the cell holds the facade.
    cell: Weak<Cell>,
    key: String,
    label: String,
}

#[async_trait]
impl PlacedFacade for MachineServiceFacade {
    fn key(&self) -> &str {
        &self.key
    }

    fn label(&self) -> String {
        self.label.clone()
    }

    async fn open(&self) -> Result<Box<dyn DuplexIo>, String> {
        let cell = self
            .cell
            .upgrade()
            .ok_or_else(|| format!("{} is no longer selected", self.label))?;
        let incarnation = fresh_incarnation(&self.orch, &cell, &self.label).await?;
        let (gateway, machine) = tokio::io::duplex(DUPLEX_CAPACITY);
        let (sink, output) = mpsc::unbounded_channel();
        *incarnation.stdout_sink.lock().unwrap() = Some(sink);
        tokio::spawn(pump(self.orch.clone(), cell, incarnation, output, machine));
        Ok(Box::new(gateway))
    }
}

/// A serving incarnation no gateway connection has taken yet. An MCP session
/// is one per process, so one already taken is let go and replaced.
async fn fresh_incarnation(
    orch: &Orchestrator,
    cell: &Arc<Cell>,
    label: &str,
) -> Result<Arc<Incarnation>, String> {
    for _ in 0..3 {
        let incarnation = ensure_serving(orch, cell, "gateway")
            .await
            .map_err(|error| format!("{label}: {}", error.message))?;
        if incarnation.transport != Transport::McpStdio {
            return Err(format!("{label} does not serve MCP"));
        }
        if !incarnation.attached.swap(true, Ordering::AcqRel) {
            return Ok(incarnation);
        }
        detach(
            orch,
            cell,
            &incarnation,
            "a new gateway connection replaces it",
        )
        .await;
    }
    Err(format!("{label}: no fresh process could be started"))
}

/// Let go of a resident whose gateway connection ended, so the next one
/// starts fresh. Not a crash: the runner ended it.
async fn detach(orch: &Orchestrator, cell: &Arc<Cell>, incarnation: &Arc<Incarnation>, why: &str) {
    {
        let _ops = cell.ops.lock().await;
        let mut state = cell.state.lock().unwrap();
        if state
            .current
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, incarnation))
        {
            state.current = None;
            state.status = ServiceState::Stopped;
        }
    }
    lifecycle(
        orch,
        cell,
        incarnation.generation,
        &format!("generation {} let go: {why}", incarnation.generation),
        "gateway",
    )
    .await;
    retire(incarnation.clone()).await;
    if let Some(machine_cell) = cell.machine.as_ref() {
        machine_cell.wake.notify_one();
    }
}

/// Carry bytes between the gateway's half of the duplex and the resident.
/// The resident's exit drops its sink, which ends this and so the gateway's
/// stream; the gateway closing its half lets the resident go.
async fn pump(
    orch: Orchestrator,
    cell: Arc<Cell>,
    incarnation: Arc<Incarnation>,
    mut output: mpsc::UnboundedReceiver<Vec<u8>>,
    mut stream: DuplexStream,
) {
    let mut input = vec![0; 16 * 1024];
    loop {
        tokio::select! {
            bytes = output.recv() => match bytes {
                Some(bytes) => {
                    if stream.write_all(&bytes).await.is_err() {
                        break;
                    }
                }
                None => return,
            },
            read = stream.read(&mut input) => match read {
                Ok(0) | Err(_) => break,
                Ok(count) => {
                    if let Err(error) = incarnation.input.write(&input[..count]).await {
                        tracing::warn!(service = %cell.service, %error, "machine service input pump stopped");
                        return;
                    }
                }
            },
        }
    }
    if incarnation.phase() != Phase::Exited {
        detach(&orch, &cell, &incarnation, "its gateway connection closed").await;
    }
}

/// A machine service instance's state, derived live from its cell, its probe
/// and its machine's report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InstanceState {
    NotInstalled { reason: String },
    Installing,
    MachineAbsent,
    Starting,
    WaitingForOperator { reason: String },
    Ready,
    Degraded { reason: String },
    Failed { reason: String },
}

impl InstanceState {
    /// The state as a short phrase: `ready`, `waiting for the operator: …`.
    pub(crate) fn sentence(&self) -> String {
        match self {
            Self::NotInstalled { reason } => format!("not installed: {reason}"),
            Self::Installing => "installing".to_string(),
            Self::MachineAbsent => format!("not running: {MACHINE_ABSENT}"),
            Self::Starting => "starting".to_string(),
            Self::WaitingForOperator { reason } => {
                format!("waiting for the operator: {}", operator_reason(reason))
            }
            Self::Ready => "ready".to_string(),
            Self::Degraded { reason } => format!("degraded: {reason}"),
            Self::Failed { reason } => format!("failed: {reason}"),
        }
    }

    /// This state on `machine`, as the wire carries it.
    pub(crate) fn verdict(&self, machine: &str) -> MachineVerdict {
        let (state, reason) = match self {
            Self::NotInstalled { reason } => (MachineStateTag::NotInstalled, Some(reason.clone())),
            Self::Installing => (MachineStateTag::Installing, None),
            Self::MachineAbsent => (MachineStateTag::MachineAbsent, None),
            Self::Starting => (MachineStateTag::Starting, None),
            Self::WaitingForOperator { reason } => (
                MachineStateTag::WaitingForOperator,
                Some(operator_reason(reason)),
            ),
            Self::Ready => (MachineStateTag::Ready, None),
            Self::Degraded { reason } => (MachineStateTag::Degraded, Some(reason.clone())),
            Self::Failed { reason } => (MachineStateTag::Failed, Some(reason.clone())),
        };
        MachineVerdict {
            machine: machine.to_string(),
            state,
            reason,
        }
    }
}

/// What a person reads for the reasons a resident commonly waits on.
fn operator_reason(reason: &str) -> String {
    match reason {
        "permission-denied" | "accessibility-permission-denied" => {
            "Accessibility is not granted to this copy (System Settings → Privacy & Security → Accessibility)".to_string()
        }
        "no-graphical-session" => "no one is logged in to a graphical session".to_string(),
        "no-interactive-session" => "no interactive session is running".to_string(),
        other => other.to_string(),
    }
}

/// One machine service on one machine.
#[derive(Clone)]
pub(crate) struct MachineServiceInstance {
    pub key: MachineServiceKey,
    pub machine: String,
    pub state: InstanceState,
    /// `override <path> (set by ...)`, `installed <version> (pack)`, or
    /// `not installed: <reason>`, naming who chose it as the row records.
    pub provenance: String,
    /// The pack artifact this machine holds, when it is the one declared.
    pub installed_version: Option<String>,
    /// The binary the selection runs instead of the pack's artifact.
    pub override_command: Option<String>,
    /// Who made the selection, when the row recorded it.
    pub set_by: Option<cairn_config::fleet::SetBy>,
    pub catalog: Vec<McpToolDef>,
    pub probed_at_ms: Option<i64>,
    /// Whether it serves MCP to this runner's gateway: a per-machine resident
    /// with `transport: mcp-stdio`, or a fleet service's host instance.
    pub serves_mcp: bool,
    /// The gateway's way in, while the cell exists.
    pub facade: Option<Arc<dyn PlacedFacade>>,
    /// `placement: fleet`: delivered to the machine, never a resident.
    pub fleet: bool,
    /// For a fleet service's host instance that is ready, what the host
    /// launches on demand.
    pub launch: Option<NativeLaunch>,
}

impl MachineServiceInstance {
    /// The state as a short phrase. A fleet instance that is ready describes
    /// the artifact, not the capability: it is installed, and whether what it
    /// needs is present is the program's to say when it is called.
    pub(crate) fn sentence(&self) -> String {
        match (&self.state, self.fleet) {
            (InstanceState::Ready, true) => "installed".to_string(),
            (state, _) => state.sentence(),
        }
    }
}

impl std::fmt::Debug for MachineServiceInstance {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MachineServiceInstance")
            .field("key", &self.key)
            .field("machine", &self.machine)
            .field("state", &self.state)
            .field("provenance", &self.provenance)
            .field("catalog", &self.catalog.len())
            .finish_non_exhaustive()
    }
}

/// Every machine `key` has an instance on, with its state: the machines the row
/// selects it on, or for a fleet service every machine it is delivered to.
pub(crate) fn instances(
    orch: &Orchestrator,
    key: &MachineServiceKey,
) -> Vec<MachineServiceInstance> {
    let fleet = crate::config::settings::load_fleet(&orch.config_dir);
    let row = &fleet.machine_services;
    let declared = declared(orch, key);
    let views = supervisor(orch).host().machines(orch);
    if declared.placement() == Some(Placement::Fleet) {
        let enrolled = fleet
            .remote_executors
            .keys()
            .map(|executor_id| crate::fleet::management::public_name(orch, executor_id));
        return fleet_machines(key, row, &views, enrolled)
            .into_iter()
            .map(|machine| fleet_instance(orch, key, &machine, row, &declared, &views))
            .collect();
    }
    row.machines_for(key)
        .into_iter()
        .map(|(machine, selection)| instance(orch, key, machine, selection, &declared, &views))
        .collect()
}

/// Every machine service on `machine`, with its state: the per-machine ones the
/// row selects there, and every fleet service.
pub(crate) fn instances_on(orch: &Orchestrator, machine: &str) -> Vec<MachineServiceInstance> {
    let row = crate::config::settings::load_fleet(&orch.config_dir).machine_services;
    let views = supervisor(orch).host().machines(orch);
    let name = cairn_uri::normalize_executor_name(machine).unwrap_or_else(|| machine.to_string());
    let mut instances: Vec<MachineServiceInstance> = row
        .selected_on(machine)
        .into_iter()
        .filter_map(|(key, selection)| {
            let declared = declared(orch, key);
            // A fleet key's row entry is an override, not a selection.
            (declared.placement() != Some(Placement::Fleet))
                .then(|| instance(orch, key, &name, selection, &declared, &views))
        })
        .collect();
    for service in fleet_services(orch) {
        let declared = declared(orch, &service.key);
        instances.push(fleet_instance(
            orch,
            &service.key,
            &name,
            &row,
            &declared,
            &views,
        ));
    }
    instances.sort_by(|a, b| a.key.cmp(&b.key));
    instances
}

/// The keys of every machine service with an instance anywhere: each one the
/// row names, and every fleet service.
pub(crate) fn selected_keys(orch: &Orchestrator) -> Vec<MachineServiceKey> {
    let mut keys: Vec<MachineServiceKey> = crate::config::settings::load_fleet(&orch.config_dir)
        .machine_services
        .0
        .into_keys()
        .chain(fleet_services(orch).into_iter().map(|service| service.key))
        .collect();
    keys.sort();
    keys.dedup();
    keys
}

/// The public name of the machine this runner runs on: its colocated executor,
/// which shares the runner's home. Only it can launch a fleet service today.
pub(crate) const HOST_MACHINE: &str = LOCAL_EXECUTOR_NAME;

/// One fleet service an installed pack declares.
pub(crate) struct FleetService {
    pub key: MachineServiceKey,
    pub package: ServicePackage,
    /// Why the pack's own artifact may not run; see [`ServiceSource::refusal`].
    pub refusal: Option<String>,
}

/// Every fleet service the installed packs declare, in key order: the one place
/// that enumerates them, for delivery, instances, and readiness alike. A
/// declaration that does not parse is not one, since nothing says it is fleet.
pub(crate) fn fleet_services(orch: &Orchestrator) -> Vec<FleetService> {
    let mut services = Vec::new();
    for (pack, source) in supervisor(orch).sources().packs(orch) {
        let listings = match source.list() {
            Ok(listings) => listings,
            Err(error) => {
                tracing::warn!(%error, pack, "could not list a pack's services");
                continue;
            }
        };
        for listing in listings {
            let fleet = listing
                .declaration
                .as_ref()
                .is_ok_and(|declaration| declaration.placement == Placement::Fleet);
            if !fleet {
                continue;
            }
            let (Ok(key), Ok(Some(package))) = (
                MachineServiceKey::new(&pack, &listing.id),
                source.package(&listing.id),
            ) else {
                continue;
            };
            services.push(FleetService {
                key,
                package,
                refusal: source.refusal(&listing.id),
            });
        }
    }
    services.sort_by(|a, b| a.key.cmp(&b.key));
    services
}

/// The machines a fleet service has an instance on: the host, every attached or
/// enrolled machine, and any the row overrides it on, by name.
fn fleet_machines(
    key: &MachineServiceKey,
    row: &MachineServicesConfig,
    views: &[MachineView],
    enrolled: impl IntoIterator<Item = String>,
) -> Vec<String> {
    let mut machines: std::collections::BTreeSet<String> = views
        .iter()
        .map(|view| view.name.clone())
        .chain(std::iter::once(HOST_MACHINE.to_string()))
        .collect();
    machines.extend(enrolled);
    machines.extend(
        row.machines_for(key)
            .into_iter()
            .filter(|(_, selection)| selection.override_command().is_some())
            .map(|(machine, _)| machine.to_string()),
    );
    machines.into_iter().collect()
}

/// A fleet service on one machine. There is no cell: the state is the
/// machine's install report, or the operator's override.
fn fleet_instance(
    orch: &Orchestrator,
    key: &MachineServiceKey,
    machine: &str,
    row: &MachineServicesConfig,
    declared: &Declared,
    views: &[MachineView],
) -> MachineServiceInstance {
    let view = views.iter().find(|view| view.name == machine);
    let selection = row.selection(key, machine);
    let override_command = selection.and_then(MachineSelection::override_command);
    let set_by = selection.and_then(|selection| selection.set_by.clone());
    let (artifact_sha, report) = holding(key, declared, view);
    let installed_version = installed_version(report, artifact_sha);
    let provenance = provenance(
        key,
        machine,
        selection,
        installed_version.as_deref(),
        report,
        view,
        artifact_sha,
    );
    let host = machine == HOST_MACHINE;
    let mut state = fleet_state(
        key,
        machine,
        override_command,
        declared,
        view,
        artifact_sha,
        report,
    );
    let native = declared.native();
    let mut launch = None;
    if host && state == InstanceState::Ready {
        let target = view
            .and_then(|view| view.target.clone())
            .or_else(|| RemotePlatform::current().map(|platform| platform.target().to_string()));
        match native
            .ok_or_else(|| format!("no installed pack declares {key}"))
            .and_then(|native| {
                fleet_launch(
                    &orch.config_dir,
                    key,
                    native,
                    override_command,
                    set_by.as_ref(),
                    target.as_deref(),
                )
            }) {
            Ok(resolved) => launch = Some(resolved),
            Err(reason) => {
                state = InstanceState::NotInstalled {
                    reason: reason
                        .strip_prefix("not installed: ")
                        .unwrap_or(&reason)
                        .to_string(),
                }
            }
        }
    }
    MachineServiceInstance {
        key: key.clone(),
        machine: machine.to_string(),
        state,
        provenance,
        installed_version,
        override_command: override_command.map(str::to_string),
        set_by,
        catalog: Vec::new(),
        probed_at_ms: None,
        serves_mcp: host && declared.transport() == Some(Transport::McpStdio),
        facade: None,
        fleet: true,
        launch,
    }
}

/// A fleet instance's state, from what its machine reports holding. Never
/// `Starting`, `WaitingForOperator` or `Degraded`: nothing runs to be in them.
fn fleet_state(
    key: &MachineServiceKey,
    machine: &str,
    override_command: Option<&str>,
    declared: &Declared,
    view: Option<&MachineView>,
    artifact_sha: Option<&str>,
    report: Option<&InstalledMachineService>,
) -> InstanceState {
    match &declared.package {
        None => {
            return InstanceState::NotInstalled {
                reason: format!("no installed pack declares {key}"),
            }
        }
        Some(Err(error)) => {
            return InstanceState::Failed {
                reason: format!("its declaration does not parse: {error}"),
            }
        }
        Some(Ok(_)) => {}
    }
    // The host launches an override itself; another machine's is only as
    // present as the machine.
    if override_command.is_some() {
        return if view.is_some() || machine == HOST_MACHINE {
            InstanceState::Ready
        } else {
            InstanceState::MachineAbsent
        };
    }
    if let Some(refusal) = &declared.refusal {
        return InstanceState::Failed {
            reason: refusal.clone(),
        };
    }
    if view.is_none() {
        return InstanceState::MachineAbsent;
    }
    match report {
        Some(report) => match &report.outcome {
            MachineServiceInstallOutcome::Installed
                if Some(report.sha256.as_str()) == artifact_sha =>
            {
                InstanceState::Ready
            }
            MachineServiceInstallOutcome::Installed => InstanceState::NotInstalled {
                reason: format!(
                    "{machine} holds {key} {}, not the version this pack declares",
                    report.version
                ),
            },
            MachineServiceInstallOutcome::Installing => InstanceState::Installing,
            MachineServiceInstallOutcome::NotInstalled { reason } => InstanceState::NotInstalled {
                reason: reason.clone(),
            },
        },
        None if artifact_sha.is_none() => InstanceState::NotInstalled {
            reason: "the pack ships no artifact for this machine".to_string(),
        },
        None => InstanceState::NotInstalled {
            reason: "the machine has not reported it yet".to_string(),
        },
    }
}

/// What the host launches for a fleet service: the operator's override when
/// one is set, else the pack's artifact as installed under `home`, marker
/// checked, with the declaration's arguments. Never a PATH lookup. Not
/// MCP-shaped: anything that launches a fleet service on demand reads this.
pub(crate) fn fleet_launch(
    home: &std::path::Path,
    key: &MachineServiceKey,
    native: &NativeSpec,
    override_command: Option<&str>,
    set_by: Option<&cairn_config::fleet::SetBy>,
    target: Option<&str>,
) -> Result<NativeLaunch, String> {
    let program = match override_command {
        Some(command) => {
            return Ok(NativeLaunch::overriding(
                command.to_string(),
                native.args.clone(),
                set_by.cloned(),
            ))
        }
        None => {
            let target = target.ok_or_else(|| {
                "not installed: this machine runs a platform no pack artifact targets".to_string()
            })?;
            let artifact = native
                .targets
                .get(target)
                .ok_or_else(|| format!("not installed: {key} ships no artifact for {target}"))?;
            let program = ManagedProgram {
                pack: key.pack.clone(),
                service: key.service.clone(),
                sha256: artifact.sha256.clone(),
                executable: artifact.executable.clone(),
            };
            managed_program_path(home, &program)?
                .to_string_lossy()
                .into_owned()
        }
    };
    Ok(NativeLaunch::artifact(
        program,
        native.args.clone(),
        set_by.cloned(),
    ))
}

/// What the installed packs say about one key.
struct Declared {
    /// The shipped declaration or why it does not parse; `None` when no
    /// installed pack declares the key.
    package: Option<Result<ServicePackage, String>>,
    /// Why the pack's own artifact may not run.
    refusal: Option<String>,
}

impl Declared {
    fn shipped(&self) -> Option<&ServicePackage> {
        self.package
            .as_ref()
            .and_then(|package| package.as_ref().ok())
    }

    fn placement(&self) -> Option<Placement> {
        self.shipped().map(|package| package.declaration.placement)
    }

    fn transport(&self) -> Option<Transport> {
        self.shipped().map(|package| package.declaration.transport)
    }

    fn native(&self) -> Option<&NativeSpec> {
        self.shipped()
            .and_then(|package| package.declaration.native())
    }
}

fn declared(orch: &Orchestrator, key: &MachineServiceKey) -> Declared {
    let source = supervisor(orch)
        .sources()
        .packs(orch)
        .into_iter()
        .find(|(pack, _)| *pack == key.pack)
        .map(|(_, source)| source);
    Declared {
        package: source
            .as_ref()
            .and_then(|source| source.package(&key.service).transpose()),
        refusal: source.and_then(|source| source.refusal(&key.service)),
    }
}

fn instance(
    orch: &Orchestrator,
    key: &MachineServiceKey,
    machine: &str,
    selection: &MachineSelection,
    declared: &Declared,
    views: &[MachineView],
) -> MachineServiceInstance {
    let view = views.iter().find(|view| view.name == machine);
    let cell = supervisor(orch).existing("", &ServiceOwner::pack(&key.pack, machine), &key.service);
    let (artifact_sha, report) = holding(key, declared, view);
    let installed_version = installed_version(report, artifact_sha);
    let provenance = provenance(
        key,
        machine,
        Some(selection),
        installed_version.as_deref(),
        report,
        view,
        artifact_sha,
    );
    let machine_cell = cell.as_ref().and_then(|cell| cell.machine.as_ref());
    let probe = machine_cell.and_then(|machine_cell| machine_cell.probe.lock().unwrap().clone());
    let state = derive_state(
        key,
        selection,
        declared,
        view,
        report,
        cell.as_deref(),
        probe.as_ref(),
    );
    MachineServiceInstance {
        key: key.clone(),
        machine: machine.to_string(),
        state,
        provenance,
        installed_version,
        override_command: selection.override_command().map(str::to_string),
        set_by: selection.set_by.clone(),
        catalog: probe
            .as_ref()
            .map(|probe| probe.catalog.clone())
            .unwrap_or_default(),
        probed_at_ms: probe.as_ref().map(|probe| probe.at_ms),
        serves_mcp: declared.transport() == Some(Transport::McpStdio),
        facade: cell.as_ref().and_then(|cell| facade_for(orch, cell)),
        fleet: false,
        launch: None,
    }
}

/// The version a machine holds, when it holds the artifact the pack declares.
fn installed_version(
    report: Option<&InstalledMachineService>,
    artifact_sha: Option<&str>,
) -> Option<String> {
    report
        .filter(|report| report.outcome == MachineServiceInstallOutcome::Installed)
        .filter(|report| Some(report.sha256.as_str()) == artifact_sha)
        .map(|report| report.version.clone())
}

/// An instance's own sentence for what it runs and who chose it.
fn provenance(
    key: &MachineServiceKey,
    machine: &str,
    selection: Option<&MachineSelection>,
    installed_version: Option<&str>,
    report: Option<&InstalledMachineService>,
    view: Option<&MachineView>,
    artifact_sha: Option<&str>,
) -> String {
    let setter = crate::fleet::management::describe_setter;
    let override_command = selection.and_then(MachineSelection::override_command);
    let set_by = selection.and_then(|selection| selection.set_by.as_ref());
    match (override_command, installed_version, report) {
        (Some(command), _, _) => format!("override {command} ({})", setter(set_by)),
        // A fleet service's artifact has no row entry and so no setter; a
        // selection made before attribution says nothing rather than guess.
        (None, Some(version), _) => match set_by {
            Some(set_by) => format!("installed {version} (pack; {})", setter(Some(set_by))),
            None => format!("installed {version} (pack)"),
        },
        (None, None, Some(report)) => match &report.outcome {
            MachineServiceInstallOutcome::NotInstalled { reason } => {
                format!("not installed: {reason}")
            }
            MachineServiceInstallOutcome::Installing => "not installed: installing".to_string(),
            MachineServiceInstallOutcome::Installed => format!(
                "not installed: {machine} holds {} {}, not the version this pack declares",
                key, report.version
            ),
        },
        (None, None, None) => match (view, artifact_sha) {
            (None, _) => format!("not installed: {MACHINE_ABSENT}"),
            (Some(_), None) => {
                "not installed: the pack ships no artifact for this machine".to_string()
            }
            (Some(_), Some(_)) => "not installed: the machine has not reported it yet".to_string(),
        },
    }
}

/// The artifact the pack declares for this machine, and what the machine reports
/// holding for `key`: that artifact when it holds it, else whatever it does hold.
fn holding<'a>(
    key: &MachineServiceKey,
    declared: &'a Declared,
    view: Option<&'a MachineView>,
) -> (Option<&'a str>, Option<&'a InstalledMachineService>) {
    let native = declared
        .package
        .as_ref()
        .and_then(|declared| declared.as_ref().ok())
        .and_then(|package| package.declaration.native());
    let artifact_sha = view
        .and_then(|view| view.target.as_deref())
        .and_then(|target| native.and_then(|native| native.targets.get(target)))
        .map(|artifact| artifact.sha256.as_str());
    let report = view.and_then(|view| {
        let mine = |installed: &&InstalledMachineService| {
            installed.pack == key.pack && installed.service == key.service
        };
        view.installed
            .iter()
            .filter(mine)
            .find(|installed| Some(installed.sha256.as_str()) == artifact_sha)
            .or_else(|| view.installed.iter().find(mine))
    });
    (artifact_sha, report)
}

/// An instance's state, for everything that reports or announces one.
fn derive_state(
    key: &MachineServiceKey,
    selection: &MachineSelection,
    declared: &Declared,
    view: Option<&MachineView>,
    report: Option<&InstalledMachineService>,
    cell: Option<&Cell>,
    probe: Option<&ProbeReport>,
) -> InstanceState {
    let attached = view.is_some();
    // A machine away from the runner has no executor to judge yet.
    let floors = view.is_none_or(|view| view.floors_service_residents);
    let runs_artifact = selection.override_command().is_none();
    if !runs_artifact && !floors {
        return InstanceState::Failed {
            reason: OVERRIDE_NEEDS_FLOOR.to_string(),
        };
    }
    match (&declared.package, &declared.refusal) {
        (None, _) => InstanceState::NotInstalled {
            reason: format!("no installed pack declares {key}"),
        },
        (Some(Err(error)), _) => InstanceState::Failed {
            reason: format!("its declaration does not parse: {error}"),
        },
        (Some(Ok(_)), Some(refusal)) if runs_artifact => InstanceState::Failed {
            reason: refusal.clone(),
        },
        (Some(Ok(_)), _) => live_state(
            attached,
            runs_artifact
                && report.is_some_and(|report| {
                    report.outcome == MachineServiceInstallOutcome::Installing
                }),
            cell,
            probe,
        ),
    }
}

/// Tell open windows this instance's state changed, if it has since the last
/// time this was asked.
///
/// The comparison is what keeps a five-minute probe that keeps saying `ready`
/// from refetching every catalog in every window. Returns whether it announced.
pub(super) fn announce_state(orch: &Orchestrator, cell: &Arc<Cell>) -> bool {
    let Some(machine_cell) = cell.machine.as_ref() else {
        return false;
    };
    let row = crate::config::settings::load_fleet(&orch.config_dir).machine_services;
    let key = &machine_cell.key;
    // An unselected machine reports no instance, and the write that unselected
    // it told open windows itself.
    let Some(selection) = row.selection(key, &machine_cell.machine) else {
        return false;
    };
    let declared = declared(orch, key);
    let view = machine_view(orch, &machine_cell.machine);
    let (_, report) = holding(key, &declared, view.as_ref());
    // As `instance` reads it: a drained cell has left the set.
    let current = supervisor(orch).existing("", &cell.owner, &cell.service);
    let probe = current
        .as_ref()
        .and_then(|cell| cell.machine.as_ref())
        .and_then(|machine_cell| machine_cell.probe.lock().unwrap().clone());
    let state = derive_state(
        key,
        selection,
        &declared,
        view.as_ref(),
        report,
        current.as_deref(),
        probe.as_ref(),
    );
    {
        let mut announced = machine_cell.announced.lock().unwrap();
        if announced.as_ref() == Some(&state) {
            return false;
        }
        *announced = Some(state);
    }
    orch.emit_machine_service_change();
    true
}

/// Run [`apply_machine_services`] on its own task, from code that is not
/// async or must not wait on placement.
pub(crate) fn spawn_apply(orch: &Orchestrator, reason: &'static str) {
    if tokio::runtime::Handle::try_current().is_err() {
        return;
    }
    let orch = orch.clone();
    tokio::spawn(async move { apply_machine_services(&orch, reason).await });
}

fn live_state(
    attached: bool,
    installing: bool,
    cell: Option<&Cell>,
    probe: Option<&ProbeReport>,
) -> InstanceState {
    if !attached {
        return InstanceState::MachineAbsent;
    }
    if installing {
        return InstanceState::Installing;
    }
    let Some(cell) = cell else {
        return InstanceState::Starting;
    };
    let (status, last_error, serving) = {
        let state = cell.state.lock().unwrap();
        (
            state.status,
            state.last_error.clone(),
            state
                .current
                .as_ref()
                .is_some_and(|current| current.serving()),
        )
    };
    match status {
        ServiceState::Failed => {
            return InstanceState::Failed {
                reason: last_error.unwrap_or_else(|| "it crashed repeatedly".to_string()),
            }
        }
        ServiceState::Backoff => {
            return InstanceState::Degraded {
                reason: format!(
                    "restarting after a crash: {}",
                    last_error.unwrap_or_else(|| "it exited".to_string())
                ),
            }
        }
        _ => {}
    }
    if !serving {
        let blocked = cell
            .machine
            .as_ref()
            .and_then(|machine_cell| machine_cell.blocked.lock().unwrap().clone());
        return match blocked {
            Some(reason) if reason == "installing" => InstanceState::Installing,
            Some(reason) if reason == MACHINE_ABSENT => InstanceState::MachineAbsent,
            Some(reason) => InstanceState::NotInstalled {
                reason: reason
                    .strip_prefix("not installed: ")
                    .unwrap_or(&reason)
                    .to_string(),
            },
            None => InstanceState::Starting,
        };
    }
    match probe.map(|probe| &probe.outcome) {
        None => InstanceState::Starting,
        Some(ProbeOutcome::Ready) => InstanceState::Ready,
        Some(ProbeOutcome::NotReady {
            reason,
            waiting_for_operator: true,
        }) => InstanceState::WaitingForOperator {
            reason: reason.clone().unwrap_or_else(|| "not ready".to_string()),
        },
        Some(ProbeOutcome::NotReady { reason, .. }) => InstanceState::Degraded {
            reason: reason
                .clone()
                .unwrap_or_else(|| "its probe says it is not ready".to_string()),
        },
        Some(ProbeOutcome::NoAnswer { reason }) => InstanceState::Degraded {
            reason: reason.clone(),
        },
    }
}
